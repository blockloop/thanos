package receive

import (
	"context"
	"net/http"
	"strconv"
	"sync"

	"github.com/go-kit/kit/log"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/prompb"
	"github.com/prometheus/prometheus/storage"
	terrors "github.com/prometheus/prometheus/tsdb/errors"
)

// Cluster is a map of address to Writer where writer can be local, http, or grpc
type Cluster struct {
	m sync.Map
}

func (c Cluster) Set(addr string, w Writer) {
	c.m.Store(addr, w)
}

func (c Cluster) Get(addr string) (Writer, bool) {
	if w, ok := c.m.Load(addr); ok {
		return w.(Writer), ok
	}
	return nil, false
}

type ClusterWriter struct {
	logger      log.Logger
	LocalWriter *LocalWriter
	options     *Options
	hashring    Hashring
	cluster     Cluster

	mtx sync.RWMutex

	// Metrics
	forwardRequestsTotal *prometheus.CounterVec
}

func NewClusterWriter(l log.Logger, lw *LocalWriter, h Hashring) *ClusterWriter {
	cw := &ClusterWriter{
		logger:  l,
		cluster: Cluster{},
	}
	if h != nil {
		cw.Hashring(h)
	}
	return cw
}

// Hashring sets the hashring used for writing
func (cw *ClusterWriter) Hashring(h Hashring) {
	cw.mtx.Lock()
	defer cw.mtx.Unlock()
	cw.hashring = h

	for _, host := range h.Hosts() {
		if host == cw.options.Endpoint {
			cw.cluster.Set(cw.options.Endpoint, cw.LocalWriter)
		} else {
			cw.cluster.Set(host, NewHTTPWriter(cw.logger, host, cw.options.TenantHeader, cw.options.ReplicaHeader))
		}
	}
}

// Write accepts a write request, batches its time series by
// corresponding endpoint, and forwards them in parallel to the
// correct endpoint. Requests destined for the local node are written
// the the local receiver. For a given write request, at most one outgoing
// write request will be made to every other node in the hashring,
// unless the request needs to be replicated.
// The function only returns when all requests have finished
// or the context is canceled.
func (cw *ClusterWriter) Write(ctx context.Context, tenant string, r replica, ts []prompb.TimeSeries) error {
	wreqs := make(map[string]*prompb.WriteRequest)
	replicas := make(map[string]replica)

	// It is possible that hashring is ready in testReady() but unready now,
	// so need to lock here.
	cw.mtx.RLock()
	if cw.hashring == nil {
		cw.mtx.RUnlock()
		return errors.New("hashring is not ready")
	}

	// Batch all of the time series in the write request
	// into several smaller write requests that are
	// grouped by target endpoint. This ensures that
	// for any incoming write request to a node,
	// at most one outgoing write request will be made
	// to every other node in the hashring, rather than
	// one request per time series.
	for _, t := range ts {
		endpoint, err := cw.hashring.GetN(tenant, &t, r.n)
		if err != nil {
			cw.mtx.RUnlock()
			return err
		}
		if _, ok := wreqs[endpoint]; !ok {
			wreqs[endpoint] = &prompb.WriteRequest{}
			replicas[endpoint] = r
		}
		wr := wreqs[endpoint]
		wr.Timeseries = append(wr.Timeseries, t)
	}
	cw.mtx.RUnlock()

	return cw.parallelizeRequests(ctx, tenant, replicas, wreqs)
}

// parallelizeRequests parallelizes a given set of write requests.
// The function only returns when all requests have finished
// or the context is canceled.
func (cw *ClusterWriter) parallelizeRequests(ctx context.Context, tenant string, replicas map[string]replica, wreqs map[string]*prompb.WriteRequest) error {
	ec := make(chan error)
	defer close(ec)
	// We don't wan't to use a sync.WaitGroup here because that
	// introduces an unnecessary second synchronization mechanism,
	// the first being the error chan. Plus, it saves us a goroutine
	// as in order to collect errors while doing wg.Wait, we would
	// need a separate error collection goroutine.
	var n int
	for endpoint, wr := range wreqs {
		n++
		// If the request is not yet replicated, let's replicate it.
		// If the replication factor isn't greater than 1, let's
		// just forward the requests.
		if !replicas[endpoint].replicated && cw.options.ReplicationFactor > 1 {
			go func(endpoint string) {
				ec <- cw.replicate(ctx, tenant, wreqs[endpoint])
			}(endpoint)
			continue
		}
		// Make a request to the specified endpoint.
		go func(endpoint string) {
			w, ok := cw.cluster.Get(endpoint)
			if !ok {
				w = NewHTTPWriter(cw.logger, endpoint, cw.options.TenantHeader, cw.options.ReplicaHeader)
				cw.cluster.Set(endpoint, w)
			}
			ec <- w.Write(ctx, tenant, replicas[endpoint], wr.Timeseries)
		}(endpoint)
	}

	// Collect any errors from forwarding the time series.
	// Rather than doing a wg.Wait here, we decrement a counter
	// for every error received on the chan. This simplifies
	// error collection and avoids data races with a separate
	// error collection goroutine.
	var errs terrors.MultiError
	for ; n > 0; n-- {
		if err := <-ec; err != nil {
			errs.Add(err)
		}
	}

	return errs.Err()
}

// replicate replicates a write request to (replication-factor) nodes
// selected by the tenant and time series.
// The function only returns when all replication requests have finished
// or the context is canceled.
func (cw *ClusterWriter) replicate(ctx context.Context, tenant string, wreq *prompb.WriteRequest) error {
	wreqs := make(map[string]*prompb.WriteRequest)
	replicas := make(map[string]replica)
	var i uint64

	// It is possible that hashring is ready in testReady() but unready now,
	// so need to lock here.
	cw.mtx.RLock()
	if cw.hashring == nil {
		cw.mtx.RUnlock()
		return errors.New("hashring is not ready")
	}

	for i = 0; i < cw.options.ReplicationFactor; i++ {
		endpoint, err := cw.hashring.GetN(tenant, &wreq.Timeseries[0], i)
		if err != nil {
			cw.mtx.RUnlock()
			return err
		}
		wreqs[endpoint] = wreq
		replicas[endpoint] = replica{i, true}
	}
	cw.mtx.RUnlock()

	err := cw.parallelizeRequests(ctx, tenant, replicas, wreqs)
	if errs, ok := err.(terrors.MultiError); ok {
		if uint64(countCause(errs, isConflict)) >= (cw.options.ReplicationFactor+1)/2 {
			return errors.Wrap(conflictErr, "did not meet replication threshold")
		}
		if uint64(len(errs)) >= (cw.options.ReplicationFactor+1)/2 {
			return errors.Wrap(err, "did not meet replication threshold")
		}
		return nil
	}
	return errors.Wrap(err, "could not replicate write request")
}

// countCause counts the number of errors within the given error
// whose causes satisfy the given function.
// countCause will inspect the error's cause or, if the error is a MultiError,
// the cause of each contained error but will not traverse any deeper.
func countCause(err error, f func(error) bool) int {
	errs, ok := err.(terrors.MultiError)
	if !ok {
		errs = []error{err}
	}
	var n int
	for i := range errs {
		if f(errors.Cause(errs[i])) {
			n++
		}
	}
	return n
}

// isConflict returns whether or not the given error represents a conflict.
func isConflict(err error) bool {
	if err == nil {
		return false
	}
	return err == conflictErr || err == storage.ErrDuplicateSampleForTimestamp || err == storage.ErrOutOfOrderSample || err == storage.ErrOutOfBounds || err.Error() == strconv.Itoa(http.StatusConflict)
}

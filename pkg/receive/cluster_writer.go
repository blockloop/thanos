package receive

import (
	"bytes"
	"context"
	"net/http"
	"strconv"
	"sync"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/prompb"
	"github.com/prometheus/prometheus/storage"
	terrors "github.com/prometheus/prometheus/tsdb/errors"
	"github.com/thanos-io/thanos/pkg/tracing"
)

// Cluster is a map of address to Writer where writer can be local, http, or grpc
type Cluster map[string]Writer

type ClusterWriter struct {
	logger   log.Logger
	options  *Options
	hashring Hashring
	cluster  *Cluster

	mtx sync.RWMutex

	// Metrics
	forwardRequestsTotal *prometheus.CounterVec
}

func NewClusterWriter(l log.Logger, h Hashring, cluster *Cluster) *ClusterWriter {
	return &ClusterWriter{
		logger:   l,
		hashring: h,
		cluster:  cluster,
		forwardRequestsTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "thanos_receive_forward_requests_total",
				Help: "The number of forward requests.",
			}, []string{"result"},
		),
	}
}

func (w *ClusterWriter) Write(ctx context.Context, tenant string, r replica, ts []prompb.TimeSeries) error {
	return w.forward(ctx, tenant, r, ts)
}

// forward accepts a write request, batches its time series by
// corresponding endpoint, and forwards them in parallel to the
// correct endpoint. Requests destined for the local node are written
// the the local receiver. For a given write request, at most one outgoing
// write request will be made to every other node in the hashring,
// unless the request needs to be replicated.
// The function only returns when all requests have finished
// or the context is canceled.
func (w *ClusterWriter) forward(ctx context.Context, tenant string, r replica, ts []prompb.TimeSeries) error {
	wreqs := make(map[string]*prompb.WriteRequest)
	replicas := make(map[string]replica)

	// It is possible that hashring is ready in testReady() but unready now,
	// so need to lock here.
	w.mtx.RLock()
	if w.hashring == nil {
		w.mtx.RUnlock()
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
		endpoint, err := w.hashring.GetN(tenant, &t, r.n)
		if err != nil {
			w.mtx.RUnlock()
			return err
		}
		if _, ok := wreqs[endpoint]; !ok {
			wreqs[endpoint] = &prompb.WriteRequest{}
			replicas[endpoint] = r
		}
		wr := wreqs[endpoint]
		wr.Timeseries = append(wr.Timeseries, t)
	}
	w.mtx.RUnlock()

	return w.parallelizeRequests(ctx, tenant, replicas, wreqs)
}

// parallelizeRequests parallelizes a given set of write requests.
// The function only returns when all requests have finished
// or the context is canceled.
func (w *ClusterWriter) parallelizeRequests(ctx context.Context, tenant string, replicas map[string]replica, wreqs map[string]*prompb.WriteRequest) error {
	ec := make(chan error)
	defer close(ec)
	// We don't wan't to use a sync.WaitGroup here because that
	// introduces an unnecessary second synchronization mechanism,
	// the first being the error chan. Plus, it saves us a goroutine
	// as in order to collect errors while doing wg.Wait, we would
	// need a separate error collection goroutine.
	var n int
	for endpoint := range wreqs {
		n++
		// If the request is not yet replicated, let's replicate it.
		// If the replication factor isn't greater than 1, let's
		// just forward the requests.
		if !replicas[endpoint].replicated && w.options.ReplicationFactor > 1 {
			go func(endpoint string) {
				ec <- w.replicate(ctx, tenant, wreqs[endpoint])
			}(endpoint)
			continue
		}
		// If the endpoint for the write request is the
		// local node, then don't make a request but store locally.
		// By handing replication to the local node in the same
		// function as replication to other nodes, we can treat
		// a failure to write locally as just another error that
		// can be ignored if the replication factor is met.
		if endpoint == w.options.Endpoint {
			go func(endpoint string) {
				var err error
				w.mtx.RLock()
				if w.writer == nil {
					err = errors.New("storage is not ready")
				} else {
					err = w.writer.Write(tenant, wreqs[endpoint])
					// When a MultiError is added to another MultiError, the error slices are concatenated, not nested.
					// To avoid breaking the counting logic, we need to flatten the error.
					if errs, ok := err.(terrors.MultiError); ok {
						if countCause(errs, isConflict) > 0 {
							err = errors.Wrap(conflictErr, errs.Error())
						} else {
							err = errors.New(errs.Error())
						}
					}
				}
				w.mtx.RUnlock()
				if err != nil {
					level.Error(w.logger).Log("msg", "storing locally", "err", err, "endpoint", endpoint)
				}
				ec <- err
			}(endpoint)
			continue
		}
		// Make a request to the specified endpoint.
		go func(endpoint string) {
			buf, err := proto.Marshal(wreqs[endpoint])
			if err != nil {
				level.Error(w.logger).Log("msg", "marshaling proto", "err", err, "endpoint", endpoint)
				ec <- err
				return
			}
			req, err := http.NewRequest("POST", endpoint, bytes.NewBuffer(snappy.Encode(nil, buf)))
			if err != nil {
				level.Error(w.logger).Log("msg", "creating request", "err", err, "endpoint", endpoint)
				ec <- err
				return
			}
			req.Header.Add(w.options.TenantHeader, tenant)
			req.Header.Add(w.options.ReplicaHeader, strconv.FormatUint(replicas[endpoint].n, 10))

			// Increment the counters as necessary now that
			// the requests will go out.
			defer func() {
				if err != nil {
					w.forwardRequestsTotal.WithLabelValues("error").Inc()
					return
				}
				w.forwardRequestsTotal.WithLabelValues("success").Inc()
			}()

			// Create a span to track the request made to another receive node.
			span, ctx := tracing.StartSpan(ctx, "thanos_receive_forward")
			defer span.Finish()

			// Actually make the request against the endpoint
			// we determined should handle these time series.
			var res *http.Response
			res, err = w.client.Do(req.WithContext(ctx))
			if err != nil {
				level.Error(w.logger).Log("msg", "forwarding request", "err", err, "endpoint", endpoint)
				ec <- err
				return
			}
			if res.StatusCode != http.StatusOK {
				err = errors.New(strconv.Itoa(res.StatusCode))
				level.Error(w.logger).Log("msg", "forwarding returned non-200 status", "err", err, "endpoint", endpoint)
				ec <- err
				return
			}
			ec <- nil
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
func (w *ClusterWriter) replicate(ctx context.Context, tenant string, wreq *prompb.WriteRequest) error {
	wreqs := make(map[string]*prompb.WriteRequest)
	replicas := make(map[string]replica)
	var i uint64

	// It is possible that hashring is ready in testReady() but unready now,
	// so need to lock here.
	w.mtx.RLock()
	if w.hashring == nil {
		w.mtx.RUnlock()
		return errors.New("hashring is not ready")
	}

	for i = 0; i < w.options.ReplicationFactor; i++ {
		endpoint, err := w.hashring.GetN(tenant, &wreq.Timeseries[0], i)
		if err != nil {
			w.mtx.RUnlock()
			return err
		}
		wreqs[endpoint] = wreq
		replicas[endpoint] = replica{i, true}
	}
	w.mtx.RUnlock()

	err := w.parallelizeRequests(ctx, tenant, replicas, wreqs)
	if errs, ok := err.(terrors.MultiError); ok {
		if uint64(countCause(errs, isConflict)) >= (w.options.ReplicationFactor+1)/2 {
			return errors.Wrap(conflictErr, "did not meet replication threshold")
		}
		if uint64(len(errs)) >= (w.options.ReplicationFactor+1)/2 {
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

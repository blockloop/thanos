package receive

import (
	"bytes"
	"context"
	"net/http"
	"strconv"
	"sync"

	"github.com/go-kit/kit/log/level"
	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/prompb"
	"github.com/thanos-io/thanos/pkg/tracing"
)

type HTTPWriter struct {
	addr          string
	c             *http.Client
	replicaHeader string
	tenantHeader  string

	mtx sync.Mutex
}

func NewHTTPWriter(addr, tenantHeader, replicaHeader string) {
	return &HTTPWriter{
		addr:          addr,
		tenantHeader:  tenantHeader,
		replicaHeader: replicaHeader,
	}
}

func (w *HTTPWriter) HTTPClient(c *http.Client) error {
	w.mtx.Lock()
	defer w.mtx.Unlock()
	w.c = c
}

func (w *HTTPWriter) Write(ctx context.Context, tenant string, r replica, ts []prompb.TimeSeries) error {
	buf, err := proto.Marshal(&prompb.WriteRequest{ts})
	if err != nil {
		level.Error(w.logger).Log("msg", "marshaling proto", "err", err, "endpoint", w.addr)
		ec <- err
		return
	}
	req, err := http.NewRequest("POST", w.addr, bytes.NewBuffer(snappy.Encode(nil, buf)))
	if err != nil {
		level.Error(w.logger).Log("msg", "creating request", "err", err, "endpoint", w.addr)
		ec <- err
		return
	}
	req.Header.Add(w.tenantHeader, tenant)
	req.Header.Add(w.replicaHeader, strconv.FormatUint(r.n, 10))

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
	res, err := w.client.Do(req.WithContext(ctx))
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
}

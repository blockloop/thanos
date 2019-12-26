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
	"github.com/prometheus/prometheus/prompb"
	"github.com/thanos-io/thanos/pkg/tracing"
)

type HTTPWriter struct {
	logger        log.Logger
	addr          string
	c             *http.Client
	replicaHeader string
	tenantHeader  string

	mtx sync.Mutex
}

func NewHTTPWriter(logger log.Logger, addr, tenantHeader, replicaHeader string) *HTTPWriter {
	return &HTTPWriter{
		logger:        logger,
		addr:          addr,
		tenantHeader:  tenantHeader,
		replicaHeader: replicaHeader,
	}
}

func (hw *HTTPWriter) HTTPClient(c *http.Client) {
	hw.mtx.Lock()
	defer hw.mtx.Unlock()
	hw.c = c
}

func (hw *HTTPWriter) Write(ctx context.Context, tenant string, r replica, ts []prompb.TimeSeries) error {
	buf, err := proto.Marshal(&prompb.WriteRequest{Timeseries: ts})
	if err != nil {
		level.Error(hw.logger).Log("msg", "marshaling proto", "err", err, "endpoint", hw.addr)
		return err
	}
	req, err := http.NewRequest("POST", hw.addr, bytes.NewBuffer(snappy.Encode(nil, buf)))
	if err != nil {
		level.Error(hw.logger).Log("msg", "creating request", "err", err, "endpoint", hw.addr)
		return err
	}
	req.Header.Add(hw.tenantHeader, tenant)
	req.Header.Add(hw.replicaHeader, strconv.FormatUint(r.n, 10))

	// Increment the counters as necessary now that
	// the requests will go out.
	defer func() {
		if err != nil {
			forwardRequestsTotal.WithLabelValues("error").Inc()
			return
		}
		forwardRequestsTotal.WithLabelValues("success").Inc()
	}()

	// Create a span to track the request made to another receive node.
	span, ctx := tracing.StartSpan(ctx, "thanos_receive_forward")
	defer span.Finish()

	// Actually make the request against the endpoint
	// we determined should handle these time series.
	res, err := hw.c.Do(req.WithContext(ctx))
	if err != nil {
		level.Error(hw.logger).Log("msg", "forwarding request", "err", err, "endpoint", hw.addr)
		return err
	}
	if res.StatusCode != http.StatusOK {
		err = errors.New(strconv.Itoa(res.StatusCode))
		level.Error(hw.logger).Log("msg", "forwarding returned non-200 status", "err", err, "endpoint", hw.addr)
		return err
	}
	return nil
}

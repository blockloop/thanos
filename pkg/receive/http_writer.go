package receive

import (
	"context"
	"net/http"

	"github.com/prometheus/prometheus/prompb"
)

type HTTPWriter struct {
	addr string
	c    *http.Client
}

func NewHTTPWriter(addr string) {
	return &HTTPWriter{
		addr: addr,
		// TODO
	}
}

func Write(ctx context.Context, tenant string, r replica, ts []prompb.TimeSeries) error {
	return nil
}

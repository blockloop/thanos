package receive

import (
	"context"

	"github.com/prometheus/prometheus/prompb"
	"github.com/thanos-io/thanos/pkg/store/storepb"
)

type GRPCWriter struct {
	c *storepb.WriteableStoreClient
}

func NewGRPCWriter(c *storepb.WriteableStoreClient) {
	return &GRPCWriter{
		c: c,
	}
}

func Write(ctx context.Context, tenant string, r replica, ts []prompb.TimeSeries) error {
	return nil
}

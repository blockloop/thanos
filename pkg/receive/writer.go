package receive

import (
	"context"

	"github.com/prometheus/prometheus/prompb"
)

const (
	// DefaultTenantHeader is the default header used to designate the tenant making a write request.
	DefaultTenantHeader = "THANOS-TENANT"
	// DefaultReplicaHeader is the default header used to designate the replica count of a write request.
	DefaultReplicaHeader = "THANOS-REPLICA"
)

type Writer interface {
	Write(ctx context.Context, tenant string, r replica, ts []prompb.TimeSeries) error
}

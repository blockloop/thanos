package receive

import (
	"crypto/tls"
	"fmt"
	"io/ioutil"
	stdlog "log"
	"net"
	"net/http"
	"strconv"
	"sync"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	conntrack "github.com/mwitkow/go-conntrack"
	opentracing "github.com/opentracing/opentracing-go"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/route"
	"github.com/prometheus/prometheus/prompb"
	extpromhttp "github.com/thanos-io/thanos/pkg/extprom/http"
	"github.com/thanos-io/thanos/pkg/runutil"
	"github.com/thanos-io/thanos/pkg/tracing"
)

var (
	// conflictErr is returned whenever an operation fails due to any conflict-type error.
	conflictErr = errors.New("conflict")

	forwardRequestsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "thanos_receive_forward_requests_total",
			Help: "The number of forward requests.",
		}, []string{"result"},
	)
)

// Options for the web Handler.
type Options struct {
	Writer            *LocalWriter
	ListenAddress     string
	Registry          prometheus.Registerer
	Endpoint          string
	TenantHeader      string
	ReplicaHeader     string
	ReplicationFactor uint64
	Tracer            opentracing.Tracer
	TLSConfig         *tls.Config
	TLSClientConfig   *tls.Config
}

// Handler serves a Prometheus remote write receiving HTTP endpoint.
type Handler struct {
	client   *http.Client
	logger   log.Logger
	writer   Writer
	router   *route.Router
	options  *Options
	listener net.Listener

	mtx sync.RWMutex
}

func NewHandler(logger log.Logger, o *Options) *Handler {
	if logger == nil {
		logger = log.NewNopLogger()
	}

	transport := http.DefaultTransport.(*http.Transport)
	transport.TLSClientConfig = o.TLSClientConfig
	client := &http.Client{Transport: transport}
	if o.Tracer != nil {
		client.Transport = tracing.HTTPTripperware(logger, client.Transport)
	}

	h := &Handler{
		client:  client,
		logger:  logger,
		writer:  o.Writer,
		router:  route.New(),
		options: o,
	}

	ins := extpromhttp.NewNopInstrumentationMiddleware()
	if o.Registry != nil {
		ins = extpromhttp.NewInstrumentationMiddleware(o.Registry)
		o.Registry.MustRegister(forwardRequestsTotal)
	}

	readyf := h.testReady
	instrf := func(name string, next func(w http.ResponseWriter, r *http.Request)) http.HandlerFunc {
		if o.Tracer != nil {
			next = tracing.HTTPMiddleware(o.Tracer, name, logger, http.HandlerFunc(next))
		}
		return ins.NewHandler(name, http.HandlerFunc(next))
	}

	h.router.Post("/api/v1/receive", instrf("receive", readyf(h.receive)))

	return h
}

// SetWriter sets the writer.
// The writer must be set to a non-nil value in order for the
// handler to be ready and usable.
// If the writer is nil, then the handler is marked as not ready.
func (h *Handler) SetWriter(w *LocalWriter) {
	h.mtx.Lock()
	defer h.mtx.Unlock()
	h.writer = w
}

// Verifies whether the server is ready or not.
func (h *Handler) isReady() bool {
	h.mtx.RLock()
	sr := h.writer != nil
	h.mtx.RUnlock()
	return sr
}

// Checks if server is ready, calls f if it is, returns 503 if it is not.
func (h *Handler) testReady(f http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if h.isReady() {
			f(w, r)
			return
		}

		w.WriteHeader(http.StatusServiceUnavailable)
		_, err := fmt.Fprintf(w, "Service Unavailable")
		if err != nil {
			h.logger.Log("msg", "failed to write to response body", "err", err)
		}
	}
}

// Close stops the Handler.
func (h *Handler) Close() {
	if h.listener != nil {
		runutil.CloseWithLogOnErr(h.logger, h.listener, "receive HTTP listener")
	}
}

// Run serves the HTTP endpoints.
func (h *Handler) Run() error {
	level.Info(h.logger).Log("msg", "Start listening for connections", "address", h.options.ListenAddress)

	var err error
	h.listener, err = net.Listen("tcp", h.options.ListenAddress)
	if err != nil {
		return err
	}

	// Monitor incoming connections with conntrack.
	h.listener = conntrack.NewListener(h.listener,
		conntrack.TrackWithName("http"),
		conntrack.TrackWithTracing())

	errlog := stdlog.New(log.NewStdlibAdapter(level.Error(h.logger)), "", 0)

	httpSrv := &http.Server{
		Handler:   h.router,
		ErrorLog:  errlog,
		TLSConfig: h.options.TLSConfig,
	}

	return httpSrv.Serve(h.listener)
}

// replica encapsulates the replica number of a request and if the request is
// already replicated.
type replica struct {
	n          uint64
	replicated bool
}

func (h *Handler) receive(w http.ResponseWriter, r *http.Request) {
	compressed, err := ioutil.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	reqBuf, err := snappy.Decode(nil, compressed)
	if err != nil {
		level.Error(h.logger).Log("msg", "snappy decode error", "err", err)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	var wreq prompb.WriteRequest
	if err := proto.Unmarshal(reqBuf, &wreq); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	var rep replica
	replicaRaw := r.Header.Get(h.options.ReplicaHeader)
	// If the header is empty, we assume the request is not yet replicated.
	if replicaRaw != "" {
		if rep.n, err = strconv.ParseUint(replicaRaw, 10, 64); err != nil {
			http.Error(w, "could not parse replica header", http.StatusBadRequest)
			return
		}
		rep.replicated = true
	}
	// The replica value in the header is zero-indexed, thus we need >=.
	if rep.n >= h.options.ReplicationFactor {
		http.Error(w, "replica count exceeds replication factor", http.StatusBadRequest)
		return
	}

	tenant := r.Header.Get(h.options.TenantHeader)

	if err := h.writer.Write(r.Context(), tenant, rep, wreq.Timeseries); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

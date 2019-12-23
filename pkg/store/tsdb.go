package store

import (
	"context"
	"io"
	"math"
	"sort"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/prompb"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/thanos-io/thanos/pkg/component"
	"github.com/thanos-io/thanos/pkg/runutil"
	"github.com/thanos-io/thanos/pkg/store/storepb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const streamCommitChunkSize = 500

// TSDBStore implements the store API against a local TSDB instance.
// It attaches the provided external labels to all results. It only responds with raw data
// and does not support downsampling.
type TSDBStore struct {
	logger         log.Logger
	db             *tsdb.DB
	component      component.SourceStoreAPI
	externalLabels labels.Labels
}

// NewTSDBStore creates a new TSDBStore.
func NewTSDBStore(logger log.Logger, _ prometheus.Registerer, db *tsdb.DB, component component.SourceStoreAPI, externalLabels labels.Labels) *TSDBStore {
	if logger == nil {
		logger = log.NewNopLogger()
	}
	return &TSDBStore{
		logger:         logger,
		db:             db,
		component:      component,
		externalLabels: externalLabels,
	}
}

// Info returns store information about the Prometheus instance.
func (s *TSDBStore) Info(ctx context.Context, r *storepb.InfoRequest) (*storepb.InfoResponse, error) {
	res := &storepb.InfoResponse{
		Labels:    make([]storepb.Label, 0, len(s.externalLabels)),
		StoreType: s.component.ToProto(),
		MinTime:   0,
		MaxTime:   math.MaxInt64,
	}
	if blocks := s.db.Blocks(); len(blocks) > 0 {
		res.MinTime = blocks[0].Meta().MinTime
	}
	for _, l := range s.externalLabels {
		res.Labels = append(res.Labels, storepb.Label{
			Name:  l.Name,
			Value: l.Value,
		})
	}

	// Until we deprecate the single labels in the reply, we just duplicate
	// them here for migration/compatibility purposes.
	res.LabelSets = []storepb.LabelSet{}
	if len(res.Labels) > 0 {
		res.LabelSets = append(res.LabelSets, storepb.LabelSet{
			Labels: res.Labels,
		})
	}
	return res, nil
}

// Series returns all series for a requested time range and label matcher. The returned data may
// exceed the requested time bounds.
func (s *TSDBStore) Series(r *storepb.SeriesRequest, srv storepb.Store_SeriesServer) error {
	match, newMatchers, err := matchesExternalLabels(r.Matchers, s.externalLabels)
	if err != nil {
		return status.Error(codes.InvalidArgument, err.Error())
	}

	if !match {
		return nil
	}

	if len(newMatchers) == 0 {
		return status.Error(codes.InvalidArgument, errors.New("no matchers specified (excluding external labels)").Error())
	}

	matchers, err := translateMatchers(newMatchers)
	if err != nil {
		return status.Error(codes.InvalidArgument, err.Error())
	}

	q, err := s.db.Querier(r.MinTime, r.MaxTime)
	if err != nil {
		return status.Error(codes.Internal, err.Error())
	}
	defer runutil.CloseWithLogOnErr(s.logger, q, "close tsdb querier series")

	set, err := q.Select(matchers...)
	if err != nil {
		return status.Error(codes.Internal, err.Error())
	}

	var respSeries storepb.Series

	for set.Next() {
		series := set.At()

		// TODO(fabxc): An improvement over this trivial approach would be to directly
		// use the chunks provided by TSDB in the response.
		// But since the sidecar has a similar approach, optimizing here has only
		// limited benefit for now.
		// NOTE: XOR encoding supports a max size of 2^16 - 1 samples, so we need
		// to chunk all samples into groups of no more than 2^16 - 1
		// See: https://github.com/thanos-io/thanos/pull/1038.
		c, err := s.encodeChunks(series.Iterator(), math.MaxUint16)
		if err != nil {
			return status.Errorf(codes.Internal, "encode chunk: %s", err)
		}

		respSeries.Labels = s.translateAndExtendLabels(series.Labels(), s.externalLabels)
		respSeries.Chunks = append(respSeries.Chunks[:0], c...)

		if err := srv.Send(storepb.NewSeriesResponse(&respSeries)); err != nil {
			return status.Error(codes.Aborted, err.Error())
		}
	}
	return nil
}

func (s *TSDBStore) encodeChunks(it tsdb.SeriesIterator, maxSamplesPerChunk int) (chks []storepb.AggrChunk, err error) {
	var (
		chkMint int64
		chk     *chunkenc.XORChunk
		app     chunkenc.Appender
		isNext  = it.Next()
	)

	for isNext {
		if chk == nil {
			chk = chunkenc.NewXORChunk()
			app, err = chk.Appender()
			if err != nil {
				return nil, err
			}
			chkMint, _ = it.At()
		}

		app.Append(it.At())
		chkMaxt, _ := it.At()

		isNext = it.Next()
		if isNext && chk.NumSamples() < maxSamplesPerChunk {
			continue
		}

		// Cut the chunk.
		chks = append(chks, storepb.AggrChunk{
			MinTime: chkMint,
			MaxTime: chkMaxt,
			Raw:     &storepb.Chunk{Type: storepb.Chunk_XOR, Data: chk.Bytes()},
		})
		chk = nil
	}
	if it.Err() != nil {
		return nil, errors.Wrap(it.Err(), "read TSDB series")
	}

	return chks, nil

}

// translateAndExtendLabels transforms a metrics into a protobuf label set. It additionally
// attaches the given labels to it, overwriting existing ones on collision.
func (s *TSDBStore) translateAndExtendLabels(m, extend labels.Labels) []storepb.Label {
	lset := make([]storepb.Label, 0, len(m)+len(extend))

	for _, l := range m {
		if extend.Get(l.Name) != "" {
			continue
		}
		lset = append(lset, storepb.Label{
			Name:  l.Name,
			Value: l.Value,
		})
	}
	for _, l := range extend {
		lset = append(lset, storepb.Label{
			Name:  l.Name,
			Value: l.Value,
		})
	}
	sort.Slice(lset, func(i, j int) bool {
		return lset[i].Name < lset[j].Name
	})
	return lset
}

// LabelNames returns all known label names.
func (s *TSDBStore) LabelNames(ctx context.Context, _ *storepb.LabelNamesRequest) (
	*storepb.LabelNamesResponse, error,
) {
	q, err := s.db.Querier(math.MinInt64, math.MaxInt64)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	defer runutil.CloseWithLogOnErr(s.logger, q, "close tsdb querier label names")

	res, err := q.LabelNames()
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &storepb.LabelNamesResponse{Names: res}, nil
}

// LabelValues returns all known label values for a given label name.
func (s *TSDBStore) LabelValues(ctx context.Context, r *storepb.LabelValuesRequest) (
	*storepb.LabelValuesResponse, error,
) {
	q, err := s.db.Querier(math.MinInt64, math.MaxInt64)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	defer runutil.CloseWithLogOnErr(s.logger, q, "close tsdb querier label values")

	res, err := q.LabelValues(r.Label)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &storepb.LabelValuesResponse{Values: res}, nil
}

// RemoteWrite receives a stream of write requests and performs a remote write action with them
func (s *TSDBStore) RemoteWrite(stream storepb.Store_RemoteWriteServer) error {
	for {
		resp, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			level.Error(s.logger).Log("msg", "read from grpc stream failure", "err", err)
			return err
		}

		// Write all metrics sent
		WriteTimeSeries(resp.Timeseries, s.db, s.logger)
	}
	return nil
}

// WriteTimeSeries writes a set of timeseries metrics to the tsdb
func WriteTimeSeries(timeseries []prompb.TimeSeries, tsdb *tsdb.DB, logger log.Logger) {
	ap := tsdb.Appender()

	commit := func() {
		if err := ap.Commit(); err != nil {
			level.Error(logger).Log("msg", "failure trying to commit write to store", "err", err)
			if err := ap.Rollback(); err != nil {
				level.Error(logger).Log("msg", "failure trying to rollback write to store", "err", err)
			}
		}
	}
	defer commit()

	for i, ts := range timeseries {
		if i%streamCommitChunkSize == 0 {
			commit()
		}
		lbls := make(labels.Labels, len(ts.Labels))
		for i, l := range ts.Labels {
			lbls[i] = labels.Label{
				Name:  l.GetName(),
				Value: l.GetValue(),
			}
		}
		// soring guarantees hash consistency
		sort.Sort(lbls)

		var ref uint64
		var err error
		for _, s := range ts.Samples {
			if ref == 0 {
				ref, err = ap.Add(lbls, s.GetTimestamp(), s.GetValue())
			} else {
				err = ap.AddFast(ref, s.GetTimestamp(), s.GetValue())
			}
			if err != nil {
				level.Error(logger).Log("msg", "failure trying to append sample to store", "err", err)
			}
		}
	}
}

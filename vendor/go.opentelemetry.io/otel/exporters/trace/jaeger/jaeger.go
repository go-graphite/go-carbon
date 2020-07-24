// Copyright The OpenTelemetry Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package jaeger

import (
	"context"
	"encoding/binary"

	"google.golang.org/api/support/bundler"
	"google.golang.org/grpc/codes"

	"go.opentelemetry.io/otel/api/global"
	"go.opentelemetry.io/otel/api/kv"
	"go.opentelemetry.io/otel/api/kv/value"
	apitrace "go.opentelemetry.io/otel/api/trace"
	gen "go.opentelemetry.io/otel/exporters/trace/jaeger/internal/gen-go/jaeger"
	export "go.opentelemetry.io/otel/sdk/export/trace"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

const defaultServiceName = "OpenTelemetry"

type Option func(*options)

// options are the options to be used when initializing a Jaeger export.
type options struct {
	// Process contains the information about the exporting process.
	Process Process

	// BufferMaxCount defines the total number of traces that can be buffered in memory
	BufferMaxCount int

	// BatchMaxCount defines the maximum number of spans sent in one batch
	BatchMaxCount int

	Config *sdktrace.Config

	// RegisterGlobal is set to true if the trace provider of the new pipeline should be
	// registered as Global Trace Provider
	RegisterGlobal bool

	Disabled bool
}

// WithProcess sets the process with the information about the exporting process.
func WithProcess(process Process) Option {
	return func(o *options) {
		o.Process = process
	}
}

// WithBufferMaxCount defines the total number of traces that can be buffered in memory
func WithBufferMaxCount(bufferMaxCount int) Option {
	return func(o *options) {
		o.BufferMaxCount = bufferMaxCount
	}
}

// WithBatchMaxCount defines the maximum number of spans in one batch
func WithBatchMaxCount(batchMaxCount int) Option {
	return func(o *options) {
		o.BatchMaxCount = batchMaxCount
	}
}

// WithSDK sets the SDK config for the exporter pipeline.
func WithSDK(config *sdktrace.Config) Option {
	return func(o *options) {
		o.Config = config
	}
}

// RegisterAsGlobal enables the registration of the trace provider of the new pipeline
// as Global Trace Provider.
func RegisterAsGlobal() Option {
	return func(o *options) {
		o.RegisterGlobal = true
	}
}

func WithDisabled(disabled bool) Option {
	return func(o *options) {
		o.Disabled = disabled
	}
}

// NewRawExporter returns a trace.Exporter implementation that exports
// the collected spans to Jaeger.
//
// It will IGNORE Disabled option.
func NewRawExporter(endpointOption EndpointOption, opts ...Option) (*Exporter, error) {
	uploader, err := endpointOption()
	if err != nil {
		return nil, err
	}

	o := options{}
	opts = append(opts, WithProcessFromEnv())
	for _, opt := range opts {
		opt(&o)
	}

	service := o.Process.ServiceName
	if service == "" {
		service = defaultServiceName
	}
	tags := make([]*gen.Tag, 0, len(o.Process.Tags))
	for _, tag := range o.Process.Tags {
		t := keyValueToTag(tag)
		if t != nil {
			tags = append(tags, t)
		}
	}
	e := &Exporter{
		uploader: uploader,
		process: &gen.Process{
			ServiceName: service,
			Tags:        tags,
		},
		o: o,
	}
	bundler := bundler.NewBundler((*gen.Span)(nil), func(bundle interface{}) {
		if err := e.upload(bundle.([]*gen.Span)); err != nil {
			global.Handle(err)
		}
	})

	// Set BufferedByteLimit with the total number of spans that are permissible to be held in memory.
	// This needs to be done since the size of messages is always set to 1. Failing to set this would allow
	// 1G messages to be held in memory since that is the default value of BufferedByteLimit.
	if o.BufferMaxCount != 0 {
		bundler.BufferedByteLimit = o.BufferMaxCount
	}

	// The default value bundler uses is 10, increase to send larger batches
	if o.BatchMaxCount != 0 {
		bundler.BundleCountThreshold = o.BatchMaxCount
	}

	e.bundler = bundler
	return e, nil
}

// NewExportPipeline sets up a complete export pipeline
// with the recommended setup for trace provider
func NewExportPipeline(endpointOption EndpointOption, opts ...Option) (apitrace.Provider, func(), error) {
	o := options{}
	opts = append(opts, WithDisabledFromEnv())
	for _, opt := range opts {
		opt(&o)
	}
	if o.Disabled {
		return &apitrace.NoopProvider{}, func() {}, nil
	}

	exporter, err := NewRawExporter(endpointOption, opts...)
	if err != nil {
		return nil, nil, err
	}
	syncer := sdktrace.WithSyncer(exporter)
	tp, err := sdktrace.NewProvider(syncer)
	if err != nil {
		return nil, nil, err
	}
	if exporter.o.Config != nil {
		tp.ApplyConfig(*exporter.o.Config)
	}
	if exporter.o.RegisterGlobal {
		global.SetTraceProvider(tp)
	}

	return tp, exporter.Flush, nil
}

// Process contains the information exported to jaeger about the source
// of the trace data.
type Process struct {
	// ServiceName is the Jaeger service name.
	ServiceName string

	// Tags are added to Jaeger Process exports
	Tags []kv.KeyValue
}

// Exporter is an implementation of trace.SpanSyncer that uploads spans to Jaeger.
type Exporter struct {
	process  *gen.Process
	bundler  *bundler.Bundler
	uploader batchUploader
	o        options
}

var _ export.SpanSyncer = (*Exporter)(nil)

// ExportSpan exports a SpanData to Jaeger.
func (e *Exporter) ExportSpan(ctx context.Context, d *export.SpanData) {
	_ = e.bundler.Add(spanDataToThrift(d), 1)
	// TODO(jbd): Handle oversized bundlers.
}

func spanDataToThrift(data *export.SpanData) *gen.Span {
	tags := make([]*gen.Tag, 0, len(data.Attributes))
	for _, kv := range data.Attributes {
		tag := keyValueToTag(kv)
		if tag != nil {
			tags = append(tags, tag)
		}
	}

	// TODO (jmacd): OTel has a broad "last value wins"
	// semantic. Should resources be appended before span
	// attributes, above, to allow span attributes to
	// overwrite resource attributes?
	if data.Resource != nil {
		for iter := data.Resource.Iter(); iter.Next(); {
			if tag := keyValueToTag(iter.Attribute()); tag != nil {
				tags = append(tags, tag)
			}
		}
	}

	if il := data.InstrumentationLibrary; il.Name != "" {
		tags = append(tags, getStringTag("instrumentation.name", il.Name))
		if il.Version != "" {
			tags = append(tags, getStringTag("instrumentation.version", il.Name))
		}
	}

	tags = append(tags,
		getInt64Tag("status.code", int64(data.StatusCode)),
		getStringTag("status.message", data.StatusMessage),
		getStringTag("span.kind", data.SpanKind.String()),
	)

	// Ensure that if Status.Code is not OK, that we set the "error" tag on the Jaeger span.
	// See Issue https://github.com/census-instrumentation/opencensus-go/issues/1041
	if data.StatusCode != codes.OK {
		tags = append(tags, getBoolTag("error", true))
	}

	var logs []*gen.Log
	for _, a := range data.MessageEvents {
		fields := make([]*gen.Tag, 0, len(a.Attributes))
		for _, kv := range a.Attributes {
			tag := keyValueToTag(kv)
			if tag != nil {
				fields = append(fields, tag)
			}
		}
		fields = append(fields, getStringTag("name", a.Name))
		logs = append(logs, &gen.Log{
			Timestamp: a.Time.UnixNano() / 1000,
			Fields:    fields,
		})
	}

	var refs []*gen.SpanRef
	for _, link := range data.Links {
		refs = append(refs, &gen.SpanRef{
			TraceIdHigh: int64(binary.BigEndian.Uint64(link.TraceID[0:8])),
			TraceIdLow:  int64(binary.BigEndian.Uint64(link.TraceID[8:16])),
			SpanId:      int64(binary.BigEndian.Uint64(link.SpanID[:])),
			// TODO(paivagustavo): properly set the reference type when specs are defined
			//  see https://github.com/open-telemetry/opentelemetry-specification/issues/65
			RefType: gen.SpanRefType_CHILD_OF,
		})
	}

	return &gen.Span{
		TraceIdHigh:   int64(binary.BigEndian.Uint64(data.SpanContext.TraceID[0:8])),
		TraceIdLow:    int64(binary.BigEndian.Uint64(data.SpanContext.TraceID[8:16])),
		SpanId:        int64(binary.BigEndian.Uint64(data.SpanContext.SpanID[:])),
		ParentSpanId:  int64(binary.BigEndian.Uint64(data.ParentSpanID[:])),
		OperationName: data.Name, // TODO: if span kind is added then add prefix "Sent"/"Recv"
		Flags:         int32(data.SpanContext.TraceFlags),
		StartTime:     data.StartTime.UnixNano() / 1000,
		Duration:      data.EndTime.Sub(data.StartTime).Nanoseconds() / 1000,
		Tags:          tags,
		Logs:          logs,
		References:    refs,
	}
}

func keyValueToTag(keyValue kv.KeyValue) *gen.Tag {
	var tag *gen.Tag
	switch keyValue.Value.Type() {
	case value.STRING:
		s := keyValue.Value.AsString()
		tag = &gen.Tag{
			Key:   string(keyValue.Key),
			VStr:  &s,
			VType: gen.TagType_STRING,
		}
	case value.BOOL:
		b := keyValue.Value.AsBool()
		tag = &gen.Tag{
			Key:   string(keyValue.Key),
			VBool: &b,
			VType: gen.TagType_BOOL,
		}
	case value.INT32:
		i := int64(keyValue.Value.AsInt32())
		tag = &gen.Tag{
			Key:   string(keyValue.Key),
			VLong: &i,
			VType: gen.TagType_LONG,
		}
	case value.INT64:
		i := keyValue.Value.AsInt64()
		tag = &gen.Tag{
			Key:   string(keyValue.Key),
			VLong: &i,
			VType: gen.TagType_LONG,
		}
	case value.UINT32:
		i := int64(keyValue.Value.AsUint32())
		tag = &gen.Tag{
			Key:   string(keyValue.Key),
			VLong: &i,
			VType: gen.TagType_LONG,
		}
	case value.UINT64:
		// we'll ignore the value if it overflows
		if i := int64(keyValue.Value.AsUint64()); i >= 0 {
			tag = &gen.Tag{
				Key:   string(keyValue.Key),
				VLong: &i,
				VType: gen.TagType_LONG,
			}
		}
	case value.FLOAT32:
		f := float64(keyValue.Value.AsFloat32())
		tag = &gen.Tag{
			Key:     string(keyValue.Key),
			VDouble: &f,
			VType:   gen.TagType_DOUBLE,
		}
	case value.FLOAT64:
		f := keyValue.Value.AsFloat64()
		tag = &gen.Tag{
			Key:     string(keyValue.Key),
			VDouble: &f,
			VType:   gen.TagType_DOUBLE,
		}
	}
	return tag
}

func getInt64Tag(k string, i int64) *gen.Tag {
	return &gen.Tag{
		Key:   k,
		VLong: &i,
		VType: gen.TagType_LONG,
	}
}

func getStringTag(k, s string) *gen.Tag {
	return &gen.Tag{
		Key:   k,
		VStr:  &s,
		VType: gen.TagType_STRING,
	}
}

func getBoolTag(k string, b bool) *gen.Tag {
	return &gen.Tag{
		Key:   k,
		VBool: &b,
		VType: gen.TagType_BOOL,
	}
}

// Flush waits for exported trace spans to be uploaded.
//
// This is useful if your program is ending and you do not want to lose recent spans.
func (e *Exporter) Flush() {
	e.bundler.Flush()
}

func (e *Exporter) upload(spans []*gen.Span) error {
	batch := &gen.Batch{
		Spans:   spans,
		Process: e.process,
	}

	return e.uploader.upload(batch)
}

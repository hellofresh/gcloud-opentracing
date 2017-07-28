package gcloudtracer

import (
	"bytes"
	"context"
	"fmt"
	"strconv"
	"time"

	basictracer "github.com/opentracing/basictracer-go"
	opentracing "github.com/opentracing/opentracing-go"
	"github.com/opentracing/opentracing-go/ext"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
	"golang.org/x/oauth2/jwt"
	cloudtrace "google.golang.org/api/cloudtrace/v1"
	"google.golang.org/api/support/bundler"
)

var _ basictracer.SpanRecorder = &Recorder{}

var labelMap = map[string]string{
	string(ext.PeerHostname):   `trace.cloud.google.com/http/host`,
	string(ext.HTTPMethod):     `trace.cloud.google.com/http/method`,
	string(ext.HTTPStatusCode): `trace.cloud.google.com/http/status_code`,
	string(ext.HTTPUrl):        `trace.cloud.google.com/http/url`,
}

// Recorder implements basictracer.SpanRecorder interface
// used to write traces to the GCE StackDriver.
type Recorder struct {
	project     string
	ctx         context.Context
	log         Logger
	traceClient *cloudtrace.Service
	bundler     *bundler.Bundler
}

// NewRecorder creates new GCloud StackDriver recorder.
func NewRecorder(ctx context.Context, opts ...Option) (*Recorder, error) {
	var options Options
	for _, o := range opts {
		o(&options)
	}
	if err := options.Valid(); err != nil {
		return nil, err
	}
	if options.log == nil {
		options.log = &defaultLogger{}
	}

	// Your credentials should be obtained from the Google
	// Developer Console (https://console.developers.google.com).
	conf := &jwt.Config{
		Email:        options.credentials.Email,
		PrivateKey:   options.credentials.PrivateKey,
		PrivateKeyID: options.credentials.PrivateKeyID,
		Scopes: []string{
			"https://www.googleapis.com/auth/trace.append",
			"https://www.googleapis.com/auth/trace.readonly",
			"https://www.googleapis.com/auth/cloud-platform",
		},
		TokenURL: google.JWTTokenURL,
	}

	c, err := cloudtrace.New(conf.Client(oauth2.NoContext))
	if err != nil {
		return nil, err
	}

	rec := &Recorder{
		project:     options.projectID,
		ctx:         ctx,
		traceClient: c,
		log:         options.log,
	}

	bundler := bundler.NewBundler((*cloudtrace.Trace)(nil), func(bundle interface{}) {
		traces := bundle.([]*cloudtrace.Trace)
		err := rec.upload(traces)
		if err != nil {
			rec.log.Errorf("failed to upload %d traces to the Cloud Trace server. (err = %s)", len(traces), err)
		}
	})
	bundler.DelayThreshold = 2 * time.Second
	bundler.BundleCountThreshold = 100
	// We're not measuring bytes here, we're counting traces and spans as one "byte" each.
	bundler.BundleByteThreshold = 1000
	bundler.BundleByteLimit = 1000
	bundler.BufferedByteLimit = 10000
	rec.bundler = bundler

	return rec, nil
}

// RecordSpan writes Span to the GCLoud StackDriver.
func (r *Recorder) RecordSpan(sp basictracer.RawSpan) {
	if !sp.Context.Sampled {
		return
	}

	traceID := fmt.Sprintf("%016x%016x", sp.Context.TraceID, sp.Context.TraceID)
	labels := convertTags(sp.Tags)
	transposeLabels(labels)
	addLogs(labels, sp.Logs)

	trace := &cloudtrace.Trace{
		ProjectId: r.project,
		TraceId:   traceID,
		Spans: []*cloudtrace.TraceSpan{
			{
				SpanId:       sp.Context.SpanID,
				Kind:         convertSpanKind(sp.Tags),
				Name:         sp.Operation,
				StartTime:    sp.Start.Format(time.RFC3339Nano),
				EndTime:      sp.Start.Add(sp.Duration).Format(time.RFC3339Nano),
				ParentSpanId: sp.ParentSpanID,
				Labels:       labels,
			},
		},
	}

	err := r.bundler.Add(trace, 2) // size = (1 trace + 1 span)
	if err == bundler.ErrOverflow {
		r.log.Errorf("trace upload bundle too full. uploading immediately")
		err = r.upload([]*cloudtrace.Trace{trace})
		if err != nil {
			r.log.Errorf("error uploading trace: %s", err)
		}
	}
}

func (r *Recorder) upload(traces []*cloudtrace.Trace) error {
	_, err := r.traceClient.Projects.PatchTraces(r.project, &cloudtrace.Traces{
		Traces: traces,
	}).Context(context.Background()).Do()

	return err
}

func convertTags(tags opentracing.Tags) map[string]string {
	labels := make(map[string]string)
	for k, v := range tags {
		switch v := v.(type) {
		case int:
			labels[k] = strconv.Itoa(v)
		case string:
			labels[k] = v
		}
	}
	return labels
}

func convertSpanKind(tags opentracing.Tags) string {
	switch tags[string(ext.SpanKind)] {
	case ext.SpanKindRPCServerEnum:
		return "RPC_SERVER"
	case ext.SpanKindRPCClientEnum:
		return "RPC_CLIENT"
	default:
		return "SPAN_KIND_UNSPECIFIED"
	}
}

// rewrite well-known opentracing.ext labels into those gcloud-native labels
func transposeLabels(labels map[string]string) {
	for k, t := range labelMap {
		if vv, ok := labels[k]; ok {
			labels[t] = vv
			delete(labels, k)
		}
	}
}

// copy opentracing events into gcloud trace labels
func addLogs(target map[string]string, logs []opentracing.LogRecord) {
	for i, l := range logs {
		buf := bytes.NewBufferString(l.Timestamp.String())
		for j, f := range l.Fields {
			buf.WriteString(f.Key())
			buf.WriteString("=")
			buf.WriteString(fmt.Sprint(f.Value()))
			if j != len(l.Fields)+1 {
				buf.WriteString(" ")
			}
		}
		target[fmt.Sprintf("event_%d", i)] = buf.String()
	}
}

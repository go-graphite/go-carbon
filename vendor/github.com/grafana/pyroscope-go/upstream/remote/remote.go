package remote

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/url"
	"path"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/grafana/pyroscope-go/upstream"
)

var errCloudTokenRequired = errors.New("please provide an authentication token." +
	" You can find it here: https://pyroscope.io/cloud")

const (
	authTokenDeprecationWarning = "Authtoken is specified, but deprecated and ignored. " +
		"Please switch to BasicAuthUser and BasicAuthPassword. " +
		"If you need to use Bearer token authentication for a custom setup, " +
		"you can use the HTTPHeaders option to set the Authorization header manually."
	cloudHostnameSuffix = "pyroscope.cloud"
)

type Remote struct {
	mu     sync.Mutex
	cfg    Config
	jobs   chan job
	client HTTPClient
	logger Logger

	done chan struct{}
	wg   sync.WaitGroup

	flushWG *sync.WaitGroup
}

type HTTPClient interface {
	Do(req *http.Request) (*http.Response, error)
}

type Config struct {
	// Deprecated: AuthToken will be removed in future releases.
	// Use BasicAuthUser and BasicAuthPassword instead.
	AuthToken         string
	BasicAuthUser     string // http basic auth user
	BasicAuthPassword string // http basic auth password
	TenantID          string
	HTTPHeaders       map[string]string
	Threads           int
	Address           string
	Timeout           time.Duration
	Logger            Logger
	HTTPClient        HTTPClient // optional, custom client
}

type Logger interface {
	Infof(_ string, _ ...interface{})
	Debugf(_ string, _ ...interface{})
	Errorf(_ string, _ ...interface{})
}

func NewRemote(cfg Config) (*Remote, error) {
	r := &Remote{
		cfg:  cfg,
		jobs: make(chan job, 20),
		client: &http.Client{
			Transport: &http.Transport{
				MaxConnsPerHost: cfg.Threads,
			},
			// Don't follow redirects
			// Since the go http client strips the Authorization header when doing redirects (eg http -> https)
			// https://github.com/golang/go/blob/a41763539c7ad09a22720a517a28e6018ca4db0f/src/net/http/client_test.go#L1764
			// making an authorized server return a 401
			// which is confusing since the user most likely already set up an API Key
			CheckRedirect: func(req *http.Request, via []*http.Request) error {
				return http.ErrUseLastResponse
			},
			Timeout: cfg.Timeout,
		},
		logger:  cfg.Logger,
		done:    make(chan struct{}),
		flushWG: new(sync.WaitGroup),
	}
	if cfg.HTTPClient != nil {
		r.client = cfg.HTTPClient
	}

	// parse the upstream address
	u, err := url.Parse(cfg.Address)
	if err != nil {
		return nil, err
	}

	// authorize the token first
	if cfg.AuthToken == "" && isOGPyroscopeCloud(u) {
		return nil, errCloudTokenRequired
	}

	return r, nil
}

func (r *Remote) Start() {
	r.wg.Add(r.cfg.Threads)
	for range r.cfg.Threads {
		go r.handleJobs()
	}
}

func (r *Remote) Stop() {
	if r.done != nil {
		close(r.done)
	}

	// wait for uploading goroutines exit
	r.wg.Wait()
}

func (r *Remote) Upload(uj *upstream.UploadJob) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.flushWG.Add(1)
	j := job{
		upload: uj,
		flush:  r.flushWG,
	}
	select {
	case r.jobs <- j:
	default:
		j.flush.Done()
		r.logger.Errorf("remote upload queue is full, dropping a profile job")
	}
}

func (r *Remote) Flush() {
	r.mu.Lock()
	flush := r.flushWG
	r.flushWG = new(sync.WaitGroup)
	r.mu.Unlock()
	flush.Wait()
}

func (r *Remote) uploadProfile(j *upstream.UploadJob) error {
	u, err := url.Parse(r.cfg.Address)
	if err != nil {
		return fmt.Errorf("url parse: %w", err)
	}

	body := &bytes.Buffer{}

	writer := multipart.NewWriter(body)
	fw, err := writer.CreateFormFile("profile", "profile.pprof")
	if err != nil {
		return err
	}
	_, _ = fw.Write(j.Profile)
	if j.SampleTypeConfig != nil {
		fw, err = writer.CreateFormFile("sample_type_config", "sample_type_config.json")
		if err != nil {
			return err
		}
		b, err := json.Marshal(j.SampleTypeConfig)
		if err != nil {
			return err
		}
		_, _ = fw.Write(b)
	}
	if err = writer.Close(); err != nil {
		return err
	}

	q := u.Query()
	q.Set("name", j.Name)
	q.Set("from", strconv.FormatInt(j.StartTime.UnixNano(), 10))
	q.Set("until", strconv.FormatInt(j.EndTime.UnixNano(), 10))
	q.Set("spyName", j.SpyName)
	q.Set("sampleRate", strconv.Itoa(int(j.SampleRate)))
	q.Set("units", j.Units)
	q.Set("aggregationType", j.AggregationType)

	u.Path = path.Join(u.Path, "ingest")
	u.RawQuery = q.Encode()

	r.logger.Debugf("uploading at %s", u.String())
	// new a request for the job
	request, err := http.NewRequestWithContext(context.Background(), http.MethodPost, u.String(), body)
	if err != nil {
		return fmt.Errorf("new http request: %w", err)
	}
	contentType := writer.FormDataContentType()
	r.logger.Debugf("content type: %s", contentType)
	request.Header.Set("Content-Type", contentType)
	// request.Header.Set("Content-Type", "binary/octet-stream+"+string(j.Format))

	switch {
	case r.cfg.AuthToken != "" && isOGPyroscopeCloud(u):
		request.Header.Set("Authorization", "Bearer "+r.cfg.AuthToken)
	case r.cfg.BasicAuthUser != "" && r.cfg.BasicAuthPassword != "":
		request.SetBasicAuth(r.cfg.BasicAuthUser, r.cfg.BasicAuthPassword)
	case r.cfg.AuthToken != "":
		request.Header.Set("Authorization", "Bearer "+r.cfg.AuthToken)
		r.logger.Infof(authTokenDeprecationWarning)
	}
	if r.cfg.TenantID != "" {
		request.Header.Set("X-Scope-OrgID", r.cfg.TenantID)
	}
	for k, v := range r.cfg.HTTPHeaders {
		request.Header.Set(k, v)
	}

	// do the request and get the response
	response, err := r.client.Do(request)
	if err != nil {
		return fmt.Errorf("do http request: %w", err)
	}
	defer func() {
		_ = response.Body.Close()
	}()

	// read all the response body
	respBody, err := io.ReadAll(response.Body)
	if err != nil {
		return fmt.Errorf("read response body: %w", err)
	}

	if response.StatusCode != http.StatusOK {
		return fmt.Errorf("failed to upload: (%d) '%s'", //nolint:err113
			response.StatusCode, string(respBody))
	}

	return nil
}

// handle the jobs
func (r *Remote) handleJobs() {
	for {
		select {
		case <-r.done:
			r.wg.Done()

			return
		case j := <-r.jobs:
			r.safeUpload(j.upload)
			j.flush.Done()
		}
	}
}

func isOGPyroscopeCloud(u *url.URL) bool {
	return strings.HasSuffix(u.Host, cloudHostnameSuffix)
}

// do safe upload
func (r *Remote) safeUpload(job *upstream.UploadJob) {
	defer func() {
		if catch := recover(); catch != nil {
			r.logger.Errorf("recover stack: %v: %v", catch, string(debug.Stack()))
		}
	}()

	// update the profile data to server
	if err := r.uploadProfile(job); err != nil {
		r.logger.Errorf("upload profile: %v", err)
	}
}

type job struct {
	upload *upstream.UploadJob
	flush  *sync.WaitGroup
}

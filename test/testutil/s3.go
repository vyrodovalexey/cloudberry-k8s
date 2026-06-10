// Package testutil provides shared test utilities for functional, integration, and e2e tests.
package testutil

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"time"
)

// S3TestClient is a minimal AWS Signature V4 S3 client used by integration
// tests to talk to the docker-compose MinIO service without pulling a full
// S3 SDK into the module. It supports exactly the operations the backup /
// restore acceptance scenarios need: put, get, list, delete and head-bucket.
type S3TestClient struct {
	// Endpoint is the S3 endpoint, e.g. "http://127.0.0.1:9000".
	Endpoint string
	// AccessKey is the S3 access key ID.
	AccessKey string
	// SecretKey is the S3 secret access key.
	SecretKey string
	// Region is the signing region (MinIO accepts any; default us-east-1).
	Region string
	// HTTPClient is the HTTP client used for requests.
	HTTPClient *http.Client
}

// NewS3TestClientFromEnv builds an S3TestClient from MINIO_ADDR,
// MINIO_ACCESS_KEY and MINIO_SECRET_KEY with docker-compose defaults.
func NewS3TestClientFromEnv() *S3TestClient {
	return &S3TestClient{
		Endpoint:   getEnvOrDefault(EnvMinIOAddr, DefaultMinIOAddr),
		AccessKey:  getEnvOrDefault(EnvMinIOAccessKey, DefaultMinIOAccessKey),
		SecretKey:  getEnvOrDefault(EnvMinIOSecretKey, DefaultMinIOSecretKey),
		Region:     "us-east-1",
		HTTPClient: &http.Client{Timeout: 30 * time.Second},
	}
}

// IsAvailable reports whether the S3 endpoint responds within the context
// deadline. It uses the unauthenticated health endpoint exposed by MinIO.
func (c *S3TestClient) IsAvailable(ctx context.Context) bool {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet,
		strings.TrimSuffix(c.Endpoint, "/")+"/minio/health/live", nil)
	if err != nil {
		return false
	}
	resp, err := c.HTTPClient.Do(req)
	if err != nil {
		return false
	}
	defer func() { _ = resp.Body.Close() }()
	return resp.StatusCode == http.StatusOK
}

// BucketExists checks whether the bucket exists (HEAD bucket).
func (c *S3TestClient) BucketExists(ctx context.Context, bucket string) (bool, error) {
	resp, err := c.do(ctx, http.MethodHead, bucket, "", nil, nil)
	if err != nil {
		return false, err
	}
	defer func() { _ = resp.Body.Close() }()
	switch resp.StatusCode {
	case http.StatusOK:
		return true, nil
	case http.StatusNotFound:
		return false, nil
	default:
		return false, fmt.Errorf("head bucket %s: unexpected status %d", bucket, resp.StatusCode)
	}
}

// PutObject uploads body under bucket/key.
func (c *S3TestClient) PutObject(ctx context.Context, bucket, key string, body []byte) error {
	resp, err := c.do(ctx, http.MethodPut, bucket+"/"+key, "", nil, body)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		data, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("put object %s/%s: status %d: %s", bucket, key, resp.StatusCode, data)
	}
	return nil
}

// GetObject downloads the object at bucket/key.
func (c *S3TestClient) GetObject(ctx context.Context, bucket, key string) ([]byte, error) {
	resp, err := c.do(ctx, http.MethodGet, bucket+"/"+key, "", nil, nil)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		data, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("get object %s/%s: status %d: %s", bucket, key, resp.StatusCode, data)
	}
	return io.ReadAll(resp.Body)
}

// DeleteObject removes the object at bucket/key.
func (c *S3TestClient) DeleteObject(ctx context.Context, bucket, key string) error {
	resp, err := c.do(ctx, http.MethodDelete, bucket+"/"+key, "", nil, nil)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusNoContent && resp.StatusCode != http.StatusOK {
		data, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("delete object %s/%s: status %d: %s", bucket, key, resp.StatusCode, data)
	}
	return nil
}

// listBucketResult is the subset of the ListObjectsV2 response the tests use.
type listBucketResult struct {
	Contents []struct {
		Key  string `xml:"Key"`
		Size int64  `xml:"Size"`
	} `xml:"Contents"`
}

// ListObjects returns the keys under prefix in bucket (single page, up to 1000).
func (c *S3TestClient) ListObjects(ctx context.Context, bucket, prefix string) ([]string, error) {
	query := url.Values{}
	query.Set("list-type", "2")
	query.Set("prefix", prefix)

	resp, err := c.do(ctx, http.MethodGet, bucket, query.Encode(), nil, nil)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		data, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("list objects %s prefix=%s: status %d: %s", bucket, prefix, resp.StatusCode, data)
	}

	var result listBucketResult
	if err := xml.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("decoding list response: %w", err)
	}
	keys := make([]string, 0, len(result.Contents))
	for _, c := range result.Contents {
		keys = append(keys, c.Key)
	}
	return keys, nil
}

// do builds, signs (AWS SigV4) and executes a path-style S3 request.
func (c *S3TestClient) do(
	ctx context.Context,
	method, path, rawQuery string,
	headers map[string]string,
	body []byte,
) (*http.Response, error) {
	endpoint, err := url.Parse(c.Endpoint)
	if err != nil {
		return nil, fmt.Errorf("parsing endpoint %q: %w", c.Endpoint, err)
	}

	reqURL := *endpoint
	reqURL.Path = "/" + strings.TrimPrefix(path, "/")
	reqURL.RawQuery = rawQuery

	var reader io.Reader
	if body != nil {
		reader = bytes.NewReader(body)
	}
	req, err := http.NewRequestWithContext(ctx, method, reqURL.String(), reader)
	if err != nil {
		return nil, fmt.Errorf("building request: %w", err)
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}

	c.sign(req, body)
	return c.HTTPClient.Do(req)
}

// sign applies AWS Signature Version 4 to the request.
func (c *S3TestClient) sign(req *http.Request, body []byte) {
	now := time.Now().UTC()
	amzDate := now.Format("20060102T150405Z")
	dateStamp := now.Format("20060102")

	payloadHash := sha256.Sum256(body)
	payloadHexDigest := hex.EncodeToString(payloadHash[:])

	req.Header.Set("Host", req.URL.Host)
	req.Header.Set("X-Amz-Date", amzDate)
	req.Header.Set("X-Amz-Content-Sha256", payloadHexDigest)

	// Canonical headers (sorted, lowercase).
	signedHeaderNames := []string{"host", "x-amz-content-sha256", "x-amz-date"}
	sort.Strings(signedHeaderNames)
	var canonicalHeaders strings.Builder
	for _, h := range signedHeaderNames {
		value := req.Header.Get(h)
		if h == "host" {
			value = req.URL.Host
		}
		canonicalHeaders.WriteString(h + ":" + strings.TrimSpace(value) + "\n")
	}
	signedHeaders := strings.Join(signedHeaderNames, ";")

	canonicalRequest := strings.Join([]string{
		req.Method,
		canonicalURI(req.URL.Path),
		canonicalQuery(req.URL.Query()),
		canonicalHeaders.String(),
		signedHeaders,
		payloadHexDigest,
	}, "\n")

	scope := strings.Join([]string{dateStamp, c.Region, "s3", "aws4_request"}, "/")
	crHash := sha256.Sum256([]byte(canonicalRequest))
	stringToSign := strings.Join([]string{
		"AWS4-HMAC-SHA256",
		amzDate,
		scope,
		hex.EncodeToString(crHash[:]),
	}, "\n")

	signingKey := hmacSHA256(
		hmacSHA256(
			hmacSHA256(
				hmacSHA256([]byte("AWS4"+c.SecretKey), dateStamp),
				c.Region),
			"s3"),
		"aws4_request")
	signature := hex.EncodeToString(hmacSHA256(signingKey, stringToSign))

	req.Header.Set("Authorization", fmt.Sprintf(
		"AWS4-HMAC-SHA256 Credential=%s/%s, SignedHeaders=%s, Signature=%s",
		c.AccessKey, scope, signedHeaders, signature))
}

// hmacSHA256 computes HMAC-SHA256 of data with key.
func hmacSHA256(key []byte, data string) []byte {
	mac := hmac.New(sha256.New, key)
	mac.Write([]byte(data))
	return mac.Sum(nil)
}

// canonicalURI percent-encodes each path segment per SigV4 rules
// (slashes preserved, everything else strictly encoded).
func canonicalURI(path string) string {
	if path == "" {
		return "/"
	}
	segments := strings.Split(path, "/")
	for i, s := range segments {
		segments[i] = uriEncode(s)
	}
	return strings.Join(segments, "/")
}

// canonicalQuery builds the SigV4 canonical query string.
func canonicalQuery(values url.Values) string {
	keys := make([]string, 0, len(values))
	for k := range values {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var parts []string
	for _, k := range keys {
		vs := append([]string(nil), values[k]...)
		sort.Strings(vs)
		for _, v := range vs {
			parts = append(parts, uriEncode(k)+"="+uriEncode(v))
		}
	}
	return strings.Join(parts, "&")
}

// uriEncode implements the strict AWS URI encoding (RFC 3986 unreserved
// characters pass through, everything else is %XX upper-case hex).
func uriEncode(s string) string {
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		ch := s[i]
		switch {
		case ch >= 'a' && ch <= 'z', ch >= 'A' && ch <= 'Z',
			ch >= '0' && ch <= '9', ch == '-', ch == '_', ch == '.', ch == '~':
			b.WriteByte(ch)
		default:
			fmt.Fprintf(&b, "%%%02X", ch)
		}
	}
	return b.String()
}

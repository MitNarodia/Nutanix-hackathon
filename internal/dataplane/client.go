package dataplane

import (
	"context"
	"crypto/tls"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
	"time"

	"github.com/MitNarodia/Nutanix-hackathon/internal/auth"
	"github.com/MitNarodia/Nutanix-hackathon/internal/store"
)

var (
	ErrRateLimited     = errors.New("peer rate limited (429)")
	ErrUnauthorized    = errors.New("HMAC signature rejected (403)")
	ErrChunkNotFound   = errors.New("chunk not found (404)")
	ErrPeerUnreachable = errors.New("peer unreachable")
)

type RateLimitError struct {
	RetryAfter time.Duration
	PeerAddr   string
}

func (e *RateLimitError) Error() string {
	return fmt.Sprintf(
		"peer %s rate limited, retry after %v",
		e.PeerAddr,
		e.RetryAfter,
	)
}

func (e *RateLimitError) Unwrap() error {
	return ErrRateLimited
}

type Client struct {
	httpClient *http.Client
	signer     auth.Signer
	cas        *store.CASBlobStore
}

func NewClient(signer auth.Signer, cas *store.CASBlobStore) *Client {
	transport := &http.Transport{
		DialContext: (&net.Dialer{
			Timeout:   3 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		MaxIdleConns:        128,
		MaxIdleConnsPerHost: 16,
		IdleConnTimeout:     90 * time.Second,
		TLSClientConfig:     &tls.Config{InsecureSkipVerify: true},
	}

	return &Client{
		httpClient: &http.Client{
			Transport: transport,
			Timeout:   10 * time.Second,
		},
		signer: signer,
		cas:    cas,
	}
}

func (c *Client) FetchSingle(ctx context.Context, peerAddr string, hash [32]byte) error {
	hashHex := hex.EncodeToString(hash[:])
	url := fmt.Sprintf("http://%s/chunk?h=%s", peerAddr, hashHex)

	req, err := http.NewRequestWithContext(
		ctx,
		http.MethodGet,
		url,
		nil,
	)
	if err != nil {
		return fmt.Errorf(
			"%w: failed to create request: %v",
			ErrPeerUnreachable,
			err,
		)
	}

	c.signer.SignHTTP(req)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrPeerUnreachable, err)
	}
	defer resp.Body.Close()

	switch resp.StatusCode {
	case http.StatusOK:
		data, err := io.ReadAll(resp.Body)
		if err != nil {
			return fmt.Errorf("failed to read response: %w", err)
		}

		return c.cas.Put(hash, data)

	case http.StatusTooManyRequests:
		retrySecs := 2

		if value := resp.Header.Get("Retry-After"); value != "" {
			if n, err := strconv.Atoi(value); err == nil {
				retrySecs = n
			}
		}

		return &RateLimitError{
			RetryAfter: time.Duration(retrySecs) * time.Second,
			PeerAddr:   peerAddr,
		}

	case http.StatusForbidden:
		return ErrUnauthorized

	case http.StatusNotFound:
		return ErrChunkNotFound

	default:
		return fmt.Errorf(
			"unexpected HTTP status code: %d",
			resp.StatusCode,
		)
	}
}

func (c *Client) FetchMeta(ctx context.Context, peerAddr string, fileID string) (*store.FileMeta, error) {
	url := fmt.Sprintf("http://%s/meta?id=%s", peerAddr, fileID)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create meta request: %w", err)
	}

	// Zero-Trust: Sign the request
	c.signer.SignHTTP(req)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("peer unreachable: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("failed to fetch meta: HTTP %d", resp.StatusCode)
	}

	var meta store.FileMeta
	if err := json.NewDecoder(resp.Body).Decode(&meta); err != nil {
		return nil, fmt.Errorf("invalid meta JSON: %w", err)
	}

	return &meta, nil
}
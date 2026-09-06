package auth

import (
	"crypto/hmac"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"
)

const MaxClockSkew = 30 * time.Second

type Signer interface {
	SignHTTP(req *http.Request)
}

type Verifier interface {
	VerifyHTTP(req *http.Request) error
}

type hmacAuth struct {
	secret []byte
	nodeID string
}

func NewAuth(secret []byte, nodeID string) *hmacAuth {
	return &hmacAuth{
		secret: secret,
		nodeID: nodeID,
	}
}

func (a *hmacAuth) SignHTTP(req *http.Request) {
	ts := strconv.FormatInt(time.Now().Unix(), 10)

	text := fmt.Sprintf(
		"%s\n%s\n%s",
		req.Method,
		req.URL.RequestURI(),
		ts,
	)

	mac := hmac.New(sha256.New, a.secret)
	mac.Write([]byte(text))

	sig := base64.StdEncoding.EncodeToString(mac.Sum(nil))

	req.Header.Set("Authorization", "HMAC "+sig)
	req.Header.Set("X-NuSync-Timestamp", ts)
	req.Header.Set("X-NuSync-NodeID", a.nodeID)
}

func (a *hmacAuth) VerifyHTTP(req *http.Request) error {
	auth := req.Header.Get("Authorization")

	if !strings.HasPrefix(auth, "HMAC ") {
		return fmt.Errorf("missing or invalid authorization header")
	}

	sigText := strings.TrimPrefix(auth, "HMAC ")

	sig, err := base64.StdEncoding.DecodeString(sigText)
	if err != nil {
		return fmt.Errorf("invalid signature: %w", err)
	}

	ts := req.Header.Get("X-NuSync-Timestamp")

	t, err := strconv.ParseInt(ts, 10, 64)
	if err != nil {
		return fmt.Errorf("invalid timestamp: %w", err)
	}

	reqTime := time.Unix(t, 0)
	if time.Since(reqTime).Abs() > MaxClockSkew {
		return fmt.Errorf("request timestamp expired or from the future")
	}

	text := fmt.Sprintf(
		"%s\n%s\n%s",
		req.Method,
		req.URL.RequestURI(),
		ts,
	)

	mac := hmac.New(sha256.New, a.secret)
	mac.Write([]byte(text))

	expected := mac.Sum(nil)

	if subtle.ConstantTimeCompare(expected, sig) != 1 {
		return fmt.Errorf("HMAC signature mismatch")
	}

	return nil
}
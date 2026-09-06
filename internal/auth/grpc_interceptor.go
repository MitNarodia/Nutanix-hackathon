package auth

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"strconv"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func UnaryClientInterceptor(secret []byte, nodeID string) grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		ts := strconv.FormatInt(time.Now().Unix(), 10)
		canonical := fmt.Sprintf("GRPC\n%s\n%s", method, ts)

		mac := hmac.New(sha256.New, secret)
		mac.Write([]byte(canonical))

		sig := base64.StdEncoding.EncodeToString(mac.Sum(nil))

		ctx = metadata.AppendToOutgoingContext(
			ctx,
			"authorization", "HMAC "+sig,
			"x-nusync-timestamp", ts,
			"x-nusync-nodeid", nodeID,
		)

		return invoker(ctx, method, req, reply, cc, opts...)
	}
}

func UnaryServerInterceptor(secret []byte) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		md, ok := metadata.FromIncomingContext(ctx)
		if !ok {
			return nil, status.Error(codes.Unauthenticated, "missing metadata")
		}

		authVals := md.Get("authorization")
		tsVals := md.Get("x-nusync-timestamp")

		if len(authVals) == 0 || len(tsVals) == 0 {
			return nil, status.Error(
				codes.Unauthenticated,
				"missing authorization metadata",
			)
		}

		ts, err := strconv.ParseInt(tsVals[0], 10, 64)
		if err != nil || time.Since(time.Unix(ts, 0)).Abs() > 30*time.Second {
			return nil, status.Error(
				codes.Unauthenticated,
				"timestamp expired or invalid",
			)
		}

		canonical := fmt.Sprintf(
			"GRPC\n%s\n%s",
			info.FullMethod,
			tsVals[0],
		)

		mac := hmac.New(sha256.New, secret)
		mac.Write([]byte(canonical))

		expectedSig := mac.Sum(nil)

		var clientSig []byte
		fmt.Sscanf(authVals[0], "HMAC %s", &clientSig)

		decodedClientSig, err := base64.StdEncoding.DecodeString(string(clientSig))
		if err != nil || !hmac.Equal(expectedSig, decodedClientSig) {
			return nil, status.Error(
				codes.PermissionDenied,
				"HMAC signature mismatch",
			)
		}

		return handler(ctx, req)
	}
}
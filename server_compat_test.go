package fujin_go_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"fmt"
	"log/slog"
	"math/big"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	client "github.com/fujin-io/fujin-go"
	nativeclient "github.com/fujin-io/fujin-go/fujin"
	grpcclient "github.com/fujin-io/fujin-go/grpc/v1"
	"github.com/fujin-io/fujin-go/internal/nativeproto"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func TestServerCompatibility(t *testing.T) {
	serverRoot := os.Getenv("FUJIN_SERVER_ROOT")
	if serverRoot == "" {
		t.Skip("set FUJIN_SERVER_ROOT to run compatibility against a Fujin server checkout")
	}
	serverRoot, err := filepath.Abs(serverRoot)
	require.NoError(t, err)

	tempDir := t.TempDir()
	binary := filepath.Join(tempDir, "fujin")
	build := exec.Command("go", "run", "./cmd/builder",
		"-local",
		"-configurator", "github.com/fujin-io/fujin/public/plugins/configurator/yaml",
		"-connector", "github.com/fujin-io/fujin/examples/plugins/connector/faker",
		"-transport", "github.com/fujin-io/fujin/public/plugins/transport/quic",
		"-tags", "fujin,grpc",
		"-ldflags", "-X main.Version=v0.2.0",
		"-output", binary,
	)
	build.Dir = serverRoot
	output, err := build.CombinedOutput()
	require.NoError(t, err, "build compatibility server:\n%s", output)

	certPath, keyPath := writeServerCertificate(t, tempDir)
	quicPort := freePort(t, "udp")
	grpcPort := freePort(t, "tcp")
	configPath := filepath.Join(tempDir, "config.yaml")
	config := fmt.Sprintf(`fujin:
  transports:
    - type: quic
      settings:
        addr: "127.0.0.1:%d"
        max_incoming_streams: 32
        max_idle_timeout: 30s
        tls:
          enabled: true
          server_cert_pem_path: %q
          server_key_pem_path: %q
        fujin:
          ping_interval: 1h
          ping_timeout: 5s
          ping_max_retries: 3
          write_deadline: 5s
          force_terminate_timeout: 5s
grpc:
  enabled: true
  addr: "127.0.0.1:%d"
connectors:
  compatibility:
    type: faker
    settings: {}
`, quicPort, certPath, keyPath, grpcPort)
	require.NoError(t, os.WriteFile(configPath, []byte(config), 0o600))

	var serverOutput bytes.Buffer
	command := exec.Command(binary)
	command.Env = append(os.Environ(),
		"FUJIN_CONFIGURATOR=yaml",
		"FUJIN_CONFIGURATOR_YAML_PATHS="+configPath,
		"FUJIN_UPGRADE_SOCK="+filepath.Join(tempDir, "upgrade.sock"),
		"FUJIN_LOG_LEVEL=ERROR",
	)
	command.Stdout = &serverOutput
	command.Stderr = &serverOutput
	require.NoError(t, command.Start())
	done := make(chan error, 1)
	go func() { done <- command.Wait() }()
	t.Cleanup(func() { stopCompatibilityServer(t, command, done, &serverOutput) })

	nativeAddr := fmt.Sprintf("127.0.0.1:%d", quicPort)
	grpcAddr := fmt.Sprintf("127.0.0.1:%d", grpcPort)
	require.Eventually(t, func() bool {
		return nativeCompatibilityCheck(nativeAddr) == nil && grpcCompatibilityCheck(grpcAddr) == nil
	}, 30*time.Second, 100*time.Millisecond, "server did not become compatible:\n%s", serverOutput.String())

	require.NoError(t, nativeCompatibilityCheck(nativeAddr))
	require.NoError(t, grpcCompatibilityCheck(grpcAddr))

	require.Contains(t, serverOutput.String(), "version: v0.2.0")
	require.Equal(t, "fujin/1", nativeproto.Version)
}

func nativeCompatibilityCheck(addr string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	conn, err := nativeclient.Dial(ctx, addr, &tls.Config{InsecureSkipVerify: true}, nil)
	if err != nil {
		return err
	}
	defer conn.Close()
	stream, err := conn.Bind(ctx, "compatibility")
	if err != nil {
		return err
	}
	defer stream.Close(context.Background())
	if err := verifyCompatibilityStream(ctx, stream); err != nil {
		return fmt.Errorf("native: %w", err)
	}
	return nil
}

func grpcCompatibilityCheck(addr string) error {
	conn, err := grpcclient.Dial(addr, slog.Default(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	defer conn.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	stream, err := conn.Bind(ctx, "compatibility")
	if err != nil {
		return err
	}
	defer stream.Close(context.Background())
	if err := verifyCompatibilityStream(ctx, stream); err != nil {
		return fmt.Errorf("grpc: %w", err)
	}
	return nil
}

func verifyCompatibilityStream(ctx context.Context, stream client.Stream) error {
	profile, ok := stream.Routes()["default"]
	if !ok {
		return errors.New("BIND response omitted default route")
	}
	if !profile.Produce || !profile.Headers || !profile.Transactions || profile.ProduceGuarantee != client.ProduceGuaranteeLocalAccept {
		return fmt.Errorf("unexpected route capabilities: %+v", profile)
	}
	if err := stream.Produce(ctx, "default", []byte("compatibility")); err != nil {
		return err
	}
	if err := stream.HProduce(ctx, "default", []byte("compatibility"), []client.Header{{Key: []byte("content-type"), Value: []byte("text/plain")}}); err != nil {
		return err
	}
	if err := stream.BeginTx(ctx, "default"); err != nil {
		return err
	}
	if err := stream.TxProduce(ctx, []byte("transactional")); err != nil {
		return err
	}
	if err := stream.CommitTx(ctx); err != nil {
		return err
	}
	err := stream.Produce(ctx, "missing", []byte("error"))
	var operationErr *client.OperationError
	if !errors.As(err, &operationErr) {
		return fmt.Errorf("missing route returned %T: %v", err, err)
	}
	if operationErr.Code != client.StatusNotFound || operationErr.Outcome != client.OutcomeNotApplied || operationErr.Reason == "" {
		return fmt.Errorf("unexpected structured error: %+v", operationErr)
	}
	return nil
}

func freePort(t *testing.T, network string) int {
	t.Helper()
	if network == "udp" {
		listener, err := net.ListenPacket("udp", "127.0.0.1:0")
		require.NoError(t, err)
		defer listener.Close()
		return listener.LocalAddr().(*net.UDPAddr).Port
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer listener.Close()
	return listener.Addr().(*net.TCPAddr).Port
}

func writeServerCertificate(t *testing.T, directory string) (string, string) {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	template := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "localhost"},
		NotBefore:    time.Now().Add(-time.Minute),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		DNSNames:     []string{"localhost"},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
	}
	certificate, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	require.NoError(t, err)
	certPath := filepath.Join(directory, "server.pem")
	keyPath := filepath.Join(directory, "server-key.pem")
	require.NoError(t, os.WriteFile(certPath, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certificate}), 0o600))
	require.NoError(t, os.WriteFile(keyPath, pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)}), 0o600))
	return certPath, keyPath
}

func stopCompatibilityServer(t *testing.T, command *exec.Cmd, done <-chan error, output *bytes.Buffer) {
	t.Helper()
	_ = command.Process.Signal(os.Interrupt)
	select {
	case err := <-done:
		if err != nil && !strings.Contains(err.Error(), "signal: interrupt") {
			t.Errorf("compatibility server exit: %v\n%s", err, output.String())
		}
	case <-time.After(15 * time.Second):
		_ = command.Process.Kill()
		<-done
		t.Errorf("compatibility server did not stop\n%s", output.String())
	}
}

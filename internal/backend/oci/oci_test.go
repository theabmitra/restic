package oci_test

import (
	"bytes"
	"context"
	"fmt"
	"github.com/restic/restic/internal/backend"
	"github.com/restic/restic/internal/backend/oci"
	"github.com/restic/restic/internal/backend/test"
	"github.com/restic/restic/internal/restic"
	rtest "github.com/restic/restic/internal/test"
	"io"
	"os"
	"testing"
	"time"
)

func newOCITestSuite() *test.Suite[oci.Config] {
	return &test.Suite[oci.Config]{
		// do not use excessive data
		MinimalData: true,

		// NewConfig returns a config for a new temporary backend that will be used in tests.
		NewConfig: func() (*oci.Config, error) {
			cfg, err := oci.ParseConfig(os.Getenv("RESTIC_TEST_OCI_REPOSITORY"))
			if err != nil {
				return nil, err
			}

			cfg.ApplyEnvironment("RESTIC_TEST_")
			cfg.Prefix = fmt.Sprintf("test-%d", time.Now().UnixNano())
			return cfg, nil
		},

		Factory: oci.NewFactory(),
	}
}

func TestUploadLargeFile(t *testing.T) {

	vars := []string{
		"RESTIC_TEST_OCI_REGION",
		"RESTIC_TEST_OCI_USER",
		"RESTIC_TEST_OCI_FINGERPRINT",
		"RESTIC_TEST_OCI_KEY_FILE",
		"RESTIC_TEST_OCI_TENANCY",
		"RESTIC_TEST_OCI_COMPARTMENT_OCID",
		"RESTIC_TEST_OCI_REPOSITORY",
		"RESTIC_OCI_TEST_LARGE_UPLOAD",
	}

	for _, v := range vars {
		if os.Getenv(v) == "" {
			t.Skipf("environment variable %v not set", v)
			return
		}
	}

	ctx, cancel := context.WithCancel(context.TODO())
	defer cancel()

	cfg, err := oci.ParseConfig(os.Getenv("RESTIC_TEST_OCI_REPOSITORY"))
	if err != nil {
		t.Fatal(err)
	}
	cfg.ApplyEnvironment("RESTIC_TEST_")
	cfg.Prefix = fmt.Sprintf("test-upload-large-%d", time.Now().UnixNano())

	tr, err := backend.Transport(backend.TransportOptions{})
	if err != nil {
		t.Fatal(err)
	}

	be, err := oci.Create(ctx, *cfg, tr)
	if err != nil {
		t.Fatal(err)
	}

	defer func() {
		err := be.Delete(ctx)
		if err != nil {
			t.Fatal(err)
		}
	}()

	data := rtest.Random(23, 300*1024*1024)
	id := restic.Hash(data)
	h := restic.Handle{Name: id.String(), Type: restic.PackFile}

	t.Logf("hash of %d bytes: %v", len(data), id)

	err = be.Save(ctx, h, restic.NewByteReader(data, be.Hasher()))
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		err := be.Remove(ctx, h)
		if err != nil {
			t.Fatal(err)
		}
	}()

	var tests = []struct {
		offset, length int
	}{
		{0, len(data)},
		{23, 1024},
		{23 + 100*1024, 500},
		{888 + 200*1024, 89999},
		{888 + 100*1024*1024, 120 * 1024 * 1024},
	}

	for _, test := range tests {
		t.Run("", func(t *testing.T) {
			want := data[test.offset : test.offset+test.length]

			buf := make([]byte, test.length)
			err = be.Load(ctx, h, test.length, int64(test.offset), func(rd io.Reader) error {
				_, err = io.ReadFull(rd, buf)
				return err
			})
			if err != nil {
				t.Fatal(err)
			}

			if !bytes.Equal(buf, want) {
				t.Fatalf("wrong bytes returned")
			}
		})
	}
}

func TestBackendOCI(t *testing.T) {
	defer func() {
		if t.Skipped() {
			rtest.SkipDisallowed(t, "restic/backend/oci.TestBackendOCI")
		}
	}()

	vars := []string{
		"RESTIC_TEST_OCI_REGION",
		"RESTIC_TEST_OCI_USER",
		"RESTIC_TEST_OCI_FINGERPRINT",
		"RESTIC_TEST_OCI_KEY_FILE",
		"RESTIC_TEST_OCI_TENANCY",
		"RESTIC_TEST_OCI_COMPARTMENT_OCID",
		"RESTIC_TEST_OCI_REPOSITORY",
	}

	for _, v := range vars {
		if os.Getenv(v) == "" {
			t.Skipf("environment variable %v not set", v)
			return
		}
	}

	t.Logf("run tests")
	newOCITestSuite().RunTests(t)
}

func BenchmarkBackendOCI(t *testing.B) {
	vars := []string{
		"RESTIC_TEST_OCI_REGION",
		"RESTIC_TEST_OCI_USER",
		"RESTIC_TEST_OCI_FINGERPRINT",
		"RESTIC_TEST_OCI_KEY_FILE",
		"RESTIC_TEST_OCI_TENANCY",
		"RESTIC_TEST_OCI_COMPARTMENT_OCID",
		"RESTIC_TEST_OCI_REPOSITORY",
	}

	for _, v := range vars {
		if os.Getenv(v) == "" {
			t.Skipf("environment variable %v not set", v)
			return
		}
	}

	t.Logf("run benchmark tests")
	newOCITestSuite().RunBenchmarks(t)
}

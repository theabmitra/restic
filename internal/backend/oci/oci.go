package oci

import (
	"context"
	"fmt"
	"github.com/oracle/oci-go-sdk/v65/common"
	"github.com/oracle/oci-go-sdk/v65/common/auth"
	"github.com/oracle/oci-go-sdk/v65/objectstorage"
	"github.com/oracle/oci-go-sdk/v65/objectstorage/transfer"
	"github.com/restic/restic/internal/backend"
	"github.com/restic/restic/internal/backend/layout"
	"github.com/restic/restic/internal/backend/location"
	"github.com/restic/restic/internal/debug"
	"github.com/restic/restic/internal/errors"
	"github.com/restic/restic/internal/options"
	"github.com/restic/restic/internal/restic"
	"hash"
	"io"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"strings"
)

// Backend stores data on an OCI object store.
type Backend struct {
	client      objectstorage.ObjectStorageClient
	cfg         Config
	connections uint
	layout.Layout
}

// make sure that *Backend implements backend.Backend
var _ restic.Backend = &Backend{}

func NewFactory() location.Factory {
	return location.NewHTTPBackendFactory("oci", ParseConfig, location.NoPassword, Create, Open)
}

func open(cfg Config, rt http.RoundTripper) (*Backend, error) {

	debug.Log("open, config %#v", cfg)

	switch cfg.OCIAuthType {
	case InstancePrincipal:
		if cfg.CompartmentOCID == "" {
			return nil, errors.Fatalf("unable to authenticate OCI object store: Tenancy ID ($OCI_COMPARTMENT_OCID) is empty")
		}

	case WorkloadPrincipal:
		if cfg.Region == "" {
			return nil, errors.Fatalf("unable to authenticate OCI object store: Tenancy ID ($OCI_REGION) is empty")
		}
		if err := os.Setenv(auth.ResourcePrincipalVersionEnvVar, auth.ResourcePrincipalVersion2_2); err != nil {
			return nil, errors.Fatalf("unable to set OCI SDK environment variable: %s\n", auth.ResourcePrincipalVersionEnvVar)
		}
		if err := os.Setenv(auth.ResourcePrincipalRegionEnvVar, cfg.Region); err != nil {
			return nil, errors.Fatalf("unable to set OCI SDK environment variable: %s\n", auth.ResourcePrincipalRegionEnvVar)
		}

	case UserPrincipal:
		if cfg.Region == "" {
			return nil, errors.Fatalf("unable to authenticate OCI object store: Tenancy ID ($OCI_REGION) is empty")
		}
		if cfg.TenancyID == "" {
			return nil, errors.Fatalf("unable to authenticate OCI object store: Tenancy ID ($OCI_TENANCY) is empty")
		}
		if cfg.UserID == "" {
			return nil, errors.Fatalf("unable to authenticate OCI object store: User ID ($OCI_USER) is empty")
		}
		if cfg.Fingerprint == "" {
			return nil, errors.Fatalf("unable to authenticate OCI object store: FingerPrint ($OCI_FINGERPRINT) is empty")
		}
		if cfg.PrivateKeyFile == "" {
			return nil, errors.Fatalf("unable to authenticate OCI object store: Private key file path ($OCI_KEY_FILE) is empty")
		}
		if cfg.CompartmentOCID == "" {
			return nil, errors.Fatalf("unable to authenticate OCI object store: Tenancy ID ($OCI_COMPARTMENT_OCID) is empty")
		}
		_, err := os.Stat(filepath.Clean(cfg.PrivateKeyFile))
		if err != nil {
			return nil, errors.Fatalf("Unable to find private key file for provider OCI: OCI_KEY_FILE")
		}

		keyData, err := os.ReadFile(filepath.Clean(cfg.PrivateKeyFile))
		if err != nil {
			return nil, errors.Fatalf("Unable to find private key file for provider OCI: OCI_KEY_FILE")
		}
		cfg.PrivateKey = options.NewSecretString(string(keyData))
	}

	ociAuthConfigProvider, err := NewConfigurationProvider(&cfg)
	if err != nil {
		debug.Log("Error %v", err)
		return nil, err
	}

	c, err := objectstorage.NewObjectStorageClientWithConfigurationProvider(ociAuthConfigProvider)
	if err != nil {
		debug.Log("Error %v", err)
		return nil, err
	}
	c.HTTPClient = &http.Client{Transport: rt}

	be := &Backend{
		client:      c,
		cfg:         cfg,
		connections: cfg.Connections,
		Layout: &layout.DefaultLayout{
			Path: cfg.Prefix,
			Join: path.Join,
		},
	}
	return be, nil
}

// Open opens the OCI backend at bucket and region. The bucket is created if it
// does not exist yet.
func Open(_ context.Context, cfg Config, rt http.RoundTripper) (restic.Backend, error) {
	return open(cfg, rt)
}

// Create opens the OCI backend at bucket and region and creates the bucket if
// it does not exist yet.
func Create(ctx context.Context, cfg Config, rt http.RoundTripper) (restic.Backend, error) {
	be, err := open(cfg, rt)
	if err != nil {
		return nil, errors.Wrap(err, "open")
	}

	ociNamespace, err := getOCINamespace(ctx, be.client)
	if err != nil {
		return nil, err
	}

	err = ensureBucketExists(ctx, be.client, ociNamespace, cfg.BucketName, cfg.CompartmentOCID)
	if err != nil {
		return nil, err
	}
	return be, nil
}

// IsNotExist returns true if the error is caused by a not existing file.
func (be *Backend) IsNotExist(err error) bool {
	var e common.ServiceError
	return errors.As(err, &e) && e.GetHTTPStatusCode() == 404
}

// Join combines path components with slashes.
func (be *Backend) Join(p ...string) string {
	return path.Join(p...)
}

func (be *Backend) Connections() uint {
	return be.cfg.Connections
}

// Location returns this backend's location (the bucket name).
func (be *Backend) Location() string {
	return be.Join(be.cfg.BucketName, be.cfg.Prefix)
}

// Hasher may return a hash function for calculating a content hash for the backend
func (be *Backend) Hasher() hash.Hash {
	return nil
}

// HasAtomicReplace returns whether Save() can atomically replace files
func (be *Backend) HasAtomicReplace() bool {
	return true
}

// Path returns the path in the bucket that is used for this backend.
func (be *Backend) Path() string {
	return be.cfg.Prefix
}

// Save stores data in the backend at the handle.
func (be *Backend) Save(ctx context.Context, h restic.Handle, rd restic.RewindReader) error {
	objName := be.Filename(h)
	ociNamespace, err := getOCINamespace(ctx, be.client)
	if err != nil {
		return err
	}

	uploadManager := transfer.NewUploadManager()
	req := transfer.UploadStreamRequest{
		UploadRequest: transfer.UploadRequest{
			NamespaceName:                       common.String(ociNamespace),
			BucketName:                          common.String(be.cfg.BucketName),
			ObjectName:                          common.String(objName),
			EnableMultipartChecksumVerification: common.Bool(true),
			AllowMultipartUploads:               common.Bool(true),
			AllowParrallelUploads:               common.Bool(true),
			ObjectStorageClient:                 &be.client,
			ContentType:                         common.String(ContentType),
		},
		StreamReader: io.NopCloser(rd),
	}
	_, err = uploadManager.UploadStream(ctx, req)

	// sanity check
	if err == nil {
		getObjectDetails, err := be.client.HeadObject(ctx, objectstorage.HeadObjectRequest{
			NamespaceName: common.String(ociNamespace),
			BucketName:    common.String(be.cfg.BucketName),
			ObjectName:    common.String(objName),
		})
		if err != nil {
			return errors.Wrap(err, "client.fetch getResponse")
		}
		size := SafeDeref[int64](getObjectDetails.ContentLength)
		if size != rd.Length() {
			return errors.Errorf("wrote %d bytes instead of the expected %d bytes", size, rd.Length())
		}
	}
	return errors.Wrap(err, "client.UploadStreamRequest")
}

// Load runs fn with a reader that yields the contents of the file at h at the
// given offset.
func (be *Backend) Load(ctx context.Context, h restic.Handle, length int, offset int64, fn func(rd io.Reader) error) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	return backend.DefaultLoad(ctx, h, length, offset, be.openReader, fn)
}

func (be *Backend) openReader(ctx context.Context, h restic.Handle, length int, offset int64) (io.ReadCloser, error) {
	objName := be.Filename(h)

	var err error
	var bytesRange string
	if length > 0 {
		bytesRange, err = getRange(offset, offset+int64(length)-1)
	} else if offset > 0 {
		bytesRange, err = getRange(offset, 0)
	}
	if err != nil {
		return nil, err
	}

	var request objectstorage.GetObjectRequest

	ociNamespace, err := getOCINamespace(ctx, be.client)
	if err != nil {
		return nil, err
	}

	if bytesRange == "" {
		request = objectstorage.GetObjectRequest{
			NamespaceName: common.String(ociNamespace),
			BucketName:    common.String(be.cfg.BucketName),
			ObjectName:    common.String(objName),
		}
	} else {
		request = objectstorage.GetObjectRequest{
			NamespaceName: common.String(ociNamespace),
			BucketName:    common.String(be.cfg.BucketName),
			ObjectName:    common.String(objName),
			// This is the parameter where you control the download size/request
			//Range: common.String("bytes=" + bytesRangeStr),
			Range: common.String(bytesRange),
		}
	}

	resp, err := be.client.GetObject(ctx, request)
	if err != nil {
		return nil, err
	}
	
	return io.NopCloser(resp.Content), nil
}

// Stat returns information about a blob.
func (be *Backend) Stat(ctx context.Context, h restic.Handle) (bi restic.FileInfo, err error) {
	objName := be.Filename(h)
	ociNamespace, err := getOCINamespace(ctx, be.client)
	if err != nil {
		return restic.FileInfo{}, err
	}

	getObjectDetails, err := be.client.HeadObject(ctx, objectstorage.HeadObjectRequest{
		NamespaceName: common.String(ociNamespace),
		BucketName:    common.String(be.cfg.BucketName),
		ObjectName:    common.String(objName),
	})
	if err != nil {
		return restic.FileInfo{}, errors.Wrap(err, "Stat")
	}
	if getObjectDetails.RawResponse.StatusCode == 404 {
		return restic.FileInfo{}, errors.Wrap(err, "File not found")
	}

	objNameSlice := strings.Split(objName, "/")
	return restic.FileInfo{Size: SafeDeref[int64](getObjectDetails.ContentLength), Name: objNameSlice[len(objNameSlice)-1]}, nil
}

// Remove removes the blob with the given name and type.
func (be *Backend) Remove(ctx context.Context, h restic.Handle) error {
	objName := be.Filename(h)

	ociNamespace, err := getOCINamespace(ctx, be.client)
	if err != nil {
		return err
	}

	err = deleteObject(ctx, be.client, ociNamespace, be.cfg.BucketName, objName)

	if be.IsNotExist(err) {
		err = nil
	}

	return errors.Wrap(err, "client.RemoveObject")
}

// List runs fn for each file in the backend which has the type t. When an
// error occurs (or fn returns an error), List stops and returns it.
func (be *Backend) List(ctx context.Context, t restic.FileType, fn func(restic.FileInfo) error) error {
	prefix, _ := be.Basedir(t)
	// make sure prefix ends with a slash
	if !strings.HasSuffix(prefix, "/") {
		prefix += "/"
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	ociNamespace, err := getOCINamespace(ctx, be.client)
	if err != nil {
		return err
	}

	listresp, err := be.client.ListObjects(ctx, objectstorage.ListObjectsRequest{
		NamespaceName: common.String(ociNamespace),
		BucketName:    common.String(be.cfg.BucketName),
		Prefix:        common.String(prefix),
	})
	if err != nil {
		return err
	}

	for _, obj := range listresp.Objects {
		name := strings.TrimPrefix(SafeDeref[string](obj.Name), prefix)
		if name == "" {
			continue
		}

		getObjectDetails, err := be.client.HeadObject(ctx, objectstorage.HeadObjectRequest{
			NamespaceName: common.String(ociNamespace),
			BucketName:    common.String(be.cfg.BucketName),
			ObjectName:    common.String(SafeDeref[string](obj.Name)),
		})
		if err != nil {
			return err
		}

		fi := restic.FileInfo{
			Name: path.Base(name),
			Size: SafeDeref[int64](getObjectDetails.ContentLength),
		}

		if ctx.Err() != nil {
			return ctx.Err()
		}

		err = fn(fi)
		if err != nil {
			return err
		}

		if ctx.Err() != nil {
			return ctx.Err()
		}
	}

	return ctx.Err()
}

// Delete removes all restic keys in the bucket. It will not remove the bucket itself.
func (be *Backend) Delete(ctx context.Context) error {
	return backend.DefaultDelete(ctx, be)
}

// Close does nothing
func (be *Backend) Close() error { return nil }

// Rename moves a file based on the new layout l.
func (be *Backend) Rename(ctx context.Context, h restic.Handle, l layout.Layout) error {
	debug.Log("Rename %v to %v", h, l)
	oldname := be.Filename(h)
	newname := l.Filename(h)

	if oldname == newname {
		debug.Log("  %v is already renamed", newname)
		return nil
	}

	debug.Log("  %v -> %v", oldname, newname)

	ociNamespace, err := getOCINamespace(ctx, be.client)
	if err != nil {
		return err
	}

	_, err = be.client.CopyObject(ctx, objectstorage.CopyObjectRequest{
		NamespaceName: common.String(ociNamespace),
		BucketName:    common.String(be.cfg.BucketName),
		CopyObjectDetails: objectstorage.CopyObjectDetails{
			SourceObjectName:      common.String(oldname),
			DestinationObjectName: common.String(newname),
			DestinationBucket:     common.String(be.cfg.BucketName),
			DestinationRegion:     common.String(be.cfg.Region),
			DestinationNamespace:  common.String(ociNamespace),
		},
	})
	if err != nil && be.IsNotExist(err) {
		debug.Log("copy failed: %v, seems to already have been renamed", err)
		return nil
	}

	if err != nil {
		debug.Log("copy failed: %v", err)
		return err
	}
	return deleteObject(ctx, be.client, ociNamespace, be.cfg.BucketName, oldname)
}

// ensureBucketExists checks for existence of bucket inside the compartment.
func ensureBucketExists(ctx context.Context, client objectstorage.ObjectStorageClient, namespace, name string, compartmentOCID string) error {
	req := objectstorage.GetBucketRequest{
		NamespaceName: &namespace,
		BucketName:    &name,
	}
	// verify if bucket exists
	response, err := client.GetBucket(context.Background(), req)
	if err != nil {
		if 404 == response.RawResponse.StatusCode {
			return createBucket(ctx, client, namespace, name, compartmentOCID)
		}
	}
	return nil
}

// createBucket creates a bucket in a compartment.
// bucketname needs to be unique within compartment. there is no concept of "child" buckets.
func createBucket(ctx context.Context, client objectstorage.ObjectStorageClient, namespace string, name string, compartmentOCID string) error {
	request := objectstorage.CreateBucketRequest{
		NamespaceName: &namespace,
	}
	request.CompartmentId = common.String(compartmentOCID)
	request.Name = common.String(name)
	request.Metadata = make(map[string]string)
	request.PublicAccessType = objectstorage.CreateBucketDetailsPublicAccessTypeNopublicaccess
	_, err := client.CreateBucket(ctx, request)
	if err != nil {
		return err
	}
	return nil
}

// getOCINamespace fetches the tenancy namespace to be used by the OCI object store client
func getOCINamespace(ctx context.Context, client objectstorage.ObjectStorageClient) (string, error) {
	request := objectstorage.GetNamespaceRequest{}
	r, err := client.GetNamespace(ctx, request)
	if err != nil {
		return "", errors.Wrap(err, "unable to fetch namespace")
	}
	return SafeDeref[string](r.Value), nil
}

// deleteObject deletes an objet from OCI object store
func deleteObject(ctx context.Context, c objectstorage.ObjectStorageClient, namespace, bucketname, objectname string) error {
	request := objectstorage.DeleteObjectRequest{
		NamespaceName: common.String(namespace),
		BucketName:    common.String(bucketname),
		ObjectName:    common.String(objectname),
	}
	_, err := c.DeleteObject(ctx, request)
	if err != nil {
		return err
	}
	return nil
}

// SafeDeref returns the de-refernced value of a pointer variable and takes into account when pointer is nil.
func SafeDeref[T any](p *T) T {
	if p == nil {
		var v T
		return v
	}
	return *p
}

// getRange returns the byte range string used to download files from OCI object store.
func getRange(start, end int64) (string, error) {
	var byteRange string
	switch {
	case start == 0 && end < 0:
		// Read last '-end' bytes. `bytes=-N`.
		byteRange = fmt.Sprintf("bytes=%d", end)
	case 0 < start && end == 0:
		// Read everything starting from offset
		// 'start'. `bytes=N-`.
		byteRange = fmt.Sprintf("bytes=%d-", start)
	case 0 <= start && start <= end:
		// Read everything starting at 'start' till the
		// 'end'. `bytes=N-M`
		byteRange = fmt.Sprintf("bytes=%d-%d", start, end)
	default:
		// All other cases such as
		// bytes=-3-
		// bytes=5-3
		// bytes=-2-4
		// bytes=-3-0
		// bytes=-3--2
		// are invalid.
		return "bytes=-1", errors.New(fmt.Sprintf("Invalid range specified: start=%d end=%d", start, end))
	}
	return byteRange, nil
}

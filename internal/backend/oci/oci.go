package oci

import (
	"bytes"
	"context"
	"fmt"
	"github.com/oracle/oci-go-sdk/v65/common"
	"github.com/oracle/oci-go-sdk/v65/objectstorage"
	"github.com/restic/restic/internal/backend"
	"github.com/restic/restic/internal/backend/layout"
	"github.com/restic/restic/internal/backend/location"
	"github.com/restic/restic/internal/debug"
	"github.com/restic/restic/internal/errors"
	"github.com/restic/restic/internal/restic"
	"hash"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"path"
	"strings"
	"time"
)

// Backend stores data on an OCI object store.
type Backend struct {
	client objectstorage.ObjectStorageClient
	cfg    Config
	layout.Layout
}

// make sure that *Backend implements backend.Backend
var _ restic.Backend = &Backend{}

func NewFactory() location.Factory {
	return location.NewHTTPBackendFactory("oci", ParseConfig, location.NoPassword, Create, Open)
}

const defaultLayout = "default"

func open(ctx context.Context, cfg Config, rt http.RoundTripper) (*Backend, error) {

	debug.Log("open, config %#v", cfg)

	// if instance principal is not set then set the user principal values
	if !cfg.UseInstancePrincipals {
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
		if cfg.Region == "" {
			return nil, errors.Fatalf("unable to authenticate OCI object store: Tenancy ID ($OCI_REGION) is empty")
		}
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

	be := &Backend{
		client: c,
		cfg:    cfg,
	}

	l, err := layout.ParseLayout(ctx, be, cfg.Layout, defaultLayout, cfg.Prefix)
	if err != nil {
		return nil, err
	}

	be.Layout = l

	return be, nil
}

// Open opens the OCI backend at bucket and region. The bucket is created if it
// does not exist yet.
func Open(ctx context.Context, cfg Config, rt http.RoundTripper) (restic.Backend, error) {
	return open(ctx, cfg, rt)
}

// Create opens the OCI backend at bucket and region and creates the bucket if
// it does not exist yet.
func Create(ctx context.Context, cfg Config, rt http.RoundTripper) (restic.Backend, error) {
	be, err := open(ctx, cfg, rt)
	if err != nil {
		return nil, errors.Wrap(err, "open")
	}

	err = ensureBucketExists(ctx, be.client, getNamespace(ctx, be.client), cfg.Bucket, cfg.CompartmentOCID)
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

type fileInfo struct {
	name    string
	size    int64
	mode    os.FileMode
	modTime time.Time
	isDir   bool
}

func (fi *fileInfo) Name() string       { return fi.name }    // base name of the file
func (fi *fileInfo) Size() int64        { return fi.size }    // length in bytes for regular files; system-dependent for others
func (fi *fileInfo) Mode() os.FileMode  { return fi.mode }    // file mode bits
func (fi *fileInfo) ModTime() time.Time { return fi.modTime } // modification time
func (fi *fileInfo) IsDir() bool        { return fi.isDir }   // abbreviation for Mode().IsDir()
func (fi *fileInfo) Sys() interface{}   { return nil }        // underlying data source (can return nil)

// ReadDir returns the entries for a directory.
func (be *Backend) ReadDir(ctx context.Context, dir string) (list []os.FileInfo, err error) {
	debug.Log("ReadDir(%v)", dir)

	// make sure dir ends with a slash
	if dir[len(dir)-1] != '/' {
		dir += "/"
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	listresp, err := be.client.ListObjects(ctx, objectstorage.ListObjectsRequest{
		NamespaceName: common.String(getNamespace(ctx, be.client)),
		BucketName:    common.String(be.cfg.Bucket),
		Prefix:        common.String(dir),
	})
	if err != nil {
		return nil, err
	}

	for _, obj := range listresp.Objects {
		name := strings.TrimPrefix(SafeDeref[string](obj.Name), dir)
		if name == "" {
			continue
		}

		getResponse, err := be.client.GetObject(ctx, objectstorage.GetObjectRequest{
			NamespaceName: common.String(getNamespace(ctx, be.client)),
			BucketName:    common.String(be.cfg.Bucket),
			ObjectName:    common.String(name),
		})
		if err != nil {
			return nil, err
		}

		entry := &fileInfo{
			name:    name,
			size:    SafeDeref[int64](getResponse.ContentLength),
			modTime: getResponse.LastModified.Time,
		}

		if name[len(name)-1] == '/' {
			entry.isDir = true
			entry.mode = os.ModeDir | 0755
			entry.name = name[:len(name)-1]
		} else {
			entry.mode = 0644
		}

		list = append(list, entry)
	}

	return list, nil
}

func (be *Backend) Connections() uint {
	return be.cfg.Connections
}

// Location returns this backend's location (the bucket name).
func (be *Backend) Location() string {
	return be.Join(be.cfg.Bucket, be.cfg.Prefix)
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

	_, err := putObject(ctx, be.client, getNamespace(ctx, be.client), be.cfg.Bucket, objName, rd.Length(), io.NopCloser(rd), nil)
	// sanity check
	if err == nil {
		getResponse, err := be.client.GetObject(ctx, objectstorage.GetObjectRequest{
			NamespaceName: common.String(getNamespace(ctx, be.client)),
			BucketName:    common.String(be.cfg.Bucket),
			ObjectName:    common.String(objName),
		})
		if err != nil {
			return errors.Wrap(err, "client.fetch getResponse")
		}
		size := SafeDeref[int64](getResponse.ContentLength)
		if size != rd.Length() {
			return errors.Errorf("wrote %d bytes instead of the expected %d bytes", size, rd.Length())
		}
	}
	return errors.Wrap(err, "client.PutObject")
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
	//fmt.Println("Bytes range = ", bytesRange)
	if err != nil {
		fmt.Println("Error in range: ", err)
		return nil, err
	}

	var request objectstorage.GetObjectRequest

	if bytesRange == "" {
		request = objectstorage.GetObjectRequest{
			NamespaceName: common.String(getNamespace(ctx, be.client)),
			BucketName:    common.String(be.cfg.Bucket),
			ObjectName:    common.String(objName),
		}
	} else {
		request = objectstorage.GetObjectRequest{
			NamespaceName: common.String(getNamespace(ctx, be.client)),
			BucketName:    common.String(be.cfg.Bucket),
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

	// In this example, we're storing the download content in memory, please be aware of any issue with oom
	content, err := ioutil.ReadAll(resp.Content)
	if err != nil {
		fmt.Println("Error = ", err)
	}

	return io.NopCloser(bytes.NewReader(content)), nil
}

// Stat returns information about a blob.
func (be *Backend) Stat(ctx context.Context, h restic.Handle) (bi restic.FileInfo, err error) {
	objName := be.Filename(h)

	getResponse, err := be.client.GetObject(ctx, objectstorage.GetObjectRequest{
		NamespaceName: common.String(getNamespace(ctx, be.client)),
		BucketName:    common.String(be.cfg.Bucket),
		ObjectName:    common.String(objName),
	})
	if err != nil {
		return restic.FileInfo{}, errors.Wrap(err, "Stat")
	}
	return restic.FileInfo{Size: SafeDeref[int64](getResponse.ContentLength), Name: objName}, nil
}

// Remove removes the blob with the given name and type.
func (be *Backend) Remove(ctx context.Context, h restic.Handle) error {
	objName := be.Filename(h)

	err := deleteObject(ctx, be.client, getNamespace(ctx, be.client), be.cfg.Bucket, objName)

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

	listresp, err := be.client.ListObjects(ctx, objectstorage.ListObjectsRequest{
		NamespaceName: common.String(getNamespace(ctx, be.client)),
		BucketName:    common.String(be.cfg.Bucket),
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

		getResponse, err := be.client.GetObject(ctx, objectstorage.GetObjectRequest{
			NamespaceName: common.String(getNamespace(ctx, be.client)),
			BucketName:    common.String(be.cfg.Bucket),
			ObjectName:    common.String(SafeDeref[string](obj.Name)),
		})
		if err != nil {
			return err
		}

		fi := restic.FileInfo{
			Name: path.Base(name),
			Size: SafeDeref[int64](getResponse.ContentLength),
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

	_, err := be.client.CopyObject(ctx, objectstorage.CopyObjectRequest{
		NamespaceName: common.String(getNamespace(ctx, be.client)),
		BucketName:    common.String(be.cfg.Bucket),
		CopyObjectDetails: objectstorage.CopyObjectDetails{
			SourceObjectName:      common.String(oldname),
			DestinationObjectName: common.String(newname),
			DestinationBucket:     common.String(be.cfg.Bucket),
			DestinationRegion:     common.String(be.cfg.Region),
			DestinationNamespace:  common.String(getNamespace(ctx, be.client)),
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
	return deleteObject(ctx, be.client, getNamespace(ctx, be.client), be.cfg.Bucket, oldname)
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

// getNamespace fetches the tenancy namespace to be used by the OCI object store client
func getNamespace(ctx context.Context, client objectstorage.ObjectStorageClient) string {
	request := objectstorage.GetNamespaceRequest{}
	r, err := client.GetNamespace(ctx, request)
	if err != nil {
		log.Fatalln(err.Error())
	}
	return *r.Value
}

// putObject uploads an objet to OCI object store
func putObject(ctx context.Context, c objectstorage.ObjectStorageClient, namespace, bucketname, objectname string, contentLen int64, content io.ReadCloser, metadata map[string]string) (objectstorage.PutObjectResponse, error) {
	request := objectstorage.PutObjectRequest{
		NamespaceName: common.String(namespace),
		BucketName:    common.String(bucketname),
		ObjectName:    common.String(objectname),
		ContentLength: common.Int64(contentLen),
		PutObjectBody: content,
		OpcMeta:       metadata,
	}
	return c.PutObject(ctx, request)
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

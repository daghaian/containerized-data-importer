package importer

import (
	"io"
	"net/url"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/pkg/errors"

	"k8s.io/klog/v2"

	"kubevirt.io/containerized-data-importer/pkg/common"
	"kubevirt.io/containerized-data-importer/pkg/util"
)

const (
	s3FolderSep                   = "/"
	httpScheme                    = "http"
	s3AccelerateEndpoint          = ".s3-accelerate.amazonaws.com"
	s3AccelerateDualstackEndpoint = ".s3-accelerate.dualstack.amazonaws.com"
	// s3DefaultRegion is used for transfer acceleration endpoints which are global
	s3DefaultRegion = "us-east-1"
)

// S3Client is the interface to the used S3 client.
type S3Client interface {
	GetObject(input *s3.GetObjectInput) (*s3.GetObjectOutput, error)
}

// may be overridden in tests
var newClientFunc = getS3Client
var getBucketRegionFunc = getBucketRegion

// S3DataSource is the struct containing the information needed to import from an S3 data source.
// Sequence of phases:
// 1. Info -> Transfer
// 2. Transfer -> Convert
type S3DataSource struct {
	// S3 end point
	ep *url.URL
	// User name
	accessKey string
	// Password
	secKey string
	// Reader
	s3Reader io.ReadCloser
	// stack of readers
	readers *FormatReaders
	// The image file in scratch space.
	url *url.URL
}

// NewS3DataSource creates a new instance of the S3DataSource
func NewS3DataSource(endpoint, accessKey, secKey string, certDir string) (*S3DataSource, error) {
	ep, err := ParseEndpoint(endpoint)
	if err != nil {
		return nil, errors.Wrapf(err, "unable to parse endpoint %q", endpoint)
	}
	s3Reader, err := createS3Reader(ep, accessKey, secKey, certDir)
	if err != nil {
		return nil, err
	}
	return &S3DataSource{
		ep:        ep,
		accessKey: accessKey,
		secKey:    secKey,
		s3Reader:  s3Reader,
	}, nil
}

// Info is called to get initial information about the data.
func (sd *S3DataSource) Info() (ProcessingPhase, error) {
	var err error
	sd.readers, err = NewFormatReaders(sd.s3Reader, uint64(0))
	if err != nil {
		klog.Errorf("Error creating readers: %v", err)
		return ProcessingPhaseError, err
	}
	if !sd.readers.Convert {
		// Downloading a raw file, we can write that directly to the target.
		return ProcessingPhaseTransferDataFile, nil
	}

	return ProcessingPhaseTransferScratch, nil
}

// Transfer is called to transfer the data from the source to a temporary location.
func (sd *S3DataSource) Transfer(path string) (ProcessingPhase, error) {
	file := filepath.Join(path, tempFile)
	if err := CleanAll(file); err != nil {
		return ProcessingPhaseError, err
	}

	size, _ := util.GetAvailableSpace(path)
	if size <= int64(0) {
		//Path provided is invalid.
		return ProcessingPhaseError, ErrInvalidPath
	}

	err := streamDataToFile(sd.readers.TopReader(), file)
	if err != nil {
		return ProcessingPhaseError, err
	}
	// If streaming succeeded, then parsing the file into URL will also succeed, no need to check error status
	sd.url, _ = url.Parse(file)
	return ProcessingPhaseConvert, nil
}

// TransferFile is called to transfer the data from the source to the passed in file.
func (sd *S3DataSource) TransferFile(fileName string) (ProcessingPhase, error) {
	if err := CleanAll(fileName); err != nil {
		return ProcessingPhaseError, err
	}

	err := streamDataToFile(sd.readers.TopReader(), fileName)
	if err != nil {
		return ProcessingPhaseError, err
	}
	return ProcessingPhaseResize, nil
}

// GetURL returns the url that the data processor can use when converting the data.
func (sd *S3DataSource) GetURL() *url.URL {
	return sd.url
}

// GetTerminationMessage returns data to be serialized and used as the termination message of the importer.
func (sd *S3DataSource) GetTerminationMessage() *common.TerminationMessage {
	return nil
}

// Close closes any readers or other open resources.
func (sd *S3DataSource) Close() error {
	var err error
	if sd.readers != nil {
		err = sd.readers.Close()
	}
	return err
}

func createS3Reader(ep *url.URL, accessKey, secKey string, certDir string) (io.ReadCloser, error) {
	klog.V(3).Infoln("Using S3 client to get data")

	endpoint := ep.Host
	urlScheme := ep.Scheme
	klog.Infof("Endpoint %s", endpoint)

	var bucket, object string
	var useAcceleration bool

	// Check if this is a transfer acceleration endpoint
	if isTransferAccelerationEndpoint(endpoint) {
		// Virtual-hosted-style: bucket is in hostname, object is the full path
		bucket = extractBucketFromHost(endpoint)
		// For virtual-hosted style, the entire path is the object key
		object = strings.TrimPrefix(ep.Path, "/")
		useAcceleration = true
		klog.V(1).Infof("Using transfer acceleration endpoint")
	} else {
		// Path-style: bucket and object are both in path
		path := strings.Trim(ep.Path, "/")
		bucket, object = extractBucketAndObject(path)
		useAcceleration = false
	}

	klog.V(1).Infof("bucket %s", bucket)
	klog.V(1).Infof("object %s", object)
	svc, err := newClientFunc(endpoint, accessKey, secKey, certDir, urlScheme, useAcceleration)
	if err != nil {
		return nil, errors.Wrapf(err, "could not build s3 client for %q", ep.Host)
	}

	objInput := &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(object),
	}
	objOutput, err := svc.GetObject(objInput)
	if err != nil {
		return nil, errors.Wrapf(err, "could not get s3 object: \"%s/%s\"", bucket, object)
	}
	objectReader := objOutput.Body
	return objectReader, nil
}

func getS3Client(endpoint, accessKey, secKey string, certDir string, urlScheme string, useAcceleration bool) (S3Client, error) {
	// Adding certs using CustomCABundle will overwrite the SystemCerts, so we opt by creating a custom HTTPClient
	httpClient, err := createHTTPClient(certDir)

	if err != nil {
		return nil, errors.Wrap(err, "Error creating http client for s3")
	}

	creds := credentials.NewStaticCredentials(accessKey, secKey, "")
	disableSSL := false
	// Disable SSL for http endpoint. This should cause the s3 client to create http requests.
	if urlScheme == httpScheme {
		disableSSL = true
	}

	var awsEndpoint *string
	var s3UseAccelerate *bool
	var s3ForcePathStyle *bool
	var region string

	if useAcceleration {
		// For transfer acceleration, don't set endpoint and use virtual-hosted style
		// Dynamically detect the bucket region from the bucket name in the endpoint
		awsEndpoint = nil
		s3UseAccelerate = aws.Bool(true)
		s3ForcePathStyle = aws.Bool(false)

		// Extract bucket name from the transfer acceleration endpoint
		bucketName := extractBucketFromHost(endpoint)
		if bucketName == "" {
			return nil, errors.New("Failed to extract bucket name from transfer acceleration endpoint")
		}

		// Dynamically detect the bucket region
		detectedRegion, err := getBucketRegionFunc(bucketName, accessKey, secKey, certDir, urlScheme)
		if err != nil {
			return nil, errors.Wrapf(err, "Failed to detect region for bucket %q", bucketName)
		}
		region = detectedRegion
		klog.V(1).Infof("Configuring S3 client with transfer acceleration, using dynamically detected region %s", region)
	} else {
		// For path-style or custom endpoints, extract region from endpoint
		awsEndpoint = aws.String(endpoint)
		s3UseAccelerate = aws.Bool(false)
		s3ForcePathStyle = aws.Bool(true)
		region = extractRegion(endpoint)
	}

	sess, err := session.NewSession(&aws.Config{
		Region:           aws.String(region),
		Endpoint:         awsEndpoint,
		Credentials:      creds,
		S3UseAccelerate:  s3UseAccelerate,
		S3ForcePathStyle: s3ForcePathStyle,
		HTTPClient:       httpClient,
		DisableSSL:       &disableSSL,
	},
	)
	if err != nil {
		return nil, err
	}

	svc := s3.New(sess)
	return svc, nil
}

func extractRegion(s string) string {
	var region string

	// Check if this is a transfer acceleration endpoint
	if isTransferAccelerationEndpoint(s) {
		// Transfer acceleration endpoints are global and don't contain region info
		// Use the default region for transfer acceleration
		return s3DefaultRegion
	}

	r, _ := regexp.Compile(`s3\.(.+)\.amazonaws\.com`)
	if matches := r.FindStringSubmatch(s); matches != nil {
		region = matches[1]
	} else {
		region = strings.Split(s, ".")[0]
	}

	return region
}

func extractBucketAndObject(s string) (string, string) {
	pathSplit := strings.Split(s, s3FolderSep)
	bucket := pathSplit[0]
	object := strings.Join(pathSplit[1:], s3FolderSep)
	return bucket, object
}

// extractBucketFromHost extracts the bucket name from the hostname for virtual-hosted-style URLs
func extractBucketFromHost(host string) string {
	// Extract bucket from hostname like "bucket-name.s3-accelerate.amazonaws.com"
	parts := strings.Split(host, ".")
	if len(parts) > 0 {
		return parts[0]
	}
	return ""
}

// isTransferAccelerationEndpoint checks if the endpoint uses S3 Transfer Acceleration
func isTransferAccelerationEndpoint(endpoint string) bool {
	return strings.Contains(endpoint, s3AccelerateEndpoint) ||
		strings.Contains(endpoint, s3AccelerateDualstackEndpoint)
}

// getBucketRegion dynamically detects the bucket region using HeadBucket API
func getBucketRegion(bucketName string, accessKey, secKey, certDir, urlScheme string) (string, error) {
	klog.V(1).Infof("Detecting region for bucket: %s", bucketName)

	// Create an HTTP client with custom certs if needed
	httpClient, err := createHTTPClient(certDir)
	if err != nil {
		return "", errors.Wrap(err, "Error creating http client for region detection")
	}

	creds := credentials.NewStaticCredentials(accessKey, secKey, "")
	disableSSL := urlScheme == httpScheme

	// Create a temporary session with the default region to make the HeadBucket call
	// HeadBucket works from any region and returns the bucket region in the response headers
	sess, err := session.NewSession(&aws.Config{
		Region:      aws.String(s3DefaultRegion),
		Credentials: creds,
		HTTPClient:  httpClient,
		DisableSSL:  &disableSSL,
	})
	if err != nil {
		return "", errors.Wrap(err, "Failed to create session for region detection")
	}

	svc := s3.New(sess)

	// Create the HeadBucket request
	req, _ := svc.HeadBucketRequest(&s3.HeadBucketInput{
		Bucket: aws.String(bucketName),
	})

	// Send the request
	err = req.Send()

	// Extract region from the X-Amz-Bucket-Region response header
	// This header is present in both success (200) and redirect (301) responses
	region := s3DefaultRegion
	if req.HTTPResponse != nil {
		if bucketRegion := req.HTTPResponse.Header.Get("X-Amz-Bucket-Region"); bucketRegion != "" {
			region = bucketRegion
			klog.V(1).Infof("Detected region for bucket %s: %s", bucketName, region)
			return region, nil
		}
	}

	// If we got an error and couldn't extract the region from headers, return the error
	if err != nil {
		return "", errors.Wrapf(err, "Failed to head bucket for bucket: %s", bucketName)
	}

	// If no error but no region header, use default
	klog.V(1).Infof("No region header found for bucket %s, using default region %s", bucketName, region)
	return region, nil
}

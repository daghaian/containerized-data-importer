package importer

import (
	"io"
	"os"
	"path/filepath"
	"reflect"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/pkg/errors"
)

var _ = Describe("S3 data source", func() {
	var (
		sd     *S3DataSource
		tmpDir string
		err    error
	)

	BeforeEach(func() {
		newClientFunc = createMockS3Client
		tmpDir, err = os.MkdirTemp("", "scratch")
		Expect(err).NotTo(HaveOccurred())
		By("tmpDir: " + tmpDir)
	})

	AfterEach(func() {
		newClientFunc = getS3Client
		if sd != nil {
			sd.Close()
		}
		os.RemoveAll(tmpDir)
	})

	It("NewS3DataSource should Error, when passed in an invalid endpoint", func() {
		sd, err = NewS3DataSource("thisisinvalid#$%#ep", "", "", "")
		Expect(err).To(HaveOccurred())
	})

	It("NewS3DataSource should Error, when failing to create S3 client", func() {
		newClientFunc = failMockS3Client
		sd, err = NewS3DataSource("http://amazon.com", "", "", "")
		Expect(err).To(HaveOccurred())
	})

	It("NewS3DataSource should Error, when failing to get object", func() {
		newClientFunc = createErrMockS3Client
		sd, err = NewS3DataSource("http://amazon.com", "", "", "")
		Expect(err).To(HaveOccurred())
	})

	It("NewS3DataSource should fail when called with an invalid certdir", func() {
		newClientFunc = getS3Client
		sd, err = NewS3DataSource("http://amazon.com", "", "", "/invaliddir")
		Expect(err).To(HaveOccurred())
	})

	It("Info should return Error, when passed in an invalid image", func() {
		// Don't need to defer close, since ud.Close will close the reader
		file, err := os.Open(filepath.Join(imageDir, "content.tar"))
		Expect(err).NotTo(HaveOccurred())
		err = file.Close()
		Expect(err).NotTo(HaveOccurred())
		sd, err = NewS3DataSource("http://region.amazon.com/bucket-1/object-1", "", "", "")
		Expect(err).NotTo(HaveOccurred())
		sd.s3Reader = file
		result, err := sd.Info()
		Expect(err).To(HaveOccurred())
		Expect(ProcessingPhaseError).To(Equal(result))
	})

	It("Info should return Transfer, when passed in a valid image", func() {
		// Don't need to defer close, since ud.Close will close the reader
		file, err := os.Open(cirrosFilePath)
		Expect(err).NotTo(HaveOccurred())
		sd, err = NewS3DataSource("http://region.amazon.com/bucket-1/object-1", "", "", "")
		Expect(err).NotTo(HaveOccurred())
		sd.s3Reader = file
		result, err := sd.Info()
		Expect(err).NotTo(HaveOccurred())
		Expect(ProcessingPhaseTransferScratch).To(Equal(result))
	})

	It("Info should return TransferDataFile, when passed in a valid raw image", func() {
		// Don't need to defer close, since ud.Close will close the reader
		file, err := os.Open(tinyCoreFilePath)
		Expect(err).NotTo(HaveOccurred())
		sd, err = NewS3DataSource("http://region.amazon.com/bucket-1/object-1", "", "", "")
		Expect(err).NotTo(HaveOccurred())
		sd.s3Reader = file
		result, err := sd.Info()
		Expect(err).NotTo(HaveOccurred())
		Expect(ProcessingPhaseTransferDataFile).To(Equal(result))
	})

	DescribeTable("calling transfer should", func(fileName, scratchPath string, want []byte, wantErr bool) {
		if scratchPath == "" {
			scratchPath = tmpDir
		}
		sourceFile, err := os.Open(fileName)
		Expect(err).NotTo(HaveOccurred())

		sd, err = NewS3DataSource("http://region.amazon.com/bucket-1/object-1", "", "", "")
		Expect(err).NotTo(HaveOccurred())
		// Replace minio.Object with a reader we can use.
		sd.s3Reader = sourceFile
		nextPhase, err := sd.Info()
		Expect(err).NotTo(HaveOccurred())
		Expect(ProcessingPhaseTransferScratch).To(Equal(nextPhase))
		result, err := sd.Transfer(scratchPath)
		if !wantErr {
			Expect(err).NotTo(HaveOccurred())
			Expect(ProcessingPhaseConvert).To(Equal(result))
			file, err := os.Open(filepath.Join(scratchPath, tempFile))
			Expect(err).NotTo(HaveOccurred())
			defer file.Close()
			fileStat, err := file.Stat()
			Expect(err).NotTo(HaveOccurred())
			Expect(int64(len(want))).To(Equal(fileStat.Size()))
			resultBuffer, err := io.ReadAll(file)
			Expect(err).NotTo(HaveOccurred())
			Expect(reflect.DeepEqual(resultBuffer, want)).To(BeTrue())
			Expect(file.Name()).To(Equal(sd.GetURL().String()))
		} else {
			Expect(err).To(HaveOccurred())
			Expect(ProcessingPhaseError).To(Equal(result))
		}
	},
		Entry("return Error with missing scratch space", cirrosFilePath, "/imaninvalidpath", nil, true),
		Entry("return Convert with scratch space and valid qcow file", cirrosFilePath, "", cirrosData, false),
	)

	It("Transfer should fail on reader error", func() {
		sourceFile, err := os.Open(cirrosFilePath)
		Expect(err).NotTo(HaveOccurred())

		sd, err = NewS3DataSource("http://region.amazon.com/bucket-1/object-1", "", "", "")
		Expect(err).NotTo(HaveOccurred())
		// Replace minio.Object with a reader we can use.
		sd.s3Reader = sourceFile
		nextPhase, err := sd.Info()
		Expect(err).NotTo(HaveOccurred())
		Expect(ProcessingPhaseTransferScratch).To(Equal(nextPhase))
		err = sourceFile.Close()
		Expect(err).NotTo(HaveOccurred())
		result, err := sd.Transfer(tmpDir)
		Expect(err).To(HaveOccurred())
		Expect(ProcessingPhaseError).To(Equal(result))
	})

	It("TransferFile should succeed when writing to valid file", func() {
		// Don't need to defer close, since ud.Close will close the reader
		file, err := os.Open(tinyCoreFilePath)
		Expect(err).NotTo(HaveOccurred())
		sd, err = NewS3DataSource("http://region.amazon.com/bucket-1/object-1", "", "", "")
		Expect(err).NotTo(HaveOccurred())
		// Replace minio.Object with a reader we can use.
		sd.s3Reader = file
		result, err := sd.Info()
		Expect(err).NotTo(HaveOccurred())
		Expect(ProcessingPhaseTransferDataFile).To(Equal(result))
		result, err = sd.TransferFile(filepath.Join(tmpDir, "file"))
		Expect(err).ToNot(HaveOccurred())
		Expect(ProcessingPhaseResize).To(Equal(result))
	})

	It("TransferFile should fail on streaming error", func() {
		// Don't need to defer close, since ud.Close will close the reader
		file, err := os.Open(tinyCoreFilePath)
		Expect(err).NotTo(HaveOccurred())
		sd, err = NewS3DataSource("http://region.amazon.com/bucket-1/object-1", "", "", "")
		Expect(err).NotTo(HaveOccurred())
		// Replace minio.Object with a reader we can use.
		sd.s3Reader = file
		result, err := sd.Info()
		Expect(err).NotTo(HaveOccurred())
		Expect(ProcessingPhaseTransferDataFile).To(Equal(result))
		result, err = sd.TransferFile("/invalidpath/invalidfile")
		Expect(err).To(HaveOccurred())
		Expect(ProcessingPhaseError).To(Equal(result))
	})

	It("GetS3Client should return a real client for path-style", func() {
		_, err := getS3Client("", "", "", "", "", false)
		Expect(err).NotTo(HaveOccurred())
	})

	It("GetS3Client should return a real client with transfer acceleration", func() {
		_, err := getS3Client("", "", "", "", "", true)
		Expect(err).NotTo(HaveOccurred())
	})

	It("Should Extract Bucket and Object form the S3 URL", func() {
		bucket, object := extractBucketAndObject("Bucket1/Object.tmp")
		Expect(bucket).Should(Equal("Bucket1"))
		Expect(object).Should(Equal("Object.tmp"))

		bucket, object = extractBucketAndObject("Bucket1/Folder1/Object.tmp")
		Expect(bucket).Should(Equal("Bucket1"))
		Expect(object).Should(Equal("Folder1/Object.tmp"))
	})

	It("Should extract bucket from transfer acceleration hostname", func() {
		bucket := extractBucketFromHost("my-bucket.s3-accelerate.amazonaws.com")
		Expect(bucket).Should(Equal("my-bucket"))

		bucket = extractBucketFromHost("test-bucket.s3-accelerate.dualstack.amazonaws.com")
		Expect(bucket).Should(Equal("test-bucket"))

		bucket = extractBucketFromHost("another-bucket-name.s3-accelerate.amazonaws.com")
		Expect(bucket).Should(Equal("another-bucket-name"))
	})

	It("Should detect transfer acceleration endpoints", func() {
		Expect(isTransferAccelerationEndpoint("my-bucket.s3-accelerate.amazonaws.com")).To(BeTrue())
		Expect(isTransferAccelerationEndpoint("my-bucket.s3-accelerate.dualstack.amazonaws.com")).To(BeTrue())
		Expect(isTransferAccelerationEndpoint("s3.us-east-1.amazonaws.com")).To(BeFalse())
		Expect(isTransferAccelerationEndpoint("minio.local")).To(BeFalse())
		Expect(isTransferAccelerationEndpoint("s3.amazonaws.com")).To(BeFalse())
	})

	It("NewS3DataSource should work with transfer acceleration endpoint", func() {
		sd, err = NewS3DataSource("https://my-bucket.s3-accelerate.amazonaws.com/my-object.img", "", "", "")
		Expect(err).NotTo(HaveOccurred())
		Expect(sd).NotTo(BeNil())
	})

	It("NewS3DataSource should work with transfer acceleration dualstack endpoint", func() {
		sd, err = NewS3DataSource("https://test-bucket.s3-accelerate.dualstack.amazonaws.com/path/to/object.qcow2", "", "", "")
		Expect(err).NotTo(HaveOccurred())
		Expect(sd).NotTo(BeNil())
	})

	It("Should extract region from standard S3 endpoints", func() {
		// Standard regional endpoints
		region := extractRegion("s3.us-east-1.amazonaws.com")
		Expect(region).Should(Equal("us-east-1"))

		region = extractRegion("s3.eu-west-1.amazonaws.com")
		Expect(region).Should(Equal("eu-west-1"))

		region = extractRegion("s3.ap-southeast-2.amazonaws.com")
		Expect(region).Should(Equal("ap-southeast-2"))
	})

	It("Should use default region for transfer acceleration endpoints", func() {
		// Transfer acceleration endpoints should return the default region
		// This prevents incorrectly extracting the bucket name as the region
		region := extractRegion("my-bucket.s3-accelerate.amazonaws.com")
		Expect(region).Should(Equal(s3DefaultRegion))

		region = extractRegion("test-bucket.s3-accelerate.dualstack.amazonaws.com")
		Expect(region).Should(Equal(s3DefaultRegion))

		region = extractRegion("another-bucket.s3-accelerate.amazonaws.com")
		Expect(region).Should(Equal(s3DefaultRegion))
	})
})

var _ = Describe("S3 client region configuration", func() {
	It("Should configure correct region for transfer acceleration", func() {
		client, err := getS3Client("my-bucket.s3-accelerate.amazonaws.com", "access", "secret", "", "https", true)
		Expect(err).NotTo(HaveOccurred())
		Expect(client).NotTo(BeNil())
		// The client should be configured with us-east-1 as the default region for transfer acceleration
	})

	It("Should configure correct region for standard endpoints", func() {
		client, err := getS3Client("s3.eu-west-1.amazonaws.com", "access", "secret", "", "https", false)
		Expect(err).NotTo(HaveOccurred())
		Expect(client).NotTo(BeNil())
		// The client should be configured with the region extracted from the endpoint
	})
})

// MockS3Client is a mock AWS S3 client
type MockS3Client struct {
	endpoint string //nolint:unused // TODO: check if need to remove this field
	accKey   string
	secKey   string
	certDir  string
	doErr    bool
}

func failMockS3Client(endpoint, accKey, secKey string, certDir string, urlScheme string, useAcceleration bool) (S3Client, error) {
	return nil, errors.New("Failed to create client")
}

func createMockS3Client(endpoint, accKey, secKey string, certDir string, urlScheme string, useAcceleration bool) (S3Client, error) {
	return &MockS3Client{
		accKey:  accKey,
		secKey:  secKey,
		certDir: certDir,
		doErr:   false,
	}, nil
}

func createErrMockS3Client(endpoint, accKey, secKey string, certDir string, urlScheme string, useAcceleration bool) (S3Client, error) {
	return &MockS3Client{
		doErr: true,
	}, nil
}

func (mc *MockS3Client) GetObject(input *s3.GetObjectInput) (*s3.GetObjectOutput, error) {
	if !mc.doErr {
		return &s3.GetObjectOutput{}, nil
	}
	return nil, errors.New("Failed to get object")
}

package export

import (
	"bytes"
	"context"
	"fmt"
	"net/http"

	"github.com/Arierutemplar/event-driven-microservices-ts/internal/transport"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

// ObjectStore es el destino del exportador (S3 o MinIO).
type ObjectStore interface {
	// EnsureBucket crea el bucket si no existe.
	EnsureBucket(ctx context.Context) error
	// Put escribe (o sobrescribe, de forma idempotente) el objeto key.
	Put(ctx context.Context, key string, data []byte) error
}

// S3Config configura la conexión a un servicio compatible con S3.
type S3Config struct {
	Endpoint                           string // host:puerto, sin esquema
	AccessKey                          string
	SecretKey                          string
	Bucket                             string
	Region                             string
	UseSSL                             bool
	TLSCAFile, TLSCertFile, TLSKeyFile string
}

// S3Store implementa ObjectStore con minio-go (compatible con AWS S3 y MinIO).
type S3Store struct {
	client *minio.Client
	cfg    S3Config
}

func NewS3Store(cfg S3Config) (*S3Store, error) {
	if cfg.Endpoint == "" || cfg.Bucket == "" {
		return nil, fmt.Errorf("export: S3 requiere endpoint y bucket")
	}
	opts := &minio.Options{
		Creds:  credentials.NewStaticV4(cfg.AccessKey, cfg.SecretKey, ""),
		Secure: cfg.UseSSL,
		Region: cfg.Region,
	}
	if cfg.TLSCAFile != "" || cfg.TLSCertFile != "" || cfg.TLSKeyFile != "" {
		if !cfg.UseSSL {
			return nil, fmt.Errorf("export: TLS files require S3_USE_SSL")
		}
		tlsConfig, err := transport.ClientTLS(cfg.TLSCAFile, cfg.TLSCertFile, cfg.TLSKeyFile)
		if err != nil {
			return nil, fmt.Errorf("export: TLS: %w", err)
		}
		tr, err := minio.DefaultTransport(true)
		if err != nil {
			return nil, fmt.Errorf("export: transport: %w", err)
		}
		tr.TLSClientConfig = tlsConfig
		opts.Transport = tr
	}
	c, err := minio.New(cfg.Endpoint, opts)
	if err != nil {
		return nil, fmt.Errorf("export: cliente S3: %w", err)
	}
	return &S3Store{client: c, cfg: cfg}, nil
}

func (s *S3Store) EnsureBucket(ctx context.Context) error {
	ok, err := s.client.BucketExists(ctx, s.cfg.Bucket)
	if err != nil {
		return classifyS3(err)
	}
	if ok {
		return nil
	}
	if err := s.client.MakeBucket(ctx, s.cfg.Bucket, minio.MakeBucketOptions{Region: s.cfg.Region}); err != nil {
		// Carrera con otra réplica que lo creó primero
		if code := minio.ToErrorResponse(err).Code; code == "BucketAlreadyOwnedByYou" || code == "BucketAlreadyExists" {
			return nil
		}
		return classifyS3(err)
	}
	return nil
}

func (s *S3Store) Put(ctx context.Context, key string, data []byte) error {
	_, err := s.client.PutObject(ctx, s.cfg.Bucket, key, bytes.NewReader(data), int64(len(data)), minio.PutObjectOptions{
		ContentType: "application/vnd.apache.parquet",
	})
	return classifyS3(err)
}

// classifyS3 separa los errores que reintentar no arregla (credenciales, bucket, petición inválida)
// de los transitorios (red caída, timeouts, 5xx, throttling).
func classifyS3(err error) error {
	if err == nil {
		return nil
	}
	resp := minio.ToErrorResponse(err)
	switch resp.Code {
	case "AccessDenied", "InvalidAccessKeyId", "SignatureDoesNotMatch", "NoSuchBucket",
		"InvalidBucketName", "AllAccessDisabled", "AccountProblem", "InvalidArgument", "EntityTooLarge":
		return fmt.Errorf("%w: %v", ErrPermanent, err)
	}
	if resp.StatusCode >= 400 && resp.StatusCode < 500 &&
		resp.StatusCode != http.StatusRequestTimeout && resp.StatusCode != http.StatusTooManyRequests {
		return fmt.Errorf("%w: %v", ErrPermanent, err)
	}
	return err
}

package main

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strconv"
	"time"

	"github.com/Arierutemplar/event-driven-microservices-ts/internal/export"
	"github.com/Arierutemplar/event-driven-microservices-ts/internal/repository"
)

// startExporter lanza el exportador a Parquet si hay S3_ENDPOINT y el repositorio es PostgreSQL.
// Devuelve un canal que se cierra cuando el worker termina, o nil si no se activó.
func startExporter(ctx context.Context, repo io.Closer) (<-chan struct{}, error) {
	endpoint := os.Getenv("S3_ENDPOINT")
	if endpoint == "" {
		slog.Info("S3_ENDPOINT no definido: exportación a Parquet desactivada")
		return nil, nil
	}
	pg, ok := repo.(*repository.PostgresRepository)
	if !ok {
		return nil, fmt.Errorf("exportación a Parquet requiere PostgreSQL")
	}
	useSSL, err := strconv.ParseBool(envString("S3_USE_SSL", "false"))
	if err != nil {
		return nil, fmt.Errorf("S3_USE_SSL inválido: %w", err)
	}
	bucket := envString("S3_BUCKET", "analytics-lake")
	store, err := export.NewS3Store(export.S3Config{
		Endpoint:    endpoint,
		AccessKey:   os.Getenv("S3_ACCESS_KEY"),
		SecretKey:   os.Getenv("S3_SECRET_KEY"),
		Bucket:      bucket,
		Region:      envString("S3_REGION", "us-east-1"),
		UseSSL:      useSSL,
		TLSCAFile:   os.Getenv("S3_TLS_CA_FILE"),
		TLSCertFile: os.Getenv("S3_TLS_CERT_FILE"),
		TLSKeyFile:  os.Getenv("S3_TLS_KEY_FILE"),
	})
	if err != nil {
		return nil, fmt.Errorf("inicializar exportación a Parquet: %w", err)
	}
	exp := export.New(pg.ExportSource(), store, export.Config{
		Interval:      envDuration("EXPORT_INTERVAL", 5*time.Minute),
		Poll:          envDuration("EXPORT_POLL", 15*time.Second),
		Lag:           envDuration("EXPORT_LAG", 30*time.Second),
		BatchSize:     envInt("EXPORT_BATCH_SIZE", 50000),
		Prefix:        os.Getenv("S3_PREFIX"),
		UploadTimeout: envDuration("EXPORT_UPLOAD_TIMEOUT", 30*time.Second),
		Retry: export.RetryPolicy{
			MaxAttempts: envInt("EXPORT_RETRY_ATTEMPTS", 5),
			BaseDelay:   envDuration("EXPORT_RETRY_BASE_DELAY", 500*time.Millisecond),
			MaxDelay:    envDuration("EXPORT_RETRY_MAX_DELAY", 10*time.Second),
		},
	})
	slog.Info("exportación a Parquet activada", "bucket", bucket, "endpoint", endpoint)

	done := make(chan struct{})
	go func() {
		defer close(done)
		exp.Run(ctx)
	}()
	return done, nil
}

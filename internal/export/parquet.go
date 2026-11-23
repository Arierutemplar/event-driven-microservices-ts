package export

import (
	"bytes"
	"fmt"
	"time"

	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/compress/snappy"

	"github.com/Arierutemplar/event-driven-microservices-ts/internal/domain"
)

// Row es el esquema Parquet de un evento. year/month/day/tenant viajan en la ruta del objeto
// (particionado estilo Hive), y tenant_id también como columna para consultas sin leer la ruta.
type Row struct {
	ID        int64     `parquet:"id"`
	EventID   string    `parquet:"event_id"`
	TenantID  string    `parquet:"tenant_id,dict"`
	EventType string    `parquet:"event_type,dict"`
	UserID    string    `parquet:"user_id"`
	Payload   string    `parquet:"payload,optional"` // JSON como texto; NULL si no había
	MsgID     string    `parquet:"msg_id,optional"`
	CreatedAt time.Time `parquet:"created_at,timestamp(microsecond)"`
}

// Encode serializa los registros a un archivo Parquet comprimido con Snappy.
func Encode(recs []domain.ExportRecord) ([]byte, error) {
	var buf bytes.Buffer
	w := parquet.NewGenericWriter[Row](&buf, parquet.Compression(&snappy.Codec{}))
	rows := make([]Row, len(recs))
	for i, r := range recs {
		rows[i] = Row{
			ID: r.ID, EventID: r.EventID, TenantID: r.TenantID, EventType: r.EventType, UserID: r.UserID,
			Payload: r.Payload, MsgID: r.MsgID, CreatedAt: r.CreatedAt.UTC(),
		}
	}
	if _, err := w.Write(rows); err != nil {
		return nil, fmt.Errorf("export: escribir parquet: %w", err)
	}
	if err := w.Close(); err != nil {
		return nil, fmt.Errorf("export: cerrar parquet: %w", err)
	}
	return buf.Bytes(), nil
}

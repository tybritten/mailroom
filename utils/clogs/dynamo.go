package clogs

import (
	"bytes"
	"compress/gzip"
	"time"

	"github.com/nyaruka/gocommon/httpx"
	"github.com/nyaruka/gocommon/jsonx"
)

const (
	dynamoTTL = 14 * 24 * time.Hour
)

// DynamoChannelLog channel log to be written to DynamoDB
type DynamoChannelLog struct {
	UUID      LogUUID   `dynamodbav:"UUID"`
	Type      string    `dynamodbav:"Type"`
	DataGZ    []byte    `dynamodbav:"DataGZ,omitempty"`
	ElapsedMS int       `dynamodbav:"ElapsedMS"`
	CreatedOn time.Time `dynamodbav:"CreatedOn,unixtime"`
	ExpiresOn time.Time `dynamodbav:"ExpiresOn,unixtime"`
}

func NewDynamoChannelLog(uuid LogUUID, logType string, httpLogs []*httpx.Log, errors []*LogError, elapsed time.Duration, createdOn time.Time) *DynamoChannelLog {
	data := jsonx.MustMarshal(map[string]any{"http_logs": httpLogs, "errors": errors})
	buf := &bytes.Buffer{}
	w := gzip.NewWriter(buf)
	w.Write(data)
	w.Close()

	return &DynamoChannelLog{
		UUID:      uuid,
		Type:      logType,
		DataGZ:    buf.Bytes(),
		ElapsedMS: int(elapsed / time.Millisecond),
		CreatedOn: createdOn,
		ExpiresOn: createdOn.Add(dynamoTTL),
	}
}

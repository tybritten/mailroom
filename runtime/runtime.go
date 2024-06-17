package runtime

import (
	"database/sql"

	"github.com/elastic/go-elasticsearch/v8"
	"github.com/gomodule/redigo/redis"
	"github.com/jmoiron/sqlx"
	"github.com/nyaruka/gocommon/storage"
)

// Runtime represents the set of services required to run many Mailroom functions. Used as a wrapper for
// those services to simplify call signatures but not create a direct dependency to Mailroom or Server
type Runtime struct {
	DB                *sqlx.DB
	ReadonlyDB        *sql.DB
	RP                *redis.Pool
	ES                *elasticsearch.TypedClient
	AttachmentStorage storage.Storage
	SessionStorage    storage.Storage
	LogStorage        storage.Storage
	Config            *Config
}

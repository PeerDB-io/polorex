package main

import (
	"context"
	"fmt"
	"net/url"

	"github.com/jackc/pgx/v5"
)

type polorexConfig struct {
	host      string
	port      uint16
	user      string
	password  string
	database  string
	slotname  string
	pubname   string
	tablename string
}

func (p *polorexConfig) createPostgresConn(ctx context.Context, replication bool) (*pgx.Conn, error) {
	var replicationSuffix string
	// needed for creating and reading from the replication slot
	if replication {
		replicationSuffix = "&replication=database"
	}
	passwordEscaped := url.QueryEscape(p.password)
	// for a url like postgres://user:password@host:port/dbname
	connString := fmt.Sprintf(
		"postgres://%s:%s@%s:%d/%s?application_name=polorex%s",
		p.user,
		passwordEscaped,
		p.host,
		p.port,
		p.database,
		replicationSuffix,
	)

	return pgx.Connect(ctx, connString)
}

package main

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/jackc/pglogrepl"
)

func setupMain(ctx context.Context, config *polorexConfig) error {
	conn, err := config.createPostgresConn(ctx, true)
	if err != nil {
		return fmt.Errorf("unable to establish replication connection: %w", err)
	}

	slog.Info("Creating table", "tablename", config.tablename)
	// assume tablename is not schema-qualified, so explicitly add public
	_, err = conn.Exec(ctx, fmt.Sprintf(`CREATE TABLE IF NOT EXISTS public.%s(
		id BIGINT PRIMARY KEY GENERATED BY DEFAULT AS IDENTITY,
		txt TEXT DEFAULT md5(random()::text))`, config.tablename,
	))
	if err != nil {
		return fmt.Errorf("failed to create table: %w", err)
	}

	slog.Info("Creating publication", "pubname", config.pubname)
	_, err = conn.Exec(ctx, fmt.Sprintf(`CREATE PUBLICATION %s FOR TABLE %s`,
		config.pubname, config.tablename))
	if err != nil {
		return fmt.Errorf("failed to create publication for table: %w", err)
	}

	// drop this using teardown as soon as possible if the DB has actual traffic on it
	slog.Info("Creating replication slot", "slotname", config.slotname)
	opts := pglogrepl.CreateReplicationSlotOptions{
		Temporary: false,
		Mode:      pglogrepl.LogicalReplication,
	}
	_, err = pglogrepl.CreateReplicationSlot(ctx,
		conn.PgConn(), config.slotname, "pgoutput", opts)
	if err != nil {
		return fmt.Errorf("failed to create replication slot: %w", err)
	}

	return nil
}

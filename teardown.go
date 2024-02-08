package main

import (
	"context"
	"fmt"
	"log/slog"
)

func teardownMain(ctx context.Context, config *polorexConfig) error {
	conn, err := config.createPostgresConn(ctx, false)
	if err != nil {
		return fmt.Errorf("unable to establish replication connection: %w", err)
	}

	slog.Info("Dropping replication slot", "slotname", config.slotname)
	// we do it like this so it doesn't error out if slot doesn't exist
	_, err = conn.Exec(ctx,
		`SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name=$1`,
		config.slotname)
	if err != nil {
		return fmt.Errorf("failed to drop replication slot: %w", err)
	}

	slog.Info("Dropping publication", "pubname", config.pubname)
	_, err = conn.Exec(ctx, fmt.Sprintf(`DROP PUBLICATION IF EXISTS %s`,
		config.pubname))
	if err != nil {
		return fmt.Errorf("failed to create publication for table: %w", err)
	}

	slog.Info("Dropping table", "tablename", config.tablename)
	_, err = conn.Exec(ctx, fmt.Sprintf(`DROP TABLE IF EXISTS %s`, config.tablename))
	if err != nil {
		return fmt.Errorf("failed to create table: %w", err)
	}

	return nil
}

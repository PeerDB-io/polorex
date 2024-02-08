package main

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"golang.org/x/sync/errgroup"
)

type txngenConfig struct {
	parallelism int
	iterations  int
	batchSize   int
	delayMs     int
	noTxn       bool
}

// executes a single thread inserting rows, optionally in a txn
func txngenProcessor(ctx context.Context, threadID int,
	config *polorexConfig, txngenConfig *txngenConfig) error {
	conn, err := config.createPostgresConn(ctx, false)
	if err != nil {
		return fmt.Errorf("unable to establish connection: %w", err)
	}

	// slightly inefficient queries to report minID and maxID
	stmt := fmt.Sprintf("INSERT INTO %s VALUES%s", config.tablename,
		strings.TrimSuffix(strings.Repeat("(default,default),", txngenConfig.batchSize), ","))
	minStmt := fmt.Sprintf("WITH ir AS (%s RETURNING id) SELECT min(id) FROM ir", stmt)
	maxStmt := fmt.Sprintf("WITH ir AS (%s RETURNING id) SELECT max(id) FROM ir", stmt)
	var minID, maxID int64

	// don't want to duplicate code for txn vs noTxn, so scoping txn to this function
	insertFunc := func(i int) error {
		switch i {
		case 0:
			err = conn.QueryRow(ctx, minStmt).Scan(&minID)
		case txngenConfig.iterations - 1:
			err = conn.QueryRow(ctx, maxStmt).Scan(&maxID)
		default:
			_, err = conn.Exec(ctx, stmt)
		}
		if err != nil {
			return fmt.Errorf("failed to execute INSERT statement outside txn: %w", err)
		}
		return nil
	}
	if !txngenConfig.noTxn {
		tx, err := conn.Begin(ctx)
		if err != nil {
			return fmt.Errorf("failed to begin transaction for inserting records: %w", err)
		}
		defer func() {
			deferErr := tx.Rollback(ctx)
			if deferErr != pgx.ErrTxClosed && deferErr != nil {
				slog.Error("error rolling back transaction for inserting records", "err", err)
			}
		}()

		insertFunc = func(i int) error {
			switch i {
			case 0:
				err = conn.QueryRow(ctx, minStmt).Scan(&minID)
			case txngenConfig.iterations - 1:
				err = conn.QueryRow(ctx, maxStmt).Scan(&maxID)
			default:
				_, err = conn.Exec(ctx, stmt)
			}
			if err != nil {
				return fmt.Errorf("failed to execute INSERT statement in txn: %w", err)
			}

			if i == (txngenConfig.iterations - 1) {
				err = tx.Commit(ctx)
				if err != nil {
					return fmt.Errorf("failed to commit transaction for inserting records: %w", err)
				}
			}
			return nil
		}
	}

	totalRecords := txngenConfig.iterations * txngenConfig.batchSize
	startTime := time.Now()
	for i := 0; i < txngenConfig.iterations; i++ {
		slog.Info("Inserting records into table",
			"thread", threadID,
			"iterations", fmt.Sprintf("%d/%d", i, txngenConfig.iterations),
			"records", fmt.Sprintf("%d/%d", i*txngenConfig.batchSize, totalRecords),
		)
		err = insertFunc(i)
		if err != nil {
			return err
		}
		if (txngenConfig.delayMs > 0) && i < (txngenConfig.iterations-1) {
			time.Sleep(time.Duration(txngenConfig.delayMs) * time.Millisecond)
		}
	}

	slog.Info("Finished inserting records",
		"totalRecords", totalRecords,
		"minID", minID,
		"maxID", maxID,
		"totalTime", time.Since(startTime),
		"rps", totalRecords/int(time.Since(startTime).Seconds()),
	)
	return nil
}

func txngenMain(ctx context.Context, config *polorexConfig, txngenConfig *txngenConfig) error {
	// cancellation will come from the quit logic in main
	g, _ := errgroup.WithContext(ctx)
	g.SetLimit(txngenConfig.parallelism)

	for i := 0; i < txngenConfig.parallelism; i++ {
		ci := i
		g.Go(func() error {
			return txngenProcessor(ctx, ci, config, txngenConfig)
		})
	}

	return g.Wait()
}

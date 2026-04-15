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
	subxacts    bool
}

// executes a single thread inserting rows, optionally in a txn
func txngenProcessor(ctx context.Context, threadID int,
	config *polorexConfig, txngenConfig *txngenConfig) error {
	conn, err := config.createPostgresConn(ctx, false)
	if err != nil {
		return fmt.Errorf("unable to establish connection: %w", err)
	}
	defer conn.Close(ctx)

	// slightly inefficient queries to report minID and maxID
	stmt := fmt.Sprintf("INSERT INTO %s VALUES%s", config.tablename,
		strings.TrimSuffix(strings.Repeat("(default,default),", txngenConfig.batchSize), ","))
	stmtName := fmt.Sprintf("stmt_%d", threadID)
	minStmt := fmt.Sprintf("WITH ir AS (%s RETURNING id) SELECT min(id) FROM ir", stmt)
	maxStmt := fmt.Sprintf("WITH ir AS (%s RETURNING id) SELECT max(id) FROM ir", stmt)
	if _, err := conn.Prepare(ctx, stmtName, stmt); err != nil {
		return fmt.Errorf("failed to prepare INSERT statement: %w", err)
	}
	var minID, maxID int64

	// don't want to duplicate code for txn vs noTxn, so scoping txn to this function
	insertFunc := func(i int) error {
		switch i {
		case 0:
			err = conn.QueryRow(ctx, minStmt).Scan(&minID)
		case txngenConfig.iterations - 1:
			err = conn.QueryRow(ctx, maxStmt).Scan(&maxID)
		default:
			_, err = conn.Exec(ctx, stmtName)
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
				err = tx.QueryRow(ctx, minStmt).Scan(&minID)
			case txngenConfig.iterations - 1:
				if txngenConfig.subxacts {
					if _, err := tx.Exec(ctx, fmt.Sprintf("SAVEPOINT polorex_subxact_%d_%d", threadID, i)); err != nil {
						return fmt.Errorf("failed to begin subtransaction: %w", err)
					}
				}
				err = tx.QueryRow(ctx, maxStmt).Scan(&maxID)
			default:
				if txngenConfig.subxacts {
					if _, err := tx.Exec(ctx, fmt.Sprintf("SAVEPOINT polorex_subxact_%d_%d", threadID, i)); err != nil {
						return fmt.Errorf("failed to begin subtransaction: %w", err)
					}
				}
				_, err = tx.Exec(ctx, stmtName)
			}
			if err != nil {
				return fmt.Errorf("failed to execute INSERT statement in txn: %w", err)
			}

			if i == (txngenConfig.iterations - 1) {
				if config.twoPhase {
					if _, err := tx.Exec(ctx, fmt.Sprintf("PREPARE TRANSACTION 'polorex_prepared_txn_%d'", threadID)); err != nil {
						return fmt.Errorf("failed to prepare statement for inserting records: %w", err)
					}
					if _, err := conn.Exec(ctx, fmt.Sprintf("COMMIT PREPARED 'polorex_prepared_txn_%d'", threadID)); err != nil {
						return fmt.Errorf("failed to commit prepared transaction for inserting records: %w", err)
					}
					slog.Info("Committed prepared transaction",
						slog.Int("thread", threadID),
						slog.Int64("minID", minID),
						slog.Int64("maxID", maxID),
					)
				}
				if err = tx.Commit(ctx); err != nil {
					return fmt.Errorf("failed to commit transaction for inserting records: %w", err)
				}
				if !config.twoPhase {
					slog.Info("Committed transaction",
						slog.Int("thread", threadID),
						slog.Int64("minID", minID),
						slog.Int64("maxID", maxID),
					)
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
		"rps", float64(totalRecords)/float64(time.Since(startTime).Nanoseconds())*1e9,
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

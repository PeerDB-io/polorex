package main

import (
	"context"
	"fmt"
	"log"
	"log/slog"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/jackc/pgx/v5/pgtype"
)

type txnreaderConfig struct {
	protocolVersion  int
	csvlog           bool
	csvlogInterval   int
	csvlogFilePrefix string
}

type txnReader struct {
	config *polorexConfig
	// used to read slot size when sending standby messages
	slotSizeConn *pgx.Conn
	// used for START_REPLICATION, gets messages in a loop
	replConn        *pgconn.PgConn
	protocolVersion int
	// for sending standby messages
	clientXLogPos pglogrepl.LSN
	// global -> used only for transactions confirmed to be committed
	relation *pglogrepl.RelationMessage

	nextStandbyMessageDeadline time.Time
	typeMap                    *pgtype.Map
	// set between StreamStart and StreamStop, needs to be passed to pglogrepl message parsing logic
	// all InsertMessages that come when this is set are part of the same txn
	inStreamV2 bool
	// set between Begin and Commit, all InsertMessages that come when this is set are part of the same txn
	inTxnReadV1 bool
	// we could be reading multiple streamed transactions at the same time, key is xid
	// can also contain subxacts, mapped to their toplevel xid in subxactMap
	uncommittedTxnsV2 map[uint32]*txnInfo
	// we could be holding multiple prepared txns at the same time, both from v1 and v2 semantics
	preparedTxnsV3 map[uint32]*txnInfo
	// to hold the transaction we are fully reading from Begin to Commit
	inProgressTxnV1 *txnInfo
	// not nil if a txn is ready to be read
	processedTxn *txnInfo
	// xid of toplevel txn we're currently streaming
	currentXidV2 uint32

	// used for timing how much time each transaction takes to read
	txnStartTime time.Time
	// for logging to CSV file
	csvlogInterval               time.Duration
	txnreadCSVLogger             *txnreadCSVLogger
	currentSecond                time.Time
	recordsReadInCurrentDuration int
}

func newTxnReader(ctx context.Context, config *polorexConfig, txnreaderConfig *txnreaderConfig) (*txnReader, error) {
	replConn, err := config.createPostgresConn(ctx, true)
	if err != nil {
		return nil, fmt.Errorf("unable to establish replication connection: %w", err)
	}
	slotSizeConn, err := config.createPostgresConn(ctx, false)
	if err != nil {
		return nil, fmt.Errorf("unable to establish connection to query slot size: %w", err)
	}

	pluginArgs := []string{
		fmt.Sprintf("proto_version '%d'", txnreaderConfig.protocolVersion),
		fmt.Sprintf("publication_names '%s'", config.pubname),
		"messages 'true'",
	}
	if txnreaderConfig.protocolVersion >= 2 {
		pluginArgs = append(pluginArgs, "streaming 'true'")
	}

	// sysident, err := pglogrepl.IdentifySystem(ctx, replConn.PgConn())
	// if err != nil {
	// 	return nil, fmt.Errorf("IdentifySystem failed: %w", err)
	// }

	err = pglogrepl.StartReplication(ctx, replConn.PgConn(), config.slotname, 0,
		pglogrepl.StartReplicationOptions{
			PluginArgs: pluginArgs,
			Mode:       pglogrepl.LogicalReplication,
		})
	if err != nil {
		return nil, fmt.Errorf("unable to start logical replication: %w", err)
	}

	var txnreadCSVLogger *txnreadCSVLogger
	if txnreaderConfig.csvlog {
		txnreadCSVLogger, err = newTxnReadCSVLogger(ctx, txnreaderConfig.csvlogFilePrefix)
		if err != nil {
			return nil, err
		}
	}

	return &txnReader{
		config:                       config,
		slotSizeConn:                 slotSizeConn,
		replConn:                     replConn.PgConn(),
		protocolVersion:              txnreaderConfig.protocolVersion,
		clientXLogPos:                0,
		relation:                     nil,
		nextStandbyMessageDeadline:   time.Time{},
		typeMap:                      pgtype.NewMap(),
		inStreamV2:                   false,
		inTxnReadV1:                  false,
		preparedTxnsV3:               map[uint32]*txnInfo{},
		uncommittedTxnsV2:            map[uint32]*txnInfo{},
		inProgressTxnV1:              nil,
		processedTxn:                 nil,
		currentXidV2:                 0,
		txnStartTime:                 time.Time{},
		csvlogInterval:               time.Duration(txnreaderConfig.csvlogInterval) * time.Second,
		txnreadCSVLogger:             txnreadCSVLogger,
		currentSecond:                time.Now(),
		recordsReadInCurrentDuration: 0,
	}, nil
}

func (t *txnReader) getSlotSizeText(ctx context.Context) (string, error) {
	// little extra for better formatting in logs
	row := t.slotSizeConn.QueryRow(ctx,
		`SELECT
		regexp_replace(
			pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(),restart_lsn)),
			' ',
			'',
			'g'
		)
	FROM
		pg_replication_slots
	WHERE
		slot_name=$1`,
		t.config.slotname)

	var slotSizeText string
	err := row.Scan(&slotSizeText)
	if err != nil {
		return "", fmt.Errorf("failed to check size of slot: %w", err)
	}
	return slotSizeText, nil
}

func (t *txnReader) getSlotSizeInMB(ctx context.Context) (int, error) {
	row := t.slotSizeConn.QueryRow(ctx,
		`SELECT
			(pg_wal_lsn_diff(pg_current_wal_lsn(),restart_lsn) / 1024 / 1024)::bigint
	FROM
		pg_replication_slots
	WHERE
		slot_name=$1`,
		t.config.slotname)

	var slotSizeInMB int
	err := row.Scan(&slotSizeInMB)
	if err != nil {
		return 0, fmt.Errorf("failed to check size of slot: %w", err)
	}
	return slotSizeInMB, nil
}

func (t *txnReader) processMessage(walData []byte) error {
	t.processedTxn = nil

	var logicalMsg pglogrepl.Message
	var err error
	switch t.protocolVersion {
	case 1:
		logicalMsg, err = pglogrepl.Parse(walData)
	case 2:
		logicalMsg, err = pglogrepl.ParseV2(walData, t.inStreamV2)
	case 3:
		logicalMsg, err = pglogrepl.ParseV3(walData, t.inStreamV2)
	default:
		return fmt.Errorf("invalid protocol version")
	}
	if err != nil {
		slog.Error("error parsing logical replication message", "err", err)
		return fmt.Errorf("error parsing logical replication message")
	}

	v1TxnBegin := func(xid uint32) {
		t.inProgressTxnV1 = newTxnInfo(xid, t.typeMap, t.relation)
		t.inTxnReadV1 = true
		t.txnStartTime = time.Now()
	}

	v2GetSubxact := func(xid uint32) (*txnInfo, error) {
		uncommittedTxn, ok := t.uncommittedTxnsV2[t.currentXidV2]
		if !ok {
			slog.Error("unable to find top level transaction", slog.Any("xid", t.currentXidV2))
			return nil, fmt.Errorf("unable to find top level transaction")
		}
		return uncommittedTxn.addAndReturnSubXact(xid), nil
	}

	switch logicalMsg := logicalMsg.(type) {
	case *pglogrepl.RelationMessage:
		if logicalMsg.RelationName != t.config.tablename {
			slog.Warn("received RelationMessage for unexpected relation",
				"relationID", logicalMsg.RelationID,
				"relationName", logicalMsg.RelationName)
		} else {
			slog.Info("received RelationMessage for table",
				"relationID", logicalMsg.RelationID,
				"relationName", logicalMsg.RelationName)
			t.relation = logicalMsg
			t.inProgressTxnV1.updateRelationMessage(logicalMsg)
		}

	case *pglogrepl.RelationMessageV2:
		if logicalMsg.RelationName != t.config.tablename {
			slog.Warn("received RelationMessage for unexpected relation",
				"relationID", logicalMsg.RelationID,
				"relationName", logicalMsg.RelationName,
				"xid", logicalMsg.Xid)
			return nil
		} else {
			// receiving RelationMessage with xid as part of a stream, only use for the same txn
			if logicalMsg.Xid > 0 {
				slog.Info("received RelationMessage for table",
					"relationID", logicalMsg.RelationID,
					"relationName", logicalMsg.RelationName,
					"xid", logicalMsg.Xid)
				uncommittedTxn, err := v2GetSubxact(logicalMsg.Xid)
				if err != nil {
					return err
				}
				uncommittedTxn.updateRelationMessage(&logicalMsg.RelationMessage)
			} else {
				// received RelationMessage without xid as part of committed txn, use globally
				slog.Info("received RelationMessage for table",
					"relationID", logicalMsg.RelationID,
					"relationName", logicalMsg.RelationName)
				t.relation = &logicalMsg.RelationMessage
				t.inProgressTxnV1.updateRelationMessage(&logicalMsg.RelationMessage)
			}

		}

	case *pglogrepl.BeginMessage:
		slog.Info("received BeginMessage, beginning reading of committed transanction", "xid", logicalMsg.Xid)
		v1TxnBegin(logicalMsg.Xid)

	case *pglogrepl.CommitMessage:
		slog.Info("received CommitMessage, finished reading of committed transanction",
			slog.String("commitLSN", logicalMsg.CommitLSN.String()))
		t.inProgressTxnV1.commitTxn(logicalMsg.CommitLSN)
		t.inTxnReadV1 = false
		t.processedTxn = t.inProgressTxnV1
		return nil

	case *pglogrepl.InsertMessage:
		if t.relation == nil {
			slog.Error("received InsertMessage without corresponding RelationMessage", "relationID", logicalMsg.RelationID)
			return fmt.Errorf("received InsertMessage without corresponding RelationMessage")
		}
		if err := t.inProgressTxnV1.processInsertMessageV1(logicalMsg); err != nil {
			return err
		}

	case *pglogrepl.InsertMessageV2:
		txnInfo := t.inProgressTxnV1
		if logicalMsg.Xid > 0 {
			uncommittedTxn, err := v2GetSubxact(logicalMsg.Xid)
			if err != nil {
				return err
			}
			txnInfo = uncommittedTxn
		}

		if err := txnInfo.processInsertMessageV2(logicalMsg); err != nil {
			return err
		}

	case *pglogrepl.StreamStartMessageV2:
		// for decoding messages while reading the stream
		t.inStreamV2 = true
		_, ok := t.uncommittedTxnsV2[logicalMsg.Xid]
		if !ok {
			// should be 1 if we don't have a map entry
			if logicalMsg.FirstSegment == 0 {
				slog.Warn("received new transaction with FirstSegment!=1", "xid", logicalMsg.Xid)
			}
			t.uncommittedTxnsV2[logicalMsg.Xid] = newTxnInfo(logicalMsg.Xid, t.typeMap, t.relation)
		}
		t.currentXidV2 = logicalMsg.Xid
		t.txnStartTime = time.Now()
		slog.Info("received StreamStartMessage, reading messages for transaction", "xid", logicalMsg.Xid)

	case *pglogrepl.StreamStopMessageV2:
		t.inStreamV2 = false
		uncommittedTxn, ok := t.uncommittedTxnsV2[t.currentXidV2]
		if !ok {
			return fmt.Errorf("received StreamStopMessage for xid %d without transaction information", t.currentXidV2)
		}
		uncommittedTxn.txnReadTime += time.Since(t.txnStartTime)
		uncommittedTxn.streamCount += 1
		slog.Info("received StreamStopMessage, continuing", "xid", t.currentXidV2, "currentCount", uncommittedTxn.getTotalCount())

	case *pglogrepl.StreamCommitMessageV2:
		// only top level transactions can commit, no need to lookup subxact
		uncommittedTxn, ok := t.uncommittedTxnsV2[logicalMsg.Xid]
		if !ok {
			slog.Error("received StreamCommitMessage for unknown transaction",
				"xid", logicalMsg.Xid)
			return fmt.Errorf("received StreamCommitMessage for xid %d without transaction information", logicalMsg.Xid)
		}
		uncommittedTxn.commitTxn(logicalMsg.CommitLSN)
		// flag a transaction as being fully processed
		t.processedTxn = uncommittedTxn
		// txn is committed, so we can use its relationMessage globally
		t.relation = uncommittedTxn.relation
		slog.Info("received StreamCommitMessage, transaction finalized", "xid", logicalMsg.Xid)

	case *pglogrepl.StreamAbortMessageV2:
		topLevelTxn, ok := t.uncommittedTxnsV2[logicalMsg.Xid]
		if !ok {
			slog.Error("received StreamAbortMessage for unknown transaction", "xid", logicalMsg.Xid)
			return fmt.Errorf("received StreamAbortMessage for unknown xid %d", t.currentXidV2)
		}
		if logicalMsg.Xid == logicalMsg.SubXid {
			// top level transaction
			slog.Info("received StreamAbortMessage, transaction aborted", "xid", logicalMsg.Xid)
			return nil
		}
		err := topLevelTxn.abortSubXact(logicalMsg.SubXid)
		if err != nil {
			return err
		}
		slog.Info("received StreamAbortMessage, subtransaction aborted", "xid", logicalMsg.Xid, "subXid", logicalMsg.SubXid)

	case *pglogrepl.BeginPrepareMessageV3:
		slog.Info("received BeginPrepareMessage",
			slog.String("prepareLSN", logicalMsg.PrepareLSN.String()),
			slog.String("transactionEndLSN", logicalMsg.TransactionEndLSN.String()),
			slog.Any("xid", logicalMsg.Xid),
			slog.String("gid", logicalMsg.Gid))

		// twophase transaction sent under v1 semantics
		v1TxnBegin(logicalMsg.Xid)

	case *pglogrepl.PrepareMessageV3:
		slog.Info("received PrepareMessage",
			slog.String("prepareLSN", logicalMsg.PrepareLSN.String()),
			slog.String("transactionEndLSN", logicalMsg.TransactionEndLSN.String()),
			slog.Any("xid", logicalMsg.Xid),
			slog.String("gid", logicalMsg.Gid))

		if t.inProgressTxnV1.xid != logicalMsg.Xid {
			slog.Error("received PrepareMessage for unknown transaction",
				"xid", logicalMsg.Xid)
			return fmt.Errorf("received PrepareMessage for unknown transaction")
		}
		t.inProgressTxnV1.prepareTxn(logicalMsg.Gid)

		t.preparedTxnsV3[t.inProgressTxnV1.xid] = t.inProgressTxnV1
		t.inTxnReadV1 = false
		t.inProgressTxnV1 = nil

	case *pglogrepl.CommitPreparedMessageV3:
		slog.Info("received CommitPreparedMessage",
			slog.String("commitLSN", logicalMsg.CommitLSN.String()),
			slog.String("transactionEndLSN", logicalMsg.TransactionEndLSN.String()),
			slog.Any("xid", logicalMsg.Xid),
			slog.String("gid", logicalMsg.Gid))

		preparedTxn, ok := t.preparedTxnsV3[logicalMsg.Xid]
		if !ok {
			slog.Error("received CommitPreparedMessage for unknown prepared transaction",
				"xid", logicalMsg.Xid)
			return fmt.Errorf("received CommitPreparedMessage for unknown prepared transaction")
		}
		preparedTxn.commitTxn(logicalMsg.CommitLSN)
		t.processedTxn = preparedTxn
		slog.Info("received CommitPreparedMessage, prepared transaction finalized",
			slog.Any("xid", logicalMsg.Xid))
		return nil

	case *pglogrepl.RollbackPreparedMessageV3:
		slog.Info("received RollbackPreparedMessage",
			slog.String("transactionRollbackLSN", logicalMsg.TransactionRollbackLSN.String()),
			slog.String("transactionEndLSN", logicalMsg.TransactionEndLSN.String()),
			slog.Any("xid", logicalMsg.Xid),
			slog.String("gid", logicalMsg.Gid))

		if _, ok := t.preparedTxnsV3[logicalMsg.Xid]; !ok {
			slog.Error("received RollbackPreparedMessage for unknown transaction",
				"xid", logicalMsg.Xid)
			return fmt.Errorf("received RollbackPreparedMessage for unknown transaction")
		}
		t.preparedTxnsV3[logicalMsg.Xid] = nil
		slog.Info("received RollbackPreparedMessage, prepared transaction destroyed",
			slog.Any("xid", logicalMsg.Xid))

	case *pglogrepl.StreamPrepareMessageV3:
		slog.Info("received StreamPrepareMessage",
			slog.String("prepareLSN", logicalMsg.PrepareLSN.String()),
			slog.String("transactionEndLSN", logicalMsg.TransactionEndLSN.String()),
			slog.Any("xid", logicalMsg.Xid),
			slog.String("gid", logicalMsg.Gid))

		// twophase transaction sent under v2 semantics
		uncommittedTxn, ok := t.uncommittedTxnsV2[logicalMsg.Xid]
		if !ok {
			slog.Error("received StreamPrepareMessage for unknown transaction",
				"xid", logicalMsg.Xid)
			return fmt.Errorf("received StreamPrepareMessage for unknown transaction")
		}
		uncommittedTxn.prepareTxn(logicalMsg.Gid)
		t.preparedTxnsV3[logicalMsg.Xid] = uncommittedTxn
		t.uncommittedTxnsV2[logicalMsg.Xid] = nil
		slog.Info("received StreamPrepareMessage, transaction prepared", slog.Any("xid", logicalMsg.Xid))

	default:
		slog.Warn("unexpected message type in pgoutput stream:", "logicalMsg", logicalMsg)
	}

	return nil
}

func (t *txnReader) processTxn(ctx context.Context) (*txnInfo, error) {
	for {
		if time.Now().After(t.nextStandbyMessageDeadline) {
			err := pglogrepl.SendStandbyStatusUpdate(ctx, t.replConn,
				pglogrepl.StandbyStatusUpdate{WALWritePosition: t.clientXLogPos})
			if err != nil {
				return nil, fmt.Errorf("failed to send StandbyStatusUpdate: %w", err)
			}
			slotSizeText, err := t.getSlotSizeText(ctx)
			if err != nil {
				return nil, err
			}
			if t.txnreadCSVLogger != nil {
				slotSizeInMB, err := t.getSlotSizeInMB(ctx)
				if err != nil {
					return nil, err
				}
				// fetching seconds independently as t.currentSecond only updates when we get inserts
				t.txnreadCSVLogger.writeSlotSize(time.Now().Format(time.RFC3339), slotSizeInMB)
				if time.Since(t.currentSecond) > t.csvlogInterval {
					err := t.txnreadCSVLogger.writeRecordsRead(t.currentSecond.Format(time.RFC3339),
						t.recordsReadInCurrentDuration)
					if err != nil {
						return nil, err
					}
					t.recordsReadInCurrentDuration = 0
					t.currentSecond = time.Now()
				}
			}
			slog.Info("sent StandbyStatusUpdate", "clientXLogPos", t.clientXLogPos.String(), "slotSize", slotSizeText)
			t.nextStandbyMessageDeadline = time.Now().Add(t.csvlogInterval)
		}

		ctx, cancel := context.WithDeadline(ctx, t.nextStandbyMessageDeadline)
		rawMsg, err := t.replConn.ReceiveMessage(ctx)
		cancel()
		if err != nil {
			if pgconn.Timeout(err) {
				continue
			}
			return nil, fmt.Errorf("ReceiveMessage failed: %w", err)
		}

		if errMsg, ok := rawMsg.(*pgproto3.ErrorResponse); ok {
			return nil, fmt.Errorf("received Postgres WAL error: %+v", errMsg)
		}

		msg, ok := rawMsg.(*pgproto3.CopyData)
		if !ok {
			log.Printf("Received unexpected message: %T\n", rawMsg)
			continue
		}

		switch msg.Data[0] {
		case pglogrepl.PrimaryKeepaliveMessageByteID:
			pkm, err := pglogrepl.ParsePrimaryKeepaliveMessage(msg.Data[1:])
			if err != nil {
				return nil, fmt.Errorf("ParsePrimaryKeepaliveMessage failed: %w", err)
			}
			slog.Debug("Primary Keepalive Message",
				"serverWALEnd", pkm.ServerWALEnd,
				"serverTime", pkm.ServerTime,
				"replyRequested", pkm.ReplyRequested)
			if pkm.ServerWALEnd > t.clientXLogPos {
				t.clientXLogPos = pkm.ServerWALEnd
			}
			if pkm.ReplyRequested {
				t.nextStandbyMessageDeadline = time.Time{}
			}

		case pglogrepl.XLogDataByteID:
			xld, err := pglogrepl.ParseXLogData(msg.Data[1:])
			if err != nil {
				return nil, fmt.Errorf("ParseXLogData failed: %w", err)
			}

			err = t.processMessage(xld.WALData)
			if err != nil {
				return nil, err
			}
			if t.processedTxn != nil {
				return t.processedTxn, nil
			}

			if xld.WALStart > t.clientXLogPos {
				t.clientXLogPos = xld.WALStart
			}
		}
	}
}

func txnreaderMain(ctx context.Context, config *polorexConfig, txnreaderConfig *txnreaderConfig) error {
	txnreader, err := newTxnReader(ctx, config, txnreaderConfig)
	if err != nil {
		return err
	}
	if txnreader.txnreadCSVLogger != nil {
		defer txnreader.txnreadCSVLogger.close()
	}

	for {
		// blocks until we have a transacion
		txn, err := txnreader.processTxn(ctx)
		if err != nil {
			return err
		}
		slog.Info("Finished reading transaction",
			slog.Int64("minID", txn.getMinID()),
			slog.Int64("maxID", txn.getMaxID()),
			slog.Uint64("count", txn.getTotalCount()),
			"commitLSN", txn.commitLSN,
			"txnReadTime", txn.txnReadTime,
			slog.Uint64("streamCount", txn.getTotalStreamCount()))
	}
}

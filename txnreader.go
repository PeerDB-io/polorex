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

type txnStatus int

const (
	txnUnknown txnStatus = iota
	txnNone
	txnV1
	txnV2
)

type txnReader struct {
	ctx    context.Context
	config *polorexConfig
	// used to read slot size when sending standby messages
	slotSizeConn *pgx.Conn
	// used for START_REPLICATION, gets messages in a loop
	replConn          *pgconn.PgConn
	protocolVersionV2 bool
	// for sending standby messages
	clientXLogPos pglogrepl.LSN
	// global -> used only for transactions confirmed to be committed
	relation                   *pglogrepl.RelationMessage
	csvlogInterval             time.Duration
	nextStandbyMessageDeadline time.Time
	typeMap                    *pgtype.Map
	// set between StreamStart and StreamStop, needs to be passed to pglogrepl message parsing logic
	// all InsertMessages that come when this is set are part of the same txn
	inStreamV2 bool
	// set between Begin and Commit, all InsertMessages that come when this is set are part of the same txn
	inTxnReadV1 bool
	// we could be reading multiple streamed transactions at the same time, key is xid
	uncommittedTxnsV2 map[uint32]*txnInfo
	// to hold the transaction we are fully reading from Begin to Commit
	inprogressTxnV1 *txnInfo
	// txnNone if no transaction has finished processing
	// txnV1 if a committed transaction is finished reading
	// txnV2 if a transaction has finished streaming and was committed via StreamCommit
	txnStatus txnStatus
	// for logging in StreamStop, and if txnV2 status the committed txn is this
	currentXid uint32
	// not sure how much of an optimization this is
	insertValues map[string]interface{}
	// used for timing how much time each transaction takes to read
	txnStartTime time.Time
	// for logging to CSV file
	txnreadCSVLogger             *txnreadCSVLogger
	currentSecond                time.Time
	recordsReadInCurrentDuration int
}

type polorexRow struct {
	id  int64
	txt string
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
	protocolVersionV2 := (txnreaderConfig.protocolVersion >= 2)
	if protocolVersionV2 {
		pluginArgs = append(pluginArgs, "streaming 'true'")
	}

	sysident, err := pglogrepl.IdentifySystem(ctx, replConn.PgConn())
	if err != nil {
		return nil, fmt.Errorf("IdentifySystem failed: %w", err)
	}

	err = pglogrepl.StartReplication(ctx, replConn.PgConn(), config.slotname, sysident.XLogPos,
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
		ctx:                          ctx,
		config:                       config,
		slotSizeConn:                 slotSizeConn,
		replConn:                     replConn.PgConn(),
		protocolVersionV2:            protocolVersionV2,
		clientXLogPos:                sysident.XLogPos,
		relation:                     nil,
		csvlogInterval:               time.Duration(txnreaderConfig.csvlogInterval) * time.Second,
		nextStandbyMessageDeadline:   time.Time{},
		typeMap:                      pgtype.NewMap(),
		inStreamV2:                   false,
		inTxnReadV1:                  false,
		uncommittedTxnsV2:            map[uint32]*txnInfo{},
		inprogressTxnV1:              nil,
		txnStatus:                    txnUnknown,
		currentXid:                   0,
		insertValues:                 make(map[string]interface{}, 2),
		txnStartTime:                 time.Time{},
		currentSecond:                time.Now(),
		recordsReadInCurrentDuration: 0,
		txnreadCSVLogger:             txnreadCSVLogger,
	}, nil
}

func (t *txnReader) decodeTextColumnData(data []byte, dataType uint32) (interface{}, error) {
	if dt, ok := t.typeMap.TypeForOID(dataType); ok {
		return dt.Codec.DecodeValue(t.typeMap, dataType, pgtype.TextFormatCode, data)
	}
	return string(data), nil
}

func (t *txnReader) getSlotSizeText() (string, error) {
	// little extra for better formatting in logs
	row := t.slotSizeConn.QueryRow(t.ctx,
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

func (t *txnReader) getSlotSizeInMB() (int, error) {
	row := t.slotSizeConn.QueryRow(t.ctx,
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

func (t *txnReader) processInsertMessage(
	insertMsg *pglogrepl.InsertMessage,
	relationMsg *pglogrepl.RelationMessage,
) (*polorexRow, error) {
	for idx, col := range insertMsg.Tuple.Columns {
		colName := relationMsg.Columns[idx].Name
		if colName != "id" && colName != "txt" {
			slog.Warn("received unexpected column in InsertMessage", "colName", colName)
		}
		switch col.DataType {
		case 'n': // null
			slog.Warn("received unexpected null value in InsertMessage", "colName", colName)
		case 'u': // unchanged toast
			slog.Warn("received unexpected unchanged toast value in InsertMessage", "colName", colName)
		case 't': // text
			val, err := t.decodeTextColumnData(col.Data, relationMsg.Columns[idx].DataType)
			if err != nil {
				return nil, fmt.Errorf("failed to decode column data: %w", err)
			}
			t.insertValues[colName] = val
		}
	}

	id, ok := t.insertValues["id"].(int64)
	if !ok {
		return nil, fmt.Errorf("did not receive column %s in InsertMessage ", "id")
	}
	txt, ok := t.insertValues["txt"].(string)
	if !ok {
		return nil, fmt.Errorf("did not receive column %s in InsertMessage", "txt")
	}
	clear(t.insertValues)

	t.recordsReadInCurrentDuration++
	return &polorexRow{id: id, txt: txt}, nil
}

func (t *txnReader) processMessage(walData []byte) error {
	t.txnStatus = txnNone

	var logicalMsg pglogrepl.Message
	var err error
	if t.protocolVersionV2 {
		logicalMsg, err = pglogrepl.ParseV2(walData, t.inStreamV2)
	} else {
		logicalMsg, err = pglogrepl.Parse(walData)
	}

	if err != nil {
		slog.Error("error parsing logical replication message", "err", err)
		return fmt.Errorf("error parsing logical replication message")
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
				_, ok := t.uncommittedTxnsV2[logicalMsg.Xid]
				if !ok {
					slog.Error("received RelationMessage for unknown transaction",
						"relationID", logicalMsg.RelationID,
						"xid", logicalMsg.Xid)
					return fmt.Errorf("received RelationMessage for unknown streaming transaction")
				}
				t.uncommittedTxnsV2[logicalMsg.Xid].relationV2 = logicalMsg
			} else {
				// received RelationMessage without xid as part of committed txn, use globally
				slog.Info("received RelationMessage for table",
					"relationID", logicalMsg.RelationID,
					"relationName", logicalMsg.RelationName)
				t.relation = &logicalMsg.RelationMessage
			}

		}

	case *pglogrepl.BeginMessage:
		slog.Info("received BeginMessage, beginning reading of committed transanction", "xid", logicalMsg.Xid)
		t.inprogressTxnV1 = newTxnInfo(logicalMsg.Xid)
		t.inTxnReadV1 = true
		t.txnStartTime = time.Now()

	case *pglogrepl.CommitMessage:
		slog.Info("received CommitMessage, finished reading of committed transanction", "commitLSN", logicalMsg.CommitLSN)
		t.inprogressTxnV1.committed = true
		t.inprogressTxnV1.commitLSN = logicalMsg.CommitLSN
		t.inTxnReadV1 = false
		t.txnStatus = txnV1
		t.inprogressTxnV1.txnReadTime += time.Since(t.txnStartTime)
		return nil

	case *pglogrepl.InsertMessage:
		if t.relation == nil {
			slog.Error("received InsertMessage without corresponding RelationMessage", "relationID", logicalMsg.RelationID)
			return fmt.Errorf("received InsertMessage without corresponding RelationMessage")
		}
		row, err := t.processInsertMessage(logicalMsg, t.relation)
		if err != nil {
			return err
		}
		t.inprogressTxnV1.updateTxnInfoWithRow(row)

	case *pglogrepl.InsertMessageV2:
		var relationMessage *pglogrepl.RelationMessage
		// part of a stream, process with its RelationMessage
		if logicalMsg.Xid > 0 {
			_, ok := t.uncommittedTxnsV2[logicalMsg.Xid]
			if !ok {
				slog.Error("received InsertMessage for unknown streaming transaction",
					"relationID", logicalMsg.RelationID,
					"xid", logicalMsg.Xid)
				return fmt.Errorf("received InsertMessage for unknown streaming transaction")
			}
			relationMessage = &t.uncommittedTxnsV2[logicalMsg.Xid].relationV2.RelationMessage
		} else {
			if t.relation == nil {
				slog.Error("received InsertMessage without corresponding RelationMessage", "relationID", logicalMsg.RelationID)
				return fmt.Errorf("received InsertMessage without corresponding RelationMessage")
			}
			relationMessage = t.relation
		}

		row, err := t.processInsertMessage(&logicalMsg.InsertMessage, relationMessage)
		if err != nil {
			return err
		}

		if logicalMsg.Xid == 0 {
			t.inprogressTxnV1.updateTxnInfoWithRow(row)
		} else {
			uncommittedTxn, ok := t.uncommittedTxnsV2[logicalMsg.Xid]
			if ok {
				uncommittedTxn.updateTxnInfoWithRow(row)
			} else {
				return fmt.Errorf("received InsertMessageV2 for xid %d without transaction information", t.currentXid)
			}
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
			t.uncommittedTxnsV2[logicalMsg.Xid] = newTxnInfo(logicalMsg.Xid)
		}
		t.currentXid = logicalMsg.Xid
		t.txnStartTime = time.Now()
		slog.Info("received StreamStartMessage, reading messages for transaction", "xid", logicalMsg.Xid)

	case *pglogrepl.StreamStopMessageV2:
		t.inStreamV2 = false
		uncommittedTxn, ok := t.uncommittedTxnsV2[t.currentXid]
		if !ok {
			return fmt.Errorf("received StreamStopMessage for xid %d without transaction information", t.currentXid)
		}
		uncommittedTxn.txnReadTime += time.Since(t.txnStartTime)
		slog.Info("received StreamStopMessage, continuing", "xid", t.currentXid, "currentCount", uncommittedTxn.count)

	case *pglogrepl.StreamCommitMessageV2:
		uncommittedTxn, ok := t.uncommittedTxnsV2[logicalMsg.Xid]
		if ok {
			uncommittedTxn.committed = true
			uncommittedTxn.commitLSN = logicalMsg.CommitLSN
			// flag a transaction as being fully processed
			t.txnStatus = txnV2
			t.currentXid = uncommittedTxn.xid
			slog.Info("received StreamCommitMessage, transaction finalized", "xid", logicalMsg.Xid)
		} else {
			return fmt.Errorf("received StreamStopMessage for xid %d without transaction information", t.currentXid)
		}

	case *pglogrepl.StreamAbortMessageV2:
		_, ok := t.uncommittedTxnsV2[logicalMsg.Xid]
		if ok {
			t.uncommittedTxnsV2[logicalMsg.Xid] = nil
			slog.Info("received StreamAbortMessage, transaction destroyed", "xid", logicalMsg.Xid)
		} else {
			return fmt.Errorf("received StreamStopMessage for xid %d without transaction information", t.currentXid)
		}
	default:
		slog.Warn("unexpected message type in pgoutput stream: %T", logicalMsg)
	}

	return nil
}

func (t *txnReader) processTxn() (*txnInfo, error) {
	for {
		if time.Now().After(t.nextStandbyMessageDeadline) {
			err := pglogrepl.SendStandbyStatusUpdate(t.ctx, t.replConn,
				pglogrepl.StandbyStatusUpdate{WALWritePosition: t.clientXLogPos})
			if err != nil {
				return nil, fmt.Errorf("failed to send StandbyStatusUpdate: %w", err)
			}
			slotSizeText, err := t.getSlotSizeText()
			if err != nil {
				return nil, err
			}
			if t.txnreadCSVLogger != nil {
				slotSizeInMB, err := t.getSlotSizeInMB()
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

		ctx, cancel := context.WithDeadline(t.ctx, t.nextStandbyMessageDeadline)
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
			if t.txnStatus == txnV1 {
				return t.inprogressTxnV1, nil
			} else if t.txnStatus == txnV2 {
				return t.uncommittedTxnsV2[t.currentXid], nil
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
		txn, err := txnreader.processTxn()
		if err != nil {
			return err
		}
		slog.Info("Finished reading transaction",
			"minID", txn.minID,
			"maxID", txn.maxID,
			"count", txn.count,
			"commitLSN", txn.commitLSN,
			"txnReadTime", txn.txnReadTime)
	}
}

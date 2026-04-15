package main

import (
	"errors"
	"fmt"
	"log/slog"
	"math"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgtype"
)

type txnInfo struct {
	xid       uint32
	count     uint64
	minID     int64
	maxID     int64
	committed bool
	commitLSN pglogrepl.LSN
	// start with global relationMessage, if txn is streamed
	// it may give a different relationMessage
	relation *pglogrepl.RelationMessage
	// assuming readonly, global to txnReader
	typeMap     *pgtype.Map
	txnReadTime time.Duration
	// only when slot and replication is configured for two-phase commit
	prepared bool
	// user-defined transaction GID
	gid string
	// 0 for v1 transactions, >0 for v2 (streamed) transactions
	streamCount uint64
	// only for v2 transactions, they may have subxacts that need to be handled/aborted separately
	subXactsV2 map[uint32]*txnInfo
}

func newTxnInfo(xid uint32, typeMap *pgtype.Map, relationMessage *pglogrepl.RelationMessage) *txnInfo {
	return &txnInfo{
		xid:         xid,
		count:       0,
		minID:       math.MaxInt64,
		maxID:       math.MinInt64,
		committed:   false,
		commitLSN:   0,
		relation:    relationMessage,
		typeMap:     typeMap,
		txnReadTime: 0,
		prepared:    false,
		gid:         "",
		streamCount: 0,
		subXactsV2:  nil,
	}
}

func (t *txnInfo) updateRelationMessage(relationMessage *pglogrepl.RelationMessage) {
	t.relation = relationMessage
}

func (t *txnInfo) decodeTextColumnData(data []byte, dataType uint32) (any, error) {
	if dt, ok := t.typeMap.TypeForOID(dataType); ok {
		return dt.Codec.DecodeValue(t.typeMap, dataType, pgtype.TextFormatCode, data)
	}
	return string(data), nil
}

func (t *txnInfo) processInsertMessage(insertMsg *pglogrepl.InsertMessage, xid uint32) error {
	if t.relation == nil {
		slog.Error("received InsertMessage without corresponding RelationMessage",
			slog.Any("relationID", insertMsg.RelationID))
		return fmt.Errorf("received InsertMessage without corresponding RelationMessage")
	}

	var id *int64
	var txt *string
	for idx, col := range insertMsg.Tuple.Columns {
		colName := t.relation.Columns[idx].Name
		if colName != "id" && colName != "txt" {
			slog.Warn("received unexpected column in InsertMessage", "colName", colName)
		}
		switch col.DataType {
		case 'n': // null
			slog.Warn("received unexpected null value in InsertMessage", "colName", colName)
		case 'u': // unchanged toast
			slog.Warn("received unexpected unchanged toast value in InsertMessage", "colName", colName)
		case 't': // text
			val, err := t.decodeTextColumnData(col.Data, t.relation.Columns[idx].DataType)
			if err != nil {
				return fmt.Errorf("failed to decode column data: %w", err)
			}
			if colName == "id" {
				idVal, ok := val.(int64)
				if !ok {
					return fmt.Errorf("failed to cast id as int64")
				}
				id = &idVal
				t.minID = min(t.minID, idVal)
				t.maxID = max(t.maxID, idVal)
			} else if colName == "txt" {
				txtVal, ok := val.(string)
				if !ok {
					return fmt.Errorf("failed to cast txt as string")
				}
				txt = &txtVal
			}
		}
	}

	if id == nil {
		return errors.New("did not receive column 'id' in InsertMessage")
	}
	if txt == nil {
		return errors.New("did not receive column 'txt' in InsertMessage")
	}
	t.count++
	slog.Debug("InsertMessage processed", slog.Int64("id", *id), slog.String("txt", *txt), slog.Any("xid", t.xid))
	return nil
}

func (t *txnInfo) processInsertMessageV1(insertMsg *pglogrepl.InsertMessage) error {
	return t.processInsertMessage(insertMsg, t.xid)
}

func (t *txnInfo) processInsertMessageV2(insertMsg *pglogrepl.InsertMessageV2) error {
	return t.processInsertMessage(&insertMsg.InsertMessage, insertMsg.Xid)
}

func (t *txnInfo) prepareTxn(gid string) {
	t.prepared = true
	t.gid = gid
}

func (t *txnInfo) commitTxn(commitLSN pglogrepl.LSN) {
	t.committed = true
	t.commitLSN = commitLSN
	for _, subXact := range t.subXactsV2 {
		subXact.committed = true
		subXact.commitLSN = commitLSN
	}
}

func (t *txnInfo) addAndReturnSubXact(xid uint32) *txnInfo {
	if xid == t.xid {
		return t
	} else if t.subXactsV2 == nil {
		t.subXactsV2 = make(map[uint32]*txnInfo)
	} else if uncommittedSubXact, ok := t.subXactsV2[xid]; ok {
		return uncommittedSubXact
	}
	slog.Info("New subtransaction added", "xid", t.xid, "subxid", xid)
	t.subXactsV2[xid] = newTxnInfo(xid, t.typeMap, t.relation)
	return t.subXactsV2[xid]
}

func (t *txnInfo) abortSubXact(xid uint32) error {
	if xid == t.xid {
		return errors.New("cannot abort toplevel transaction")
	} else if _, ok := t.subXactsV2[xid]; !ok {
		return errors.New("subtransaction not found")
	}
	delete(t.subXactsV2, xid)
	return nil
}

func (t *txnInfo) getMinID() int64 {
	minID := t.minID
	for _, subXact := range t.subXactsV2 {
		if subXact.minID < minID {
			minID = subXact.minID
		}
	}
	return minID
}

func (t *txnInfo) getMaxID() int64 {
	maxID := t.maxID
	for _, subXact := range t.subXactsV2 {
		if subXact.maxID > maxID {
			maxID = subXact.maxID
		}
	}
	return maxID
}

func (t *txnInfo) getTotalCount() uint64 {
	totalCount := t.count
	for _, subXact := range t.subXactsV2 {
		totalCount += subXact.count
	}
	return totalCount
}

func (t *txnInfo) getTotalStreamCount() uint64 {
	totalStreamCount := t.streamCount
	for _, subXact := range t.subXactsV2 {
		totalStreamCount += subXact.streamCount
	}
	return totalStreamCount
}

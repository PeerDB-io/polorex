package main

import (
	"math"
	"time"

	"github.com/jackc/pglogrepl"
)

type txnInfo struct {
	xid       uint32
	count     uint
	minID     int64
	maxID     int64
	committed bool
	commitLSN pglogrepl.LSN
	// only used for streamed transactions - can't use a global RelationMessage
	// since we might be processing several transactions at once
	relationV2  *pglogrepl.RelationMessageV2
	txnReadTime time.Duration
}

func newTxnInfo(xid uint32) *txnInfo {
	return &txnInfo{
		xid:         xid,
		count:       0,
		minID:       math.MaxInt64,
		maxID:       math.MinInt64,
		committed:   false,
		commitLSN:   0,
		txnReadTime: 0,
	}
}

func (t *txnInfo) updateTxnInfoWithRow(row *polorexRow) {
	t.count++
	if t.minID > row.id {
		t.minID = row.id
	}
	if t.maxID < row.id {
		t.maxID = row.id
	}
}

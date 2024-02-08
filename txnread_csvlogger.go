package main

import (
	"context"
	"encoding/csv"
	"fmt"
	"os"
	"time"
)

type txnreadCSVLogger struct {
	recordsReadCSVWriter *csv.Writer
	slotSizeCSVWriter    *csv.Writer
}

func createCSVWriter(fileName string) (*csv.Writer, error) {
	file, err := os.OpenFile(fileName, os.O_CREATE|os.O_WRONLY|os.O_EXCL, 0644)
	if err != nil {
		if os.IsExist(err) {
			return nil, fmt.Errorf("file '%s' already exists", fileName)
		} else {
			return nil, fmt.Errorf("error while creating file '%s': %w", fileName, err)
		}
	}

	return csv.NewWriter(file), nil
}

func newTxnReadCSVLogger(ctx context.Context, csvlogPrefix string) (*txnreadCSVLogger, error) {
	currentSecond := time.Now().Unix()
	recordsReadCSVWriter, err := createCSVWriter(fmt.Sprintf("%srecords_read_%d.csv", csvlogPrefix, currentSecond))
	if err != nil {
		return nil, err
	}
	slotSizeCSVWriter, err := createCSVWriter(fmt.Sprintf("%sslot_size_%d.csv", csvlogPrefix, currentSecond))
	if err != nil {
		return nil, err
	}

	return &txnreadCSVLogger{
		recordsReadCSVWriter: recordsReadCSVWriter,
		slotSizeCSVWriter:    slotSizeCSVWriter,
	}, nil
}

func (t *txnreadCSVLogger) close() error {
	if t.recordsReadCSVWriter != nil {
		// this is a very weird API
		t.recordsReadCSVWriter.Flush()
		if t.recordsReadCSVWriter.Error() != nil {
			return fmt.Errorf("error while closing CSV file: %w", t.recordsReadCSVWriter.Error())
		}
	}
	if t.slotSizeCSVWriter != nil {
		// this is a very weird API
		t.slotSizeCSVWriter.Flush()
		if t.slotSizeCSVWriter.Error() != nil {
			return fmt.Errorf("error while closing CSV file: %w", t.slotSizeCSVWriter.Error())
		}
	}

	return nil
}

func (t *txnreadCSVLogger) writeRecordsRead(currentSecond string, recordsRead int) error {
	err := t.recordsReadCSVWriter.Write([]string{currentSecond, fmt.Sprint(recordsRead)})
	if err != nil {
		return fmt.Errorf("failed to write record to CSV file: %w", err)
	}
	return nil
}

func (t *txnreadCSVLogger) writeSlotSize(currentSecond string, slotSizeMB int) error {
	err := t.slotSizeCSVWriter.Write([]string{currentSecond, fmt.Sprint(slotSizeMB)})
	if err != nil {
		return fmt.Errorf("failed to write record to CSV file: %w", err)
	}
	return nil
}

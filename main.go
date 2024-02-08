package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/urfave/cli/v3"
)

func main() {
	appCtx, appCancel := context.WithCancel(context.Background())
	// setup shutdown handling
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	// cancel the context when we receive a shutdown signal
	go func() {
		<-quit
		appCancel()
	}()

	hostFlag := &cli.StringFlag{
		Name:    "host",
		Value:   "localhost",
		Sources: cli.EnvVars("PGHOST", "POLOREX_HOST"),
	}
	portFlag := &cli.UintFlag{
		Name:    "port",
		Value:   5432,
		Sources: cli.EnvVars("PGPORT", "POLOREX_PORT"),
	}
	userFlag := &cli.StringFlag{
		Name:    "user",
		Value:   "postgres",
		Sources: cli.EnvVars("PGUSER", "POLOREX_USER"),
	}
	passwordFlag := &cli.StringFlag{
		Name:    "password",
		Value:   "postgres",
		Sources: cli.EnvVars("PGPASSWORD", "POLOREX_PASSWORD"),
	}
	databaseFlag := &cli.StringFlag{
		Name:    "database",
		Value:   "postgres",
		Sources: cli.EnvVars("PGDATABASE", "POLOREX_DATABASE"),
	}
	slotnameFlag := &cli.StringFlag{
		Name:    "slotname",
		Value:   "polorex_slot",
		Sources: cli.EnvVars("POLOREX_SLOTNAME"),
	}
	pubnameFlag := &cli.StringFlag{
		Name:    "pubname",
		Value:   "polorex_pub",
		Sources: cli.EnvVars("POLOREX_PUBNAME"),
	}
	tablenameFlag := &cli.StringFlag{
		Name:    "tablename",
		Value:   "polorex_table",
		Sources: cli.EnvVars("POLOREX_TABLENAME"),
	}

	txngenParallelismFlag := &cli.UintFlag{
		Name:    "parallelism",
		Value:   1,
		Sources: cli.EnvVars("POLOREX_PARALLELISM"),
	}
	txngenIterationsFlag := &cli.UintFlag{
		Name:    "iterations",
		Value:   1,
		Sources: cli.EnvVars("POLOREX_ITERATIONS"),
	}
	txngenBatchSizeFlag := &cli.UintFlag{
		Name:    "batchsize",
		Value:   1000,
		Sources: cli.EnvVars("POLOREX_BATCH_SIZE"),
	}
	txngenDelayMsFlag := &cli.UintFlag{
		Name:    "delayms",
		Value:   0,
		Sources: cli.EnvVars("POLOREX_DELAY_MS"),
	}
	txngenNoTxnFlag := &cli.BoolFlag{
		Name:    "notxn",
		Value:   false,
		Sources: cli.EnvVars("POLOREX_NOTXN"),
	}

	txnreadProtocolVersionFlag := &cli.UintFlag{
		Name:    "protocol",
		Value:   1,
		Sources: cli.EnvVars("POLOREX_PROTOCOL_VERSION"),
		Validator: func(i uint64) error {
			if i >= 1 && i <= 4 {
				return nil
			}
			return fmt.Errorf("invalid protocol version %d", i)
		},
	}
	txnreadCSVLogFlag := &cli.BoolFlag{
		Name:    "csvlog",
		Value:   false,
		Sources: cli.EnvVars("POLOREX_CSVLOG"),
	}
	txnreadCSVLogFrequencyFlag := &cli.UintFlag{
		Name:    "csvlog_interval",
		Usage:   "affects standby timeout and records read",
		Value:   60,
		Sources: cli.EnvVars("POLOREX_CSVLOG_INTERVAL"),
	}
	txnreadCSVLogFilePrefixFlag := &cli.StringFlag{
		Name:    "csvlog_file_prefix",
		Value:   "polorex_txnread_",
		Sources: cli.EnvVars("POLOREX_CSVLOG_FILE_PREFIX"),
	}

	app := &cli.Command{
		Name:  "polorex",
		Usage: "A simple Go application to showcase improvements in the Postgres logical replication protocol.",
		Commands: []*cli.Command{
			{
				Name:  "setup",
				Usage: `Connects to the database and creates a table, publication and slot.`,
				Action: func(ctx context.Context, cmd *cli.Command) error {
					return setupMain(ctx, &polorexConfig{
						host:      cmd.String("host"),
						port:      uint16(cmd.Uint("port")),
						user:      cmd.String("user"),
						password:  cmd.String("password"),
						database:  cmd.String("database"),
						slotname:  cmd.String("slotname"),
						pubname:   cmd.String("pubname"),
						tablename: cmd.String("tablename"),
					})
				},
				Flags: []cli.Flag{
					hostFlag,
					portFlag,
					userFlag,
					passwordFlag,
					databaseFlag,
					slotnameFlag,
					pubnameFlag,
					tablenameFlag,
				},
			},
			{
				Name:  "teardown",
				Usage: `Connects to the database and drops the created slot, publication and table.`,
				Action: func(ctx context.Context, cmd *cli.Command) error {
					return teardownMain(ctx, &polorexConfig{
						host:      cmd.String("host"),
						port:      uint16(cmd.Uint("port")),
						user:      cmd.String("user"),
						password:  cmd.String("password"),
						database:  cmd.String("database"),
						slotname:  cmd.String("slotname"),
						pubname:   cmd.String("pubname"),
						tablename: cmd.String("tablename"),
					})
				},
				Flags: []cli.Flag{
					hostFlag,
					portFlag,
					userFlag,
					passwordFlag,
					databaseFlag,
					slotnameFlag,
					pubnameFlag,
					tablenameFlag,
				},
			},
			{
				Name:  "txngen",
				Usage: `Connects to the database and inserts records into the table.`,
				Action: func(ctx context.Context, cmd *cli.Command) error {
					return txngenMain(ctx, &polorexConfig{
						host:      cmd.String("host"),
						port:      uint16(cmd.Uint("port")),
						user:      cmd.String("user"),
						password:  cmd.String("password"),
						database:  cmd.String("database"),
						slotname:  cmd.String("slotname"),
						pubname:   cmd.String("pubname"),
						tablename: cmd.String("tablename"),
					}, &txngenConfig{
						parallelism: int(cmd.Uint("parallelism")),
						iterations:  int(cmd.Uint("iterations")),
						batchSize:   int(cmd.Uint("batchsize")),
						delayMs:     int(cmd.Uint("delayms")),
						noTxn:       cmd.Bool("notxn"),
					})
				},
				Flags: []cli.Flag{
					hostFlag,
					portFlag,
					userFlag,
					passwordFlag,
					databaseFlag,
					slotnameFlag,
					pubnameFlag,
					tablenameFlag,
					txngenParallelismFlag,
					txngenIterationsFlag,
					txngenBatchSizeFlag,
					txngenDelayMsFlag,
					txngenNoTxnFlag,
				},
			},
			{
				Name:  "txnreader",
				Usage: `Connects to the database and reads transactions in a loop.`,
				Action: func(ctx context.Context, cmd *cli.Command) error {
					return txnreaderMain(ctx, &polorexConfig{
						host:      cmd.String("host"),
						port:      uint16(cmd.Uint("port")),
						user:      cmd.String("user"),
						password:  cmd.String("password"),
						database:  cmd.String("database"),
						slotname:  cmd.String("slotname"),
						pubname:   cmd.String("pubname"),
						tablename: cmd.String("tablename"),
					}, &txnreaderConfig{
						protocolVersion:  int(cmd.Uint("protocol")),
						csvlog:           cmd.Bool("csvlog"),
						csvlogInterval:   int(cmd.Uint("csvlog_interval")),
						csvlogFilePrefix: cmd.String("csvlog_file_prefix"),
					})
				},
				Flags: []cli.Flag{
					hostFlag,
					portFlag,
					userFlag,
					passwordFlag,
					databaseFlag,
					slotnameFlag,
					pubnameFlag,
					tablenameFlag,
					txnreadProtocolVersionFlag,
					txnreadCSVLogFrequencyFlag,
					txnreadCSVLogFlag,
					txnreadCSVLogFilePrefixFlag,
				},
			},
		},
	}

	if err := app.Run(appCtx, os.Args); err != nil {
		log.Fatalf("error running polorex: %v", err)
	}
}

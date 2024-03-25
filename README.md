# Polorex - running experiments with Postgres logical replication
This repository contains a simple Go application intended to showcase improvements in the Postgres logical replication since Postgres 14. It was primarily written for [this]() blog post.

## Building
Just [clone](https://docs.github.com/en/repositories/creating-and-managing-repositories/cloning-a-repository) this repository and then run `go build` from the repository root.

## Setup
Aside from the binary itself, we also need a running Postgres cluster to connect to. We strongly recommend spinning up a cluster just for this purpose, through [Docker](https://hub.docker.com/_/postgres) or similar. We need atleast Postgres 14 to replicate using protocol version 2. Cluster parameters (host,port,database,username,password) needs to be passed to all subcommands of `polorex`. 

`polorex` assumes it has permissions to do pretty much anything on the cluster, and will break in exciting ways if it doesn't. If you're not just giving it a superuser role, ensure it atleast has permissions to do the following:

1) CONNECT to the cluster
2) CREATE a table, publication and logical replication slot
3) REPLICATE from the logical replication slot
4) INSERT rows into the table we created
5) DROP/DELETE a table, publication and logical replication slot

The basic moving parts of a logical replication setup are a replication slot, a publication and a table. After the cluster is up and running, run the `setup` subcommand, to create all 3 of these.

```
➜ ./polorex setup -port 7132
2024/01/16 14:18:34 INFO Creating table tablename=polorex_table
2024/01/16 14:18:34 INFO Creating publication pubname=polorex_pub
2024/01/16 14:18:34 INFO Creating replication slot slotname=polorex_slot
```

When done using `polorex`, you should immediately drop the slot to prevent unnecessary growth of storage, since the slot is not being read actively anymore. Alternatively, you can spin down or delete the cluster yourself.

## Start reading
Rows can be inserted into the table manually, or preferably using the `txngen` subcommand of `polorex`. The rate and parallelism of inserts can be customized to your liking.

```
➜ ./polorex txngen -port 7132 -delayms 1000 -iterations 2 -batchsize 2
2024/01/18 01:16:25 INFO Inserting records into table thread=0 iterations=0/2 records=0/4
2024/01/18 01:16:26 INFO Inserting records into table thread=0 iterations=1/2 records=2/4
2024/01/18 01:16:26 INFO Finished inserting records totalRecords=4 minID=1 maxID=4
```

The `txnreader` subcommand of `polorex` connects to the replication slot we created earlier, and will now read all `INSERTs` into the created table. You can optionally specify the logical replication protocol version, which will influence how bigger transactions are read. Finally, even though the output is fairly verbose, statistics can be logged to CSV files for plotting/analysis.

```
➜ ./polorex txnreader -port 7132 -protocol 2
2024/01/19 12:47:11 INFO sent StandbyStatusUpdate clientXLogPos=0/3D97BE8 slotSize=56bytes
2024/01/19 12:47:41 INFO sent StandbyStatusUpdate clientXLogPos=0/3D97C20 slotSize=56bytes
```

## Issues?
In case there are any issues or bugs, please don't hesitate to contact us by filing a GitHub issue or mailing `kevin AT peerdb DOT io`.

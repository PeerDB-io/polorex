# Polorex - running experiments with Postgres logical replication
This repository contains a simple Go application intended to showcase improvements in the Postgres logical replication since Postgres 14. It was primarily written for this blog post.

## Building
Couldn't be simpler, [clone](https://docs.github.com/en/repositories/creating-and-managing-repositories/cloning-a-repository) this repository and then run `go build` from the repository root.

## Setup
Aside from the binary itself, we also need a running Postgres cluster to connect to. We strongly recommend spinning up a cluster just for this purpose, through [Docker](https://hub.docker.com/_/postgres) or similar. We need atleast Postgres 14 to replicate using protocol version 2. Cluster parameters (host,port,database,username,password) needs to be passed to all subcommands of `polorex`. 

`polorex` assumes it has permissions to do pretty much anything on the cluster, and will break in exciting ways if it doesn't. If you're not just giving it a superuser role, ensure it atleast has permissions to do the following:

1) CONNECT to the cluster
2) CREATE a table, publication and logical replication slot
3) REPLICATE from the logical replication slot
4) INSERT rows into the table we created
5) DELETE a table, publication and logical replication slot

The basic moving parts of a logical replication setup are a replication slot, a publication and a table. After the cluster is up and running, run the `setup` subcommand, to create all 3 of these.

## Generate transactions
`polorex`
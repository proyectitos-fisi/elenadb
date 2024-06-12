# Elena CLI

Run with

```bash
go run cmd/elenadb/cli.go
```

```console
NAME:
   elenadb - 🚄 The Elena Database

USAGE:
   elenadb <db> [query | file.sql]

VERSION:
   0.0.69-alpha

COMMANDS:
   <db>  db directory to work with

GLOBAL OPTIONS:
   --help, -h     show help
   --version, -v  print the version
```

The CLI supports executing one-line queries or reading from a file. See [queries.md](./queries.md)

```bash
elenadb ./db.elena "dame todo de users pe"

🚆 Running query 'dame todo de users pe' on database './db.elena'
```

```bash
elenadb ./db.elena bootstrap.sql

🚆 Running file 'bootstrap.sql' on database './db.elena'
```

If no query or file is provided, Elena will start a REPL session.

```bash
elenadb ./db.elena

🚄 Elena DB version 0.0.69-alph
elena>
```

## Creating a database

When executin a command, if the database argument doesn't resolve
to an existing database directory Elena will silently create one
just for you.

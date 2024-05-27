# Elena CLI

Run with

```bash
go run cmd/elenadb/cli.go
```

```console
NAME:
   elena - 🚄 The Elena Database

USAGE:
   elena [--create] <db> [query | file.sql]

VERSION:
   0.0.69-alpha

COMMANDS:
   <db>  db directory to work with

GLOBAL OPTIONS:
   --create       creates the <db> database (default: false)
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

## Creating a database

If you try to run a query on a non-existing database, you will get an error

```bash
elenadb ./db.elena "dame todo de users pe"

database db.elena does not exist
```

First, create the database with

```bash
elenadb --create ./db.elena

🚆 Creating db db.elena
```

You can also execute commands on a newly created database. Examples:

```bash
elenadb --create ./db.elena "creame tabla users pe"

🚆 Creating db db.elena
🚆 Running query 'creame tabla users pe' on db ./db.elena
```

```bash
elenadb --create ./db.elena bootstrap.sql

🚆 Creating db db.elena
🚆 Running file 'bootstrap.sql' on db ./db.elena
```

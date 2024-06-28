# Elena architecture

## Storage

A database directory is just a bunch of .table and .index files, plus a `elena.meta.table` file.

```text
mydb.elena
├── doctor.table
├── doctor.name.index
├── doctor.id.index
├── usuario.id.index
├── usuario.table
└── elena.meta.table
```

As you can see, a table can have multiple indexes. An index is created over a field (column) with
the name `<table>.<field>.index`.

### How tables are stored

A .table file is NO MORE than a bunch of **data pages** stored as slotted pages.

We are **not**:

- Using a BTree structure here. That's for indexes.
- Using a page directory. A page directory keeps track of pages metadata and its metadata whitin a table.
  We are using a simple approach: Our pages are contiguous and we assume the last page is the one
  that contains the most recent data, and may have free space.

#### Data page

So a table file is made up of data pages. Each data page is a slotted page structured as follows:

```text
-------------------------------------------------------------------
|  HEADER (8 bytes)  |  SLOTS  |  ..........  |  INSERTED TUPLES  |
-------------------------------------------------------------------
```

- HEADER:

    ```text
    |------------------------------- HEADER --------------------------------|
    -------------------------------------------------------------------------
    | NumTuples(2) | NumDeletedTuples(2) | FreeSpace(2) | LastUsedOffset(2) |
    -------------------------------------------------------------------------

    2 + 2 + 2 + 2 = 8 bytes
    ```

- SLOTS:

    ```text
    |-------- Slot 1 ---------|-------- Slot 2 ---------|
    ---------------------------------------------------------
    | Offset (2) | Length (2) | Offset (2) | Length (2) | ...
    ---------------------------------------------------------
    ```

    Each slot is 4 bytes. 2 bytes for the tuple offset and 2 bytes for the tuple length.

    Note: the offset is relative to the beginning of the page.

- INSERTED TUPLES:

    ```text
    |--- Tuple 1 ---|--- Tuple 2 ---|
    -------------------------------------
    |    Data (N)   |    Data (N)   | ...
    -------------------------------------
    ```

    Note that there's no 'deleted' bit. A deleted tuple is just a tuple that we lost track of.
    That is, in order to delete a tuple you just nullify the slot (i.e., set to zero).

### How indexes are storaged

@eduardo needs to write this section.

```text
-------------------------------------------------------------------
|
-------------------------------------------------------------------
```

### Meta table

<!-- Tony reference code https://github.com/antoniosarosi/mkdb/blob/bf1341bc4da70971fc6c340f3a5e9c6bbc55da37/src/db.rs#L383-L397 -->

The meta table is a builtin table hardcoded in the Elena source code. It contains metadata about the
other tables and indexes in the database. Each database MUST have a meta table called `elena.meta.table`.

```text
+----------------+---------------------+-----------+--------------+--------------------------+
| type           | name                | file_id   | root         | sql                      |
+----------------+---------------------+-----------+--------------+--------------------------+
| table or index | index or table name | file id   | root page id | the CREATE sql statement |
+----------------+---------------------+-----------+--------------+--------------------------+
```

For tables the root page is assumed to be 0.

For indexes, root is the page_id of the btree root page. Index name is formatted as `<table>.<field>`.

So why we store the file_id? When resolving pages we use a uint32, where the first 16 bits are
the file_id, and the last 16 bits are the page_id.

<!--
Good ideas but not planned:

- To automatically create a Hash index for every @unique column.
-->

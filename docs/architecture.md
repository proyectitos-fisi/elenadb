# Elena architecture

## Storage

A database directory is just a bunch of .table and .index file.

```text
mydb.elena
├── doctor.table
├── doctor.name.index
├── doctor.id.index
├── usuario.id.table
└── usuario.table
```

As you can see, a table can have multiple indexes. An index is created over a field (column) with
the name `<table>.<field>.index`.

### How tables are stored

A .table file is NO MORE than a bunch of pages.

- The first page is the **header page**, which contains metadata about the table.
- The rest of the pages are **data pages** structured as slotted pages.

We are **not**:

- Using a BTree structure here. That's for indexes.
- Using a page directory. A page directory keeps track of pages metadata and its metadata whitin a table.
  We are using a simple approach: Our pages are contiguous and we assume the last page is the one
  that contains the most recent data, and may have free space.

#### Header page

```text
??? What kind of metadata we should store here ???
```

#### Data page

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

```text
-------------------------------------------------------------------
|
-------------------------------------------------------------------
```

### Meta table

The meta table is a builtin table

<!--
Good ideas but not planned:

- To automatically create a Hash index for every @unique column.
-->

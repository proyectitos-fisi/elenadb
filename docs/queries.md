# Elenadb queries

## Table queries

Types supported are: `int`, `float`, `char(n)`, `bool`, `fkey(table.column)`

Annotations supported: @id @unique

```elenaql
creame tabla usuario {
    id   int @id,
    age  int,
    code char(12) @unique,
} pe
```

```elenaql
creame tabla doctor {
    id            int               @id,
    id_user       fkey(usuario.id)?,
    document_type char(4),
    document_num  char(10),
    salary        float,
    inactive      bool,
} pe
```

## Table retrieval queries

```elenaql
dame todo de elena_meta pe

dame {todo, todo} de elena_meta pe

dame {id, salary} de doctor donde (salary>200 y inactive != falso) pe
```

## Creation queries

- [ ] Support trailing comma

```elenaql
mete {
    id_user: 10,
    document_type: "DNI",
    document_number: "72016572"
} en doctor pe
```

- [ ] Retornando (nice to have)

```elenaql
mete {
    document_type: "DNI",
    document_number: "72016572"
} en doctor retornando { id } pe
```

## Deletion

- [ ] Implement table deletion `borra de doctor`
- [ ] Implement index deletion `borra indice <index> pe`

```elenaql
borra de doctor donde (inactive=verdad) pe

borra tabla <tabla> pe
borra indice <tabla.indice> pe
```

## Table update

```elenaql
cambia en users {
  nombre: "otro nombre",
} si (id=10) pe
```

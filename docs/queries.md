# Elenadb queries

## Table creation queries

```elenaql
creame tabla usuario {
    id     int       @id @incremental pe
    nombre char(255) pe
} pe
```

```elenaql
creame tabla doctor {
    id            int               @id @incremental pe
    id_user       fkey(usuario.id)?                  pe
    document_type char(4)                            pe
    document_num  char(10)                           pe
    salary        float                              pe
    inactive      bool                               pe
} pe
```

## Table retrieval queries

```elenaql
dame todo de doctor pe
dame { id, salary } de doctor donde (salary>200 y inactive =! falso) pe
dame todo de doctor pe
dame { id, salary } de doctor donde (salary>200 y inactive =! falso) pe
```

## Register creation queries

### Compund queries

```elenaql
let pedro = dame { id } de usuario donde (nombre=="pedro") pe
mete {
    id_user: pedro.id,
    document_type: 'DNI',
    document_number: '72016572',
} en doctor pe
```

## Table deletion

```elenaql
borra de doctor donde (inactive=verdad) pe
```

## Table update

```elenaql
cambia en users {
  nombre: "otro nombre",
} si (id=10) pe
```


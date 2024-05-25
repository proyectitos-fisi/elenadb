# Elenadb queries

```elenaql
creame tabla usuario {
    id     int       @id @incremental pe
    nombre char(255) pe
} pe

creame tabla doctor {
    id            int           @id @incremental pe
    id_user       fkey(user.id)?                 pe
    document_type char(4)                        pe
    document_num  char(10)                       pe
    salary        float                          pe
    inactive      bool                           pe
} pe

dame todo de doctor pe
dame { id, salary } de doctor donde (salary>200 y inactive =! falso) pe
borra de doctor donde (inactive=verdad) pe;
```

## compound queries

```elenaql
let usuario = dame { id } de usuario donde (nombre=="pedro") pe

mete {
    id_user: usuario, document_type: 'DNI', document_number: '72016572',
} en doctor;

-> expected 'pe' got ';'
```

```elenaql
dame todo de doctor pe
dame { id, salary } de doctor donde (salary>200 y inactive =! falso) pe
```

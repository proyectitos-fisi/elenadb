![elena](https://github.com/proyectitos-fisi/elenadb/assets/153166342/cf0dac2e-5602-4ee5-b28c-3b0de0d46b17)

## 🚂 Try elena

```bash
go mod tidy
go run ./cmd/elenadb test.elena

elena>
```

## The modules

- The CLI (paolo) ✅ done, see [docs/cli.md](./docs/cli.md):
  - Design the ElenaDB CLI
  - Initially, a DB will be a directory where each file beign a table in the user system, unlike
    sqlite that uses a single file.
  - The ElenaCLI must support giving a query
  - E.g:
    - elenadb --db ./user_dir --query "creame tabla users"
    - EXPECTED 'pe' got EOF
    - elenadb --create ./user_dir
    - elanadb --version
  - Document the results in docs/queries.md
  - Allow reading queries from file

- Query language (rodro):
  - Define the operations
  - Design the language and document sample queries (See [docs/queries.md](./docs/queries.md))
  - First, design (in code) how the parsed AST looks like, so we can mock that result
    and use it on other parts of the code.
  - Then, start making the actual parser from raw strings.

- The storage (eduardo + damaris)
  - B+tree implementation ✅
  - Design how the binary layout of the 'table' data structure looks like (eduardo)
  - Start thinking on how to apply operations to the db

<!-- (!) Internal note: add your algorithms/data structures here -->

## Algorithms used

- [x] LRU-K eviction policies. A page replacement policy meant to solve the problems that LRU has,
  such as sequential flooding.
  <https://en.wikipedia.org/wiki/Page_replacement_algorithm#Variants_on_LRU>

- [ ] Disk scheduling. Used to optimize and prioritize disk accesss.
  <https://en.wikipedia.org/wiki/I/O_scheduling>

## Data structures used

- [ ] Bloom filters. A probabilistic data structure to test whether an element is on a set.
  Used to implement fast Hash Table misses.
  <https://en.wikipedia.org/wiki/Bloom_filter>

- [ ] Hash Tables. Used in Hash Joins operations.

- [ ] Concurrent queue. Thread safe queue implementation.

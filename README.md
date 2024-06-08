![Blue and Yellow Modern Artisan Parties and Celebrations X-Frame Banner (2)](https://github.com/proyectitos-fisi/elenadb/assets/153166342/d2b8bed7-f8b1-4d73-aff8-4dad1ec8f8ea)

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

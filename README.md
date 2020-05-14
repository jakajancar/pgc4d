# pgc4d - PostgreSQL client for Deno

<a href="https://github.com/jakajancar/pgc4d/actions?query=workflow%3Aci">
    <img src="https://github.com/jakajancar/pgc4d/workflows/ci/badge.svg" alt="ci">
</a>

A full-featured PostgreSQL client for [Deno](https://deno.land) including support for:

  - Connectivity using TCP, SSL* and Unix domain sockets*
  - Buffered and streaming responses
  - Various shapes of result sets (keyed rows, indexed rows, column, etc.)
  - Asynchronous notifications (using LISTEN and NOTIFY)
  - Arrays, record types and user-defined types (enums)
  - Concurrent queries (queueing)

(* currently requires `--unstable` in Deno)

## Usage

```ts
import { connectPg } from 'https://deno.land/x/pgc4d/src/mod.ts'

const db = await connectPg('postgres://username:password@hostname/database')
try {
    const result = await db.query('SELECT $1::int + $2::int sum', [10, 20])
    assertEquals(result.rows[0].sum, 30)
} finally {
    db.close()
}
```

## Documentation

Please see the [wiki](https://github.com/jakajancar/pgc4d/wiki).

## Contributing

Happy to accept fixes and improvements.

 1. Please add tests for added functionality and ensure CI passes.
 2. Follow the prevalent coding style (no semicolons, no 80-char line limit, single quotes, etc.)

You can use Docker to bring up PostgreSQL and run the tests:

    $ docker-compose run ci

## License

Licensed under the [MIT license](LICENSE).

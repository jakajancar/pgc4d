# pgc4d - PostgreSQL client for Deno

<a href="https://github.com/jakajancar/pgc4d/releases">
    <img alt="release" src="https://badgen.net/github/release/jakajancar/pgc4d/stable">
</a>

<a href="https://github.com/jakajancar/pgc4d/actions?query=workflow%3Aci">
    <img alt="ci" src="https://badgen.net/github/checks/jakajancar/pgc4d?label=ci&icon=github">
</a>

<a href="https://doc.deno.land/https/raw.githubusercontent.com/jakajancar/pgc4d/master/src/mod.ts">
    <img alt="deno doc" src="https://badgen.net/badge/doc/deno/557AAC">
</a>

<a href="https://github.com/jakajancar/pgc4d/blob/master/LICENSE">
    <img alt="license" src="https://badgen.net/github/license/jakajancar/pgc4d">
</a>

A full-featured PostgreSQL client for [Deno](https://deno.land) including support for:

  - Connectivity using TCP, SSL* and Unix domain sockets*
  - Buffered and streaming responses
  - Various shapes of result sets (keyed rows, indexed rows, column, etc.)
  - Asynchronous notifications (using LISTEN and NOTIFY)
  - Arrays, record types and user-defined types (enums)
  - Concurrent queries (queueing)

Philosophical differences from [deno-postgres](https://github.com/deno-postgres/deno-postgres):

  - Does not aim for API compatibility with Node's `node-postgres`
  - Uses only binary value encoding in client <> server communication
  - No magic detection of data types (is `[1, 2, 3]` an `int[]` or a `json` value?) - see [bugs](https://github.com/brianc/node-postgres/issues/442)
  - Strict layering of protocol and logic layers - see [the interface](src/message_types.ts)

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

[API documentation](https://doc.deno.land/https/raw.githubusercontent.com/jakajancar/pgc4d/master/src/mod.ts)

[Manual](https://github.com/jakajancar/pgc4d/wiki)

## Contributing

Happy to accept fixes and improvements.

 1. Please add tests for added functionality and ensure CI passes.
 2. Follow the prevalent coding style (no semicolons, no 80-char line limit, single quotes, etc.)

You can use Docker to bring up PostgreSQL and run the tests:

    $ docker-compose run ci

## License

Licensed under the [MIT license](LICENSE).

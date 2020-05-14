import { parseDsn } from '../src/connect_options.ts'
import { assertEquals } from '../src/deps.ts'

const { test } = Deno

test('dsn parsing', () => {
    assertEquals(parseDsn('postgres://user:pass@host:1234/db?sslmode=disable&sslrootcert=path/too/foo.key&application_name=my%20app'), {
        username: 'user',
        password: 'pass',
        hostname: 'host',
        port: 1234,
        database: 'db',
        sslMode: 'disable',
        certFile: 'path/too/foo.key',
        connectionParams: {
            application_name: 'my app'
        }
    })
})

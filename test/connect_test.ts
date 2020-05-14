const { test } = Deno
import { connectPg, PgConn } from '../src/connection.ts'
import { PgError } from '../src/types.ts'
import { testOptions, connMethodTestOptions, authMethodTestOptions } from './env.ts'
import { assertEquals, assertStrContains, unreachable, assert } from '../src/deps.ts'

for (const [method, options] of Object.entries(connMethodTestOptions)) {
    test(`connection method ${method}`, async () => {
        const db = await connectPg(options)
        db.close()
    })
}

for (const [method, options] of Object.entries(authMethodTestOptions)) {
    test(`auth method ${method}`, async () => {
        const db = await connectPg(options)
        db.close()
    })
}

test('connect using dsn', async () => {
    const { username, password, hostname, port, database, sslMode } = testOptions
    const db = await connectPg(`postgres://${username}:${password}@${hostname}:${port}/${database}?sslmode=${sslMode || 'verify-full'}`)
    db.close()
})

test('connection error', async () => {
    try {
        const db = await connectPg({ ...testOptions, port: 1234 })
        unreachable()
    } catch (e) {
        assert(e instanceof Deno.errors.ConnectionRefused)
    }
})

test('wrong password', async () => {
    try {
        const db = await connectPg({ ...testOptions, password: 'wrong' })
        unreachable()
    } catch (e) {
        assert(e instanceof PgError)
        assertEquals(e.message, `password authentication failed for user "pgc4d"`)
        assertStrContains(e.stack!, 'connect_test.ts')
    }
})

test('setting connection params', async () => {
    const db = await connectPg({ ...testOptions, connectionParams: { application_name: 'myapp 123' } })
    try {
        assertEquals(db.serverParams.get('application_name'), 'myapp 123')
    } finally {
        db.close()
    }
})

test('connection promise fulfilled only after startup', async () => {
    const db = await connectPg(testOptions)
    try {
        assert(db.pid !== undefined)
    } finally {
        await db.close()
    }
})

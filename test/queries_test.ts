const { test } = Deno
import { assertEquals, assertStringContains, unreachable, assert } from '../src/deps.ts'
import { PgError } from '../src/types.ts'
import { ConnectPgOptions } from '../src/connect_options.ts'
import { connectPg, PgConn } from '../src/connection.ts'
import { testOptions } from './env.ts'

test('query works in the simplest case', async () => {
    await withConnection(testOptions, async db => {
        const result = await db.query('SELECT 42')
        assertEquals(result.value, 42)
    })
})

test('queries in sequence', async () => {
    await withConnection(testOptions, async db => {
        const result1 = await db.query('SELECT 1')
        assertEquals(result1.value, 1)

        const result2 = await db.query('SELECT 2')
        assertEquals(result2.value, 2)

        const result3 = await db.query('SELECT 3')
        assertEquals(result3.value, 3)
    })
})

test('queries in parallel', async () => {
    await withConnection(testOptions, async db => {
        const promise1 = db.query('SELECT 1')
        const promise2 = db.query('SELECT 2')
        const promise3 = db.query('SELECT 3')
        const [result1, result2, result3] = await Promise.all([promise1, promise2, promise3])
        assertEquals(result1.value, 1)
        assertEquals(result2.value, 2)
        assertEquals(result3.value, 3)
    })
})

test('query rejects informatively', async () => {
    await withConnection(testOptions, async db => {
        try {
            await db.query('SELEKT 42')
            unreachable()
        } catch (e) {
            // console.log('test case caught: ', e)
            assert(e instanceof PgError)
            assertStringContains(e.message, 'syntax error', 'should type')
            assertStringContains(e.message, 'SELEKT', 'should contain relevant keyword')
            assertStringContains(e.stack!, 'queries_test.ts', 'should contain caller')
        }
    })
})

test('query after failed query (server-side error)', async () => {
    await withConnection(testOptions, async db => {
        try {
            await db.query('SELEKT 42')
            unreachable()
        } catch (e) {
            assert(e instanceof PgError)
            assertStringContains(e.message, 'SELEKT')
        }

        const result = await db.query('SELECT 42')
        assertEquals(result.value, 42)
    })
})

test('query after failed query (client-side error)', async () => {
    await withConnection(testOptions, async db => {
        try {
            await db.query('SELECT $1::int', ['not a number'])
            unreachable()
        } catch (e) {
            assert(e instanceof Error)
            assertStringContains(e.message, 'Error sending param $1: Expected number, got string')
        }

        const result = await db.query('SELECT 42')
        assertEquals(result.value, 42)
    })
})

test('streaming query', async () => {
    await withConnection(testOptions, async db => {
        const result = await db.queryStreaming('SELECT generate_series(1,100)')
        let sum = 0
        let numRows = 0
        for await (const value of result.columnIterator) {
            sum += value
            numRows += 1
        }
        assertEquals(sum, (1+100)/2*100)
        assertEquals(numRows, 100)
    })
})

test('streaming with parse error', async () => {
    await withConnection(testOptions, async db => {
        // should reject StreamingQueryResult promise, not only later when reading
        try {
            await db.queryStreaming('SELEKT')
            unreachable()
        } catch (e) {
            assert(e instanceof PgError)
        }
    })
})

test('aborted streaming query', async () => {
    await withConnection(testOptions, async db => {
        const result = await db.queryStreaming('SELECT generate_series(1,100)')
        let sum = 0
        let numRows = 0
        for await (const value of result.columnIterator) {
            sum += value
            numRows += 1

            // abort after 10 out of 100 rows have been read
            if (numRows == 10)
                break
        }
        assertEquals(sum, (1+10)/2*10)
        assertEquals(numRows, 10)

        // A simple query after it
        const result2 = await db.query('SELECT 42')
        assertEquals(result2.value, 42)
    })
})

test('prepared statements', async () => {
    await withConnection(testOptions, async db => {
        const stmt = await db.prepare('SELECT $1 + 100')
        const result1 = (await stmt.execute([1])).value
        const result2 = (await stmt.execute([2])).value
        const result3 = (await stmt.execute([3])).value
        assertEquals(result1, 101)
        assertEquals(result2, 102)
        assertEquals(result3, 103)
        await stmt.close()
    })
})

test('queries reject correctly when connection closed', async () => {
    const db1 = await connectPg(testOptions)
    const blockedQuery = db1.query('SELECT 1') // blocked on write, pre-results
    const queuedQuery = db1.query('SELECT 2') // queued behind
    db1.close()
    const afterCloseQuery = db1.query('SELECT 3')
    const [blockedQueryOutcome, queuedQueryOutcome, afterCloseQueryOutcome] = await Promise.allSettled([blockedQuery, queuedQuery, afterCloseQuery])
    assertRejectedCorrectly(blockedQueryOutcome)
    assertRejectedCorrectly(queuedQueryOutcome)
    assertRejectedCorrectly(afterCloseQueryOutcome)

    const db2 = await connectPg(testOptions)
    const startedQuery = await db2.queryStreaming('SELECT 1')
    db2.close()
    const nextRows = startedQuery.buffer()
    const [nextRowsOutcome] = await Promise.allSettled([nextRows])
    assertRejectedCorrectly(nextRowsOutcome)

    function assertRejectedCorrectly(outcome: any) {
        assertEquals(outcome.status, 'rejected')
        assertEquals(outcome.reason.message, 'Connection closed before query finished.')
    }
})

test('numAffectedRows works for buffered queries', async () => {
    await withConnection(testOptions, async db => {
        const result = await db.query('SELECT 42')
        assertEquals(result.value, 42)
        assertEquals(result.completed, true)
        assertEquals(result.completionInfo.numAffectedRows, 1)
    })
})

test('numAffectedRows works for streaming queries', async () => {
    await withConnection(testOptions, async db => {
        const result = await db.queryStreaming('SELECT 42')
        assertEquals(result.completed, false)
        assertEquals(result.completionInfo, undefined)

        for await (const _ of result.rowsIterator) {
            // ...
        }

        assert(result.completed)
        assert(result.completionInfo)
        assertEquals(result.completionInfo.numAffectedRows, 1)
    })
})

async function withConnection<T>(options: ConnectPgOptions, f: (db: PgConn) => T): Promise<T> {
    const db = await connectPg(options)
    try {
        const ret = await f(db)

        // Check for leaked prepared statements
        const leaks = (await db.query(`SELECT name, statement FROM pg_prepared_statements`)).rows
        assertEquals(leaks, [])

        return ret
    } finally {
        await db.close()
    }
}

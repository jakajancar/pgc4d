import { connectPg, PgConn } from '../src/connection.ts'
import { testOptions } from './env.ts'
import { assertEquals, unreachable, encode } from '../src/deps.ts'
import { ColumnValue } from '../src/types.ts'
const { test } = Deno

testType('bool',        'true',                     true)
testType('bool',        'false',                    false)
testType('bool[]',      '{t,f}',                    [true, false])
testType('bytea',       '\\x68656c6c6f',            encode('hello'))
testType('bytea[]',     '{"\\\\x68656c6c6f"}',      [encode('hello')])
testType('float4',      '-1.5',                     -1.5)
testType('float4[]',    '{-1.5}',                   [-1.5])
testType('float8',      '-1.234',                   -1.234)
testType('float8[]',    '{-1.234}',                 [-1.234])
testType('int2',        '-32768',                   -32768)
testType('int2',        '32767',                    32767)
testType('int2[]',      '{32767}',                  [32767])
testType('int4',        '-2147483648',              -2147483648)
testType('int4',        '2147483647',               2147483647)
testType('int4[]',      '{1,2,3}',                  [1,2,3])
testType('int4[]',      '{{1,2,3},{4,5,6}}',        [[1,2,3],[4,5,6]])
testType('int4[]',      '{{1,2,3},{4,NULL,6}}',     [[1,2,3],[4,null,6]])
testType('int4[]',      '{}',                       [])
testType('int8',        '-9223372036854775808',     -9223372036854775808n)
testType('int8',        '9223372036854775807',      9223372036854775807n)
testType('int8[]',      '{9223372036854775807}',    [9223372036854775807n])
testType('json',        '{}',                       {})
testType('json',        '{"foo":"bar"}',            {foo: 'bar'})
testType('json',        '[]',                       [])
testType('json',        '[1,2,3]',                  [1,2,3])
testType('json',        '"hello"',                  'hello')
testType('json',        '123',                      123)
testType('json',        'true',                     true)
testType('json',        null,                       null) // 'null'::json is not representable
testType('jsonb',       '{}',                       {})
testType('jsonb',       '{"foo": "bar"}',           {foo: 'bar'})
testType('jsonb',       '[]',                       [])
testType('jsonb',       '[1, 2, 3]',                [1,2,3])
testType('jsonb',       '"hello"',                  'hello')
testType('jsonb',       '123',                      123)
testType('jsonb',       'true',                     true)
testType('jsonb',       null,                       null) // 'null'::json is not representable
testType('name',        'hello',                    'hello')
testType('oid',         '1',                        1)
testType('text',        null,                       null)
testType('text',        'hello',                    'hello')
testType('text[]',      '{hello,there}',            ['hello', 'there'])
testType('varchar',     'hello',                    'hello')
testType('varchar[]',   '{hello,there}',            ['hello', 'there'])
testType('char',        'h',                        'h')
testType('char(5)[]',   '{hello,there}',            ['hello', 'there'])
testType('timestamp',   '2009-02-13 23:31:30',      new Date(1234567890*1000))
testType('timestamp[]', '{"2009-02-13 23:31:30"}',  [new Date(1234567890*1000)])
testType('timestamptz', '2009-02-13 23:31:30+00',   new Date(1234567890*1000))
testType('date'       , '0001-01-01',               '0001-01-01')
testType('date'       , '2020-06-21',               '2020-06-21')
testType('date'       , '9999-12-31',               '9999-12-31')
testType('void',        '',                         undefined)
testType('myenum',      'value1',                   'value1', createEnumType)
testType('mydomain',    '42',                       42, createDomain)
testType('mytable',     '(42,hello,t,)',            [42, 'hello', true, null], createTable)

function testType(name: string, pgTextValue: string | null, jsValue: ColumnValue, init?: (db:PgConn) => Promise<void>) {
    test(`sending and receiving ${name}: ${pgTextValue}`, async () => {
        const db = await connectPg(testOptions)
        try {
            if (init)
                await init(db)
            const row = (await db.query(`
                SELECT
                    $1::${name} native_to_native,
                    $1::${name}::text native_to_text,
                    $2::text::${name} text_to_native
                `,
                [jsValue, pgTextValue]
            )).indexedRow
            assertEquals(row, [jsValue, pgTextValue, jsValue]);
        } finally {
            db.close()
        }
    })
}

async function createEnumType(db: PgConn) {
    await db.query(`DROP TYPE IF EXISTS myenum`)
    await db.query(`CREATE TYPE myenum AS ENUM ('value1', 'value2', 'value3')`)
    await db.reloadTypes()
}

async function createDomain(db: PgConn) {
    await db.query(`DROP DOMAIN IF EXISTS mydomain`)
    await db.query(`CREATE DOMAIN mydomain AS integer CHECK (VALUE % 2 = 0)`)
    await db.reloadTypes()
}

async function createTable(db: PgConn) {
    await db.query(`DROP TABLE IF EXISTS mytable`)
    await db.query(`CREATE TABLE mytable (i int4, t text, b boolean, n text)`)
    await db.reloadTypes()
}

test(`sending an unsupported type`, async () => {
    const db = await connectPg(testOptions)
    try {
        await db.query(`SELECT $1::regconfig`, ['never used'])
        unreachable()
    } catch (e) {
        assertEquals(e.message, 'Error sending param $1: Unsupported type: regconfig (oid 3734, typsend regconfigsend)')
    } finally {
        db.close()
    }
})

test(`sending and receiving bpchars of incorrect lengths`, async () => {
    const db = await connectPg(testOptions)
    try {
        assertEquals((await db.query(`SELECT 'too long'::char(5)`, [])).value, 'too l')
        assertEquals((await db.query(`SELECT $1::char(5)`, ['too long'])).value, 'too l')

        assertEquals((await db.query(`SELECT 'shrt'::char(5)`, [])).value, 'shrt ')
        assertEquals((await db.query(`SELECT $1::char(5)`, ['shrt'])).value, 'shrt ')
    } finally {
        db.close()
    }
})

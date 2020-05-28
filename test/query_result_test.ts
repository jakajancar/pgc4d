const { test } = Deno
import { BufferedQueryResult } from '../src/query_result.ts'
import { ColumnMetadata } from '../src/types.ts'
import { assertEquals, assertThrows } from '../src/deps.ts'

const c1: ColumnMetadata = { name: 'c1', tableOid: 0, typeOid: 23, typeSize: 0, typeMod: 0 }
const c2: ColumnMetadata = { name: 'c2', tableOid: 0, typeOid: 23, typeSize: 0, typeMod: 0 }
const c3: ColumnMetadata = { name: 'c3', tableOid: 0, typeOid: 23, typeSize: 0, typeMod: 0 }

test('buffered result accessors work when they should', async () => {
    assertEquals(new BufferedQueryResult([c1,c2,c3], [[1,2,3],[4,5,6]], {}).indexedRows, [[1,2,3],[4,5,6]])
    assertEquals(new BufferedQueryResult([c1,c2,c3], [[1,2,3]], {}).indexedRow, [1,2,3])
    assertEquals(new BufferedQueryResult([c1,c2,c3], [[1,2,3]], {}).maybeIndexedRow, [1,2,3])
    assertEquals(new BufferedQueryResult([c1,c2,c3], [], {}).maybeIndexedRow, undefined)

    assertEquals(new BufferedQueryResult([c1,c2,c3], [[1,2,3],[4,5,6]], {}).rows, [{c1: 1, c2: 2, c3: 3}, {c1: 4, c2: 5, c3: 6}])
    assertEquals(new BufferedQueryResult([c1,c2,c3], [[1,2,3]], {}).row, {c1: 1, c2: 2, c3: 3})
    assertEquals(new BufferedQueryResult([c1,c2,c3], [[1,2,3]], {}).maybeRow, {c1: 1, c2: 2, c3: 3})
    assertEquals(new BufferedQueryResult([c1,c2,c3], [], {}).maybeRow, undefined)

    assertEquals(new BufferedQueryResult([c1], [[1],[4]], {}).column, [1,4])
    assertEquals(new BufferedQueryResult([c1], [[1]], {}).value, 1)
    assertEquals(new BufferedQueryResult([c1], [[1]], {}).maybeValue, 1)
    assertEquals(new BufferedQueryResult([c1], [], {}).maybeValue, undefined)
})

test('buffered result accessors throw when number of rows is incorrect', async () => {
    assertThrows(() => new BufferedQueryResult([c1,c2,c3], [[1,2,3],[4,5,6]], {}).indexedRow, undefined, 'Expected result to have 1-1 rows, got 2.')
    assertThrows(() => new BufferedQueryResult([c1,c2,c3], [], {}).indexedRow, undefined, 'Expected result to have 1-1 rows, got 0.')
    assertThrows(() => new BufferedQueryResult([c1,c2,c3], [[1,2,3],[4,5,6]], {}).maybeIndexedRow, undefined, 'Expected result to have 0-1 rows, got 2.')
    
    assertThrows(() => new BufferedQueryResult([c1,c2,c3], [[1,2,3],[4,5,6]], {}).row, undefined, 'Expected result to have 1-1 rows, got 2.')
    assertThrows(() => new BufferedQueryResult([c1,c2,c3], [], {}).row, undefined, 'Expected result to have 1-1 rows, got 0.')
    assertThrows(() => new BufferedQueryResult([c1,c2,c3], [[1,2,3],[4,5,6]], {}).maybeRow, undefined, 'Expected result to have 0-1 rows, got 2.')

    assertThrows(() => new BufferedQueryResult([c1], [[1],[4]], {}).value, undefined, 'Expected result to have 1-1 rows, got 2.')
    assertThrows(() => new BufferedQueryResult([c1], [], {}).value, undefined, 'Expected result to have 1-1 rows, got 0.')
    assertThrows(() => new BufferedQueryResult([c1], [[1],[4]], {}).maybeValue, undefined, 'Expected result to have 0-1 rows, got 2.')
})

test('buffered result accessors throw when number of columns is incorrect', async () => {
    assertThrows(() => new BufferedQueryResult([], [[],[]], {}).column, undefined, 'Expected result to have 1 column, got 0')
    assertThrows(() => new BufferedQueryResult([], [[]], {}).value, undefined, 'Expected result to have 1 column, got 0')
    assertThrows(() => new BufferedQueryResult([], [[]], {}).maybeValue, undefined, 'Expected result to have 1 column, got 0')
    assertThrows(() => new BufferedQueryResult([], [], {}).maybeValue, undefined, 'Expected result to have 1 column, got 0')
    
    assertThrows(() => new BufferedQueryResult([c1,c2], [[1,2],[4,4]], {}).column, undefined, 'Expected result to have 1 column, got 2')
    assertThrows(() => new BufferedQueryResult([c1,c2], [[1,2]], {}).value, undefined, 'Expected result to have 1 column, got 2')
    assertThrows(() => new BufferedQueryResult([c1,c2], [[1,2]], {}).maybeValue, undefined, 'Expected result to have 1 column, got 2')
    assertThrows(() => new BufferedQueryResult([c1,c2], [], {}).maybeValue, undefined, 'Expected result to have 1 column, got 2')
})

test('keyed row accessors throw when columns are ambiguous', async () => {
    const expectedMsg = `Cannot returned keyed rows because result columns are not uniquely named.`
    assertThrows(() => new BufferedQueryResult([c1,c1,c1], [[1,2,3],[4,5,6]], {}).rows, undefined, expectedMsg)
    assertThrows(() => new BufferedQueryResult([c1,c1,c1], [[1,2,3]], {}).row, undefined, expectedMsg)
    assertThrows(() => new BufferedQueryResult([c1,c1,c1], [[1,2,3]], {}).maybeRow, undefined, expectedMsg)
    assertThrows(() => new BufferedQueryResult([c1,c1,c1], [], {}).maybeRow, undefined, expectedMsg)
})

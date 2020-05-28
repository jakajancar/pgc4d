import { assert } from './deps.ts'
import { ColumnMetadata, ColumnValue, IndexedRow, KeyedRow } from './types.ts'

/** Query metadata received at the end of execution of a query. */
export interface CompletionInfo {
    /**
     * For an INSERT command, rows is the number of rows inserted.
     * For a DELETE command, rows is the number of rows deleted.
     * For an UPDATE command, rows is the number of rows updated.
     * For a SELECT or CREATE TABLE AS command, rows is the number of rows retrieved.
     * For a MOVE command, rows is the number of rows the cursor's position has been changed by.
     * For a FETCH command, rows is the number of rows that have been retrieved from the cursor.
     * For a COPY command, rows is the number of rows copied.
     * Undefined otherwise.
     */
    numAffectedRows?: number
}

/** Abstract superclass of streaming and buffered results. */
export abstract class QueryResult {
    constructor(
        public readonly columns: ColumnMetadata[]
    ) {}

    abstract readonly indexedRowsIterator: AsyncIterableIterator<IndexedRow>
    abstract completionInfo: CompletionInfo | undefined

    get rowsIterator(): AsyncIterableIterator<KeyedRow> {
        this._assertUniqueColumnNames()
        return mapAsyncIterator(this.indexedRowsIterator, this._toKeyedRow.bind(this))
    }

    get columnIterator(): AsyncIterableIterator<ColumnValue> {
        this._assertSingleColumn()
        return mapAsyncIterator(this.indexedRowsIterator, this._toField.bind(this))
    }

    get completed(): boolean {
        return !!this.completionInfo
    }

    protected _assertUniqueColumnNames() {
        if (new Set(this.columns.map(c => c.name)).size != this.columns.length)
            throw new Error(`Cannot returned keyed rows because result columns are not uniquely named.`)
    }

    protected _toKeyedRow(src: IndexedRow): KeyedRow {
        assert(src.length === this.columns.length)
        const ret: KeyedRow = {}
        this.columns.forEach((column, index) => {
            ret[column.name] = src[index]
        })
        return ret
    }

    protected _assertSingleColumn() {
        if (this.columns.length !== 1)
            throw new Error(`Expected result to have 1 column, got ${this.columns.length}.`)
    }

    protected _toField(src: IndexedRow): ColumnValue {
        assert(src.length === this.columns.length)
        return src[0]
    }
}

/**
 * A streaming result.
 * 
 * To free up the connection, an iterator should be retrieved from the
 * response object and either `next()` called enough times to reach
 * `done: true` or `return()`/`throw()` called to cancel the query.
 * 
 * The `for-await-of` loop handles this for you automatically.
 */
export class StreamingQueryResult extends QueryResult {
    private _unconsumed?: AsyncGenerator<IndexedRow, CompletionInfo>
    completionInfo: CompletionInfo | undefined

    constructor(columns: ColumnMetadata[], stream: AsyncGenerator<IndexedRow, CompletionInfo>) {
        super(columns)
        this._unconsumed = stream
    }
    
    get indexedRowsIterator(): AsyncIterableIterator<IndexedRow> {
        if (!this._unconsumed)
            throw new Error('You can only iterate over a streaming result once.')
        const stream = this._unconsumed
        this._unconsumed = undefined
        return (async function * (this: StreamingQueryResult) {
            this.completionInfo = yield* stream
        }).call(this)
    }

    /** Reads the result to completion and returns a `BufferedQueryResult`. */
    async buffer(): Promise<BufferedQueryResult> {
        const buffer: IndexedRow[] = []
        for await (const row of this.indexedRowsIterator) {
            buffer.push(row)
        }
        assert(this.completionInfo)
        return new BufferedQueryResult(this.columns, buffer, this.completionInfo)
    }
}

/**
 * A buffered query result that contains all the rows and `CompletionInfo`.
 */
export class BufferedQueryResult extends QueryResult {
    readonly indexedRows: IndexedRow[]
    readonly completionInfo: CompletionInfo

    constructor(columns: ColumnMetadata[], indexedRows: IndexedRow[], completionInfo: CompletionInfo) {
        super(columns)
        this.indexedRows = indexedRows
        this.completionInfo = completionInfo
    }

    get indexedRowsIterator(): AsyncIterableIterator<IndexedRow> {
        return arrayToAsyncIterator(this.indexedRows)
    }

    private _assertNumRows(min: number, max: number) {
        const len = this.indexedRows.length
        if (len < min || len > max)
            throw new Error(`Expected result to have ${min}-${max} rows, got ${len}.`)
    }

    get indexedRow(): IndexedRow                    { this._assertNumRows(1, 1); return this.indexedRows[0] }
    get maybeIndexedRow(): IndexedRow | undefined   { this._assertNumRows(0, 1); return this.indexedRows[0] }

    get rows(): KeyedRow[]                          { this._assertUniqueColumnNames(); return this.indexedRows.map(this._toKeyedRow.bind(this)) }
    get row(): KeyedRow                             { this._assertUniqueColumnNames(); return this._toKeyedRow(this.indexedRow) }
    get maybeRow(): KeyedRow | undefined            { this._assertUniqueColumnNames(); return this.maybeIndexedRow ? this._toKeyedRow(this.maybeIndexedRow) : undefined}

    get column(): ColumnValue[]                     { this._assertSingleColumn(); return this.indexedRows.map(this._toField.bind(this)) }
    get value(): ColumnValue                        { this._assertSingleColumn(); return this._toField(this.indexedRow) }
    get maybeValue(): ColumnValue | undefined       { this._assertSingleColumn(); return this.maybeIndexedRow ? this._toField(this.maybeIndexedRow) : undefined}
}

async function* arrayToAsyncIterator<T>(xs: T[]): AsyncIterableIterator<T> {
    for (const x of xs)
        yield x
}

function mapAsyncIterator<T, R>(iterator: AsyncIterator<T>, mapper: (value: T) => R): AsyncIterableIterator<R> {
    async function mapResult(resultPromise: Promise<IteratorResult<T, undefined>>): Promise<IteratorResult<R, undefined>> {
        const result = await resultPromise
        if (result.done) {
            assert(result.value === undefined)
            return { done: true, value: undefined }
        } else {
            return { done: false, value: mapper(result.value) }
        }
    }

    const ret: AsyncIterableIterator<R> = {
        next:                     (...args) => mapResult(iterator.next(...args)),
        return: iterator.return ? (...args) => mapResult(iterator.return!(...args)) : undefined,
        throw:  iterator.throw  ? (...args) => mapResult(iterator.throw!(...args)) : undefined,
        [Symbol.asyncIterator]: () => ret
    }
    return ret
}

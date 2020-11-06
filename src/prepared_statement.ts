import { Format } from './message_types.ts'
import { PgConnImpl, Lock } from './connection.ts'
import { ParameterMetadata, ColumnMetadata, ColumnValue, IndexedRow } from './types.ts'
import { assert } from './deps.ts'
import { StreamingQueryResult, BufferedQueryResult, CompletionInfo } from './query_result.ts'

export interface PreparedStatement {
    /** Executes the prepared statement and returns a buffered result once all the rows are received. */
    execute(params?: ColumnValue[]): Promise<BufferedQueryResult>

    /** Executes the prepared statement and returns a streaming result as soon as the query
     * has been accepted by the server. Rows will be retrieved as you consume them. */
    executeStreaming(params?: ColumnValue[]): Promise<StreamingQueryResult>

    /** Release the prepared statement on the server. */
    close(): Promise<void>
}

export class PreparedStatementImpl {
    constructor(
        private readonly _db: PgConnImpl,
        private readonly _name: string,
        public readonly params: ParameterMetadata[],
        public readonly columns: ColumnMetadata[]
    ) {}

    async execute(params: ColumnValue[] = []): Promise<BufferedQueryResult> {
        return (await this.executeStreaming(params)).buffer()
    }

    async executeStreaming(params: ColumnValue[] = []): Promise<StreamingQueryResult> {
        const lock = await this._db._locks.read()
        return await this._executeStreamingConsumingExistingLock(lock, params)
    }

    // takes ownership of conn, does not wait for turn
    async _executeStreamingConsumingExistingLock(lock: Lock, params: ColumnValue[] = []): Promise<StreamingQueryResult> {
        let paramValues
        try {
            paramValues = this._serializeParams(params)
        } catch (e) {
            lock.release()
            throw e
        }
        await lock.write([
            { type: 'Bind', dstPortal: '', srcStatement: this._name, paramFormats: [Format.Binary], paramValues, resultFormats: [Format.Binary] },
            { type: 'Execute', portal: '', maxRows: 0 },
            { type: 'Sync' },
        ])
        await lock.read(['BindComplete'])
        return new StreamingQueryResult(this.columns, this._createRowsIteratorFromResponse(lock))
    }

    private async * _createRowsIteratorFromResponse(lock: Lock): AsyncGenerator<IndexedRow, CompletionInfo> {
        let completed = false
        let errored = false
        try {
            while (true) {
                const msg = await lock.read(['DataRow', 'CommandComplete'])
                switch (msg.type) {
                    case 'DataRow':
                        yield this._parseRow(msg.values)
                        break
                    case 'CommandComplete':
                        completed = true
                        const tagParts = msg.tag.split(' ')
                        let numAffectedRows
                        switch (tagParts[0]) {
                            case 'INSERT':
                                return { numAffectedRows: parseInt(tagParts[2], 10) } as CompletionInfo
                            case 'DELETE':
                            case 'UPDATE':
                            case 'SELECT':
                            case 'MOVE':
                            case 'FETCH':
                            case 'COPY':
                                return { numAffectedRows: parseInt(tagParts[1], 10) } as CompletionInfo
                            default:
                                return {}
                        }
                }
            }
        } catch (err) {
            errored = true
            throw err
        } finally {
            if (!errored) {
                if (!completed) {
                    // drain so next query can run
                    while (true) {
                        const msg = await lock.read(['DataRow', 'CommandComplete'])
                        if (msg.type === 'CommandComplete')
                            break
                    }
                }
                await lock.read(['ReadyForQuery'])
                lock.release()
            }
        }
    }

    async close(): Promise<void> {
        const lock = await this._db._locks.read()
        await lock.write([
            { type: 'Close', what: 'statement', name: this._name },
            { type: 'Sync'},
        ])
        await lock.read(['CloseComplete'])
        await lock.read(['ReadyForQuery'])
        lock.release()
    }

    private _serializeParams(values: ColumnValue[]): Array<Uint8Array|null> {
        if (values.length !== this.params.length)
            throw new Error(`Statement requires ${this.params.length} params, ${values.length} passed.`)

        return values.map((value, i) => {
            try {
                if (value === null)
                    return null
                return this._db._typeRegistry.send(this.params[i].typeOid, value)
            } catch (e) {
                e.message = `Error sending param $${i+1}: ${e.message}`
                throw e
            }
        })
    }

    private _parseRow(values: Array<Uint8Array|null>): IndexedRow {
        assert(values.length === this.columns.length)
        return values.map((value, i) => {
            try {
                if (value === null)
                    return null
                return this._db._typeRegistry.recv(this.columns[i].typeOid, value)
            } catch (e) {
                e.message = `Error receiving column $${i+1}: ${e.message}`
                throw e
            }
        })
    }
}

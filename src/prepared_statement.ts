import { Format } from './message_types.ts'
import { PgConnImpl, assertType } from './connection.ts'
import { ParameterMetadata, ColumnMetadata, ColumnValue, IndexedRow } from './types.ts'
import { assert, unreachable, assertEquals } from './deps.ts'
import { StreamingQueryResult, BufferedQueryResult, CompletionInfo } from './query_result.ts'

export class PreparedStatement {
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
        await this._db._turns.read()
        return await this._executeStreamingWithoutWaitingForTurn(params)
    }

    // takes ownership of conn, does not wait for turn
    async _executeStreamingWithoutWaitingForTurn(params: ColumnValue[] = []): Promise<StreamingQueryResult> {
        const paramValues = this._serializeParams(params)
        await this._db._write([
            { type: 'Bind', dstPortal: '', srcStatement: this._name, paramFormats: [Format.Binary], paramValues, resultFormats: [Format.Binary] },
            { type: 'Execute', portal: '', maxRows: 0 },
            { type: 'Sync' },
        ])
        assertType(await this._db._readSync(), ['BindComplete'])
        return new StreamingQueryResult(this.columns, this._createRowsIteratorFromResponse())
    }

    private async * _createRowsIteratorFromResponse(): AsyncGenerator<IndexedRow, CompletionInfo> {
        let completed = false
        try {
            while (true) {
                const msg = await this._db._readSync()
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
        } finally {
            if (!completed) {
                // drain so next query can run
                while (this._db.done.pending) {
                    const msg = await this._db._readSync()
                    if (msg.type === 'CommandComplete')
                        break
                }
            }

        }
    }

    async close(): Promise<void> {
        await this._db._turns.read()
        await this._db._write([
            { type: 'Close', what: 'statement', name: this._name },
            { type: 'Sync'},
        ])
        assertType(await this._db._readSync(), ['CloseComplete'])
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

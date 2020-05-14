import { ServerMessage, ClientMessage } from './message_types.ts'
import { readMessage, writeMessage } from './message_serde.ts'
import { assert, assertEquals, BufReader, BufWriter } from './deps.ts'
import { Deferred, Pipe, hashMd5Password } from './utils.ts'
import { PreparedStatement } from './prepared_statement.ts'
import { StreamingQueryResult, BufferedQueryResult } from './query_result.ts'
import { ConnectPgOptions, computeOptions } from './connect_options.ts'
import { PgError } from './types.ts'
import { TypeRegistry } from './data_type_registry.ts'

export interface PgConn extends Deno.Closer {
    readonly pid: number
    readonly serverParams: Map<string, string>
    
    /** Resolved when connection is closed, with Error if due to a problem, or
     *  undefined if due to close() being called. Never rejects. */
    readonly done: Promise<Error | undefined>

    query(text: string, params?: any[]): Promise<BufferedQueryResult>
    queryStreaming(text: string, params?: any[]): Promise<StreamingQueryResult>
    prepare(text: string): Promise<PreparedStatement>
    
    reloadTypes(): Promise<void>

    /** Closes immediately, killing any queries in progress. They will reject.
     *  Not an issue if called multiple times. Subsequent calls will have no effect. */
    close(): void
}

export async function connectPg(url: string, options?: ConnectPgOptions): Promise<PgConn>
export async function connectPg(options: ConnectPgOptions): Promise<PgConn>
export async function connectPg(...args: any[]): Promise<PgConn>
{
    let effectiveOptions
    if (typeof args[0] === 'string') {
        effectiveOptions = computeOptions(args[0], args[1])
    } else if (args[0] instanceof Object) {
        effectiveOptions = computeOptions(undefined, args[0])
    } else {
        throw new Error('Invalid arguments passed to connectPg(). Expecting connectPg(url, options?) or connectPg(options).')
    }
    const { transport, hostname, port, path, sslMode, certFile } = effectiveOptions
    let conn
    switch (transport) {
        case 'tcp':
            conn = await Deno.connect({ transport, hostname, port })
            if (sslMode !== 'disable')
                conn = await startTlsPostgres(conn, { hostname, certFile })
            break
        case 'unix':
            conn = await Deno.connect({ transport, path } as any) // unix not yet stable
            break
        default:
            throw new Error(`Unsupported transport: ${transport}`)
    }
    const pgConn = new PgConnImpl(effectiveOptions, conn)
    await pgConn._started
    return pgConn
}

const CLOSED_BEFORE_FINISHED_TEXT = 'Connection closed before query finished.'

export class PgConnImpl implements PgConn {
    constructor(
        readonly _options: ReturnType<typeof computeOptions>,
        readonly _conn: Deno.Conn
    ) {
        this.done.finally(() => {
            // Reject any queries
            for (const promise of this._turns.reads)
                promise.reject(new Error(CLOSED_BEFORE_FINISHED_TEXT))
            for (const promise of this._syncMessage.reads)
                promise.reject(new Error(CLOSED_BEFORE_FINISHED_TEXT))

            // Closed if not already closed
            try { this._conn.close() } catch {}
        })
        this._runReadLoop()
        this._started = this._start()
    }
    
    readonly serverParams = new Map<string, string>()
    readonly _writer: BufWriter = new BufWriter(this._conn)
    readonly _reader: BufReader = new BufReader(this._conn)
    readonly _syncMessage = new Pipe<ServerMessage>()
    readonly _turns = new Pipe<void>()
    readonly _firstReadyForQuery = new Deferred<void>()
    readonly _typeRegistry = new TypeRegistry(this)
    readonly _started: Promise<void>
    pid!: number         // set after _started
    _secretKey!: number  // set after _started
    _stmtCounter: number = 0
    readonly done = new Deferred<Error | undefined>()
    
    // Write a message
    async _write(msgs: ClientMessage[]): Promise<void>
    {
        try {
            for (const msg of msgs) {
                if (this._options.debug)
                    console.debug('pg < ' + JSON.stringify(msg))
                await writeMessage(this._writer, msg)
            }
            await this._writer.flush()
        } catch (e) {
            if (e instanceof Deno.errors.BadResource && this.done.settled)
                throw new Error(CLOSED_BEFORE_FINISHED_TEXT)
            throw e
        }
    }

    // Read a message
    // (only the read loop should use this directly, you probably should use `_readSync()`)
    async _read(): Promise<ServerMessage> {
        const msg = await readMessage(this._reader)
        if (this._options.debug)
            console.debug('pg > ' + JSON.stringify(msg))
        return msg
    }

    // Keep reading messages, handling async ones, pushing sync ones to this._syncMessage
    async _runReadLoop() {
        try {
            while (this.done.pending) {
                const msg = await this._read()
                switch (msg.type) {
                    case 'ParameterStatus':
                        this.serverParams.set(msg.name, msg.value)
                        break
                    case 'NoticeResponse':
                        await this._options.onNotice(msg.notice)
                        break
                    case 'NotificationResponse':
                        await this._options.onNotification({channel: msg.channel, payload: msg.payload, sender: msg.sender})
                        break
                    case 'BackendKeyData':
                        this.pid = msg.pid
                        this._secretKey = msg.secretKey
                        break
                    case 'ReadyForQuery':
                        this._firstReadyForQuery.resolve()
                        this._turns.write()
                        break
                    default:
                        if (msg.type === 'ErrorResponse' && (msg.error.severity === 'FATAL' || msg.error.severity === 'PANIC')) {
                            this.done.resolve(new PgError(msg.error))
                        }

                        // await, to buffer at most one
                        await this._syncMessage.write(msg)
                }
            }
        } catch (e) {
            this._firstReadyForQuery.reject(e)
            this.done.resolve(e)
        }
    }

    // Read a *non-async* message. Throws on error.
    async _readSync(): Promise<ServerMessage> {
        const msg = await this._syncMessage.read()
        if (msg.type === 'ErrorResponse') {
            if (msg.error.severity !== 'FATAL' && msg.error.severity !== 'PANIC')
                await this._write([{ type: 'Sync' }])
            throw new PgError(msg.error)
        }
        return msg
    }

    async _start() {
        const params = new Map(Object.entries({
            user: this._options.username,
            database: this._options.database,
            ...this._options.connectionParams
        }).filter(([_, v]) => v !== undefined)) as Map<string,string>
        await this._write([{ type: 'StartupMessage', params}])

        authentication:
        while (true) {
            const msg = await this._readSync()
            switch (msg.type) {
                case 'AuthenticationCleartextPassword': {
                    if (!this._options.password)
                        throw new Error('Password required but not provided.')
                    await this._write([{ type: 'PasswordMessage', password: this._options.password }])
                    break
                }
                case 'AuthenticationMD5Password': {
                    if (!this._options.password)
                        throw new Error('Password required but not provided.')
                    const hash = hashMd5Password(this._options.password, this._options.username, msg.salt)
                    await this._write([{ type: 'PasswordMessage', password: hash }])
                    break
                }
                case 'AuthenticationOk':
                    break authentication
                default:
                    throw new Error(`Unexpected message: ${msg.type}`)
            }
        }

        await this._firstReadyForQuery
        assert(this.pid !== undefined)
        assert(this._secretKey !== undefined)
        assert(this.serverParams.get('integer_datetimes') === 'on')
        assert(this.serverParams.get('client_encoding') === 'UTF8')

        await this.reloadTypes()
    }
    
    async reloadTypes() {
        await this._typeRegistry.reload()
    }

    async _prepare(name: string, text: string, flushCommand: 'Sync' | 'Flush') {
        await this._write([
            { type: 'Parse', dstStatement: name, query: text, paramTypes: [] },
            { type: 'Describe', what: 'statement', name },
            { type: flushCommand },
        ])
        assertType(await this._readSync(), ['ParseComplete'])

        const paramDesc = assertType(await this._readSync(), ['ParameterDescription'])
        const params = paramDesc.typeOids.map(typeOid => ({ typeOid }))

        const rowDescOrNoData = assertType(await this._readSync(), ['RowDescription', 'NoData'])
        assert(rowDescOrNoData.type === 'RowDescription' || rowDescOrNoData.type === 'NoData')
        const columns = rowDescOrNoData.type === 'RowDescription' ? rowDescOrNoData.fields : []
    
        return new PreparedStatement(this, name, params, columns)
    }

    async prepare(text: string) {
        await this._turns.read()
        return this._prepare(`pgc4d_${this._stmtCounter++}`, text, 'Sync')
    }

    async queryStreaming(text: string, params: any[] = []) {
        await this._turns.read()
        const stmt = await this._prepare('', text, 'Flush')
        return await stmt._executeStreamingWithoutWaitingForTurn(params)
    }
    
    async query(text: string, params?: any[]) {
        return (await this.queryStreaming(text, params)).buffer()
    }

    close() {
        this.done.resolve()
    }

}

/** Reads a message of a specific type of a response stream */
export function assertType<M extends ServerMessage, T extends M['type']>(
    msg: ServerMessage,
    types: T[]
): M extends { type: T } ? M : never {
    assert((types as string[]).includes(msg.type), `Expected ${types}, got ${msg.type}`)
    return msg as any
}

async function startTlsPostgres(conn: Deno.Conn, options: { hostname: string, certFile?: string }): Promise<Deno.Conn> {
    const w = BufWriter.create(conn)
    await writeMessage(w, { type: 'SSLRequest' })
    await w.flush()

    const response = new Uint8Array(1)
    assertEquals(await conn.read(response), 1)
    switch (String.fromCharCode(response[0])) {
        case 'S':
            return await (Deno as any).startTls(conn, options)
            break
        case 'N':
            throw new Error(`Server does not allow SSL connections. Set sslMode to 'disable' to disable SSL.`)
        default:
            throw new Error(`Unexpected response to SSLRequest: ${response}`)
    }
}

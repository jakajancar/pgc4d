import { ServerMessage, ClientMessage } from './message_types.ts'
import { readMessage, writeMessage } from './message_serde.ts'
import { assert, assertEquals, BufReader, BufWriter } from './deps.ts'
import { Deferred, Pipe, hashMd5Password } from './utils.ts'
import { PreparedStatement, PreparedStatementImpl } from './prepared_statement.ts'
import { StreamingQueryResult, BufferedQueryResult } from './query_result.ts'
import { ConnectPgOptions, computeOptions } from './connect_options.ts'
import { PgError, ColumnValue, NotificationListener } from './types.ts'
import { TypeRegistry } from './data_type_registry.ts'

export interface PgConn extends Deno.Closer {
    /** Process ID of the server process attached to the current session.
     * Same as the number returned by `pg_backend_pid()` function using SQL.*/
    readonly pid: number

    /** The current setting of server parameters such as `client_encoding`
     * or `DateStyle`. */
    readonly serverParams: Map<string, string>

    /** Resolved when connection is closed, with Error if due to a problem, or
     *  undefined if due to close() being called. Never rejects. */
    readonly done: Promise<Error | undefined>

    /** Executes a query and returns a buffered result once all the rows are received. */
    query(text: string, params?: ColumnValue[]): Promise<BufferedQueryResult>

    /** Executes a query and returns a streaming result as soon as the query
     * has been accepted by the server. Rows will be retrieved as you consume them. */
    queryStreaming(text: string, params?: ColumnValue[]): Promise<StreamingQueryResult>

    /** Creates a prepared statement on the server which you can later execute
     * several times using different parameter values. Should offer improved
     * performance compared to executing completely independent queries.*/
    prepare(text: string): Promise<PreparedStatement>

    /** Adds a listener for a channel. If this is the first listener for the channel,
     * issues a `LISTEN` query against the database. Returned promise resolves after
     * the connection is confirmed to be subscribed. */
    addListener(channel: string, listener: NotificationListener): Promise<void>

    /** Removes a listener for a channel. Listener is removed immediately and will not
     * receive any further events. If this is the last listener for the channel, issues
     * an `UNLISTEN` query against the database. Returned promise resolves after
     * the connection is unsubscribed if the last listener is being removed, immediately
     * otherwise. */
    removeListener(channel: string, listener: NotificationListener): Promise<void>

    /** pgc4d loads the `pg_type` table to obtain the definitions of user-defined types.
     * You can call `reloadTypes()` after doing e.g. `CREATE TYPE ... AS ENUM` to
     * have the type recognized without re-connecting. */
    reloadTypes(): Promise<void>

    /** Closes immediately, killing any queries in progress. They will reject.
     *  Not an issue if called multiple times. Subsequent calls will have no effect. */
    close(): void
}

/**
 * Opens a new connection to a PostgreSQL server and resolves to the connection
 * (`PgConn`) once authenticated and ready to accept queries.
 *
 * Usage:
 *
 * ```ts
 * const db1 = await connectPg('postgres://username:password@hostname/database', { ... more opts ... });
 * const db2 = await connectPg({ hostname, username, password, database });
 * const db3 = await connectPg({ transport: 'unix', path: '/foo/bar.sock', username });
 * ```
 *
 * Requirements:
 *   - tcp with ssl (default) requires `--allow-net` and `--unstable`
 *   - tcp without ssl requires `--allow-net`
 *   - unix requires `--allow-read` and `--unstable`
 */
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

export class Lock {
    private clean = true
    constructor(private readonly conn: PgConnImpl) {}

    async write(msgs: ClientMessage[], sync: boolean = false): Promise<void> {
        this.clean = false
        try {
            for (const msg of msgs) {
                if (this.conn._options.debug)
                    console.debug('pg < ' + JSON.stringify(msg))
                await writeMessage(this.conn._writer, msg)
            }
            await this.conn._writer.flush()
        } catch (e) {
            if (e instanceof Deno.errors.BadResource)
                throw new Error(CLOSED_BEFORE_FINISHED_TEXT)
            throw e
        }
    }

    /** If an error is received, an exception will be thrown and the lock released. */
    async read<M extends ServerMessage, T extends M['type']>(types: T[]): Promise<M extends { type: T } ? M : never> {
        if (this.conn.done.settled)
            throw new Error(CLOSED_BEFORE_FINISHED_TEXT)
        const msg = await this.conn._syncMessage.read()
        if (msg.type === 'ErrorResponse') {
            if (msg.fields.severity !== 'FATAL' && msg.fields.severity !== 'PANIC') {
                await this.read(['ReadyForQuery'])
                this.release()
            }
            throw new PgError(msg.fields)
        } else {
            this.clean = msg.type === 'ReadyForQuery'
            assert((types as string[]).includes(msg.type), `Expected ${types}, got ${msg.type}`)
            return msg as any
        }
    }

    release(): void {
        assert(this.clean, 'Releasing lock while connection is not in a clean state.')
        this.conn._locks.write(this)
    }
}

export class PgConnImpl implements PgConn {
    constructor(
        readonly _options: ReturnType<typeof computeOptions>,
        readonly _conn: Deno.Conn
    ) {
        this.done.finally(() => {
            // Reject any queries
            for (const promise of this._locks.reads)
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
    readonly _locks = new Pipe<Lock>()
    readonly _typeRegistry = new TypeRegistry(this)
    readonly _started: Promise<void>
    pid!: number         // set after _started
    _secretKey!: number  // set after _started
    _stmtCounter: number = 0
    readonly _channels = new Map<string, {
        listeners: Set<NotificationListener>,
        subscribed: Deferred<void>
    }>()

    readonly done = new Deferred<Error | undefined>()

    // Keep reading messages, handling async ones, pushing sync ones to this._syncMessage
    async _runReadLoop() {
        try {
            while (this.done.pending) {
                const msg = await readMessage(this._reader)
                if (this._options.debug)
                    console.debug('pg > ' + JSON.stringify(msg))
                switch (msg.type) {
                    case 'ParameterStatus':
                        this.serverParams.set(msg.name, msg.value)
                        break
                    case 'NoticeResponse':
                        await this._options.onNotice(msg.fields)
                        break
                    case 'NotificationResponse': {
                        const channel = this._channels.get(msg.channel)
                        if (!channel)
                            break // unsubscribed before receiving notification
                        if (!channel.subscribed.settled)
                            break // notification from previous subscription, discard, there could be gaps
                        const promises: Array<Promise<void>> = []
                        for (const listener of channel.listeners) {
                            promises.push(listener({ channel: msg.channel, payload: msg.payload, sender: msg.sender }))
                        }
                        // await, for pushback
                        await Promise.all(promises)
                        break
                    }
                    default:
                        if (msg.type === 'ErrorResponse' && (msg.fields.severity === 'FATAL' || msg.fields.severity === 'PANIC')) {
                            this.done.resolve(new PgError(msg.fields))
                        }
                        // await, to buffer at most one
                        await this._syncMessage.write(msg)
                }
            }
        } catch (e) {
            this.done.resolve(e)
        }
    }

    async _start() {
        // We create the (single) lock that is then passed around in this._locks
        const lock = new Lock(this)

        const params = new Map(Object.entries({
            user: this._options.username,
            database: this._options.database,
            ...this._options.connectionParams
        }).filter(([_, v]) => v !== undefined)) as Map<string,string>
        await lock.write([{ type: 'StartupMessage', params}])

        authentication:
        while (true) {
            const msg = await lock.read(['AuthenticationCleartextPassword', 'AuthenticationMD5Password', 'AuthenticationOk'])
            switch (msg.type) {
                case 'AuthenticationCleartextPassword': {
                    if (!this._options.password)
                        throw new Error('Password required but not provided.')
                    await lock.write([{ type: 'PasswordMessage', password: this._options.password }])
                    break
                }
                case 'AuthenticationMD5Password': {
                    if (!this._options.password)
                        throw new Error('Password required but not provided.')
                    const hash = hashMd5Password(this._options.password, this._options.username, msg.salt)
                    await lock.write([{ type: 'PasswordMessage', password: hash }])
                    break
                }
                case 'AuthenticationOk':
                    break authentication
            }
        }

        const bkd = await lock.read(['BackendKeyData'])
        this.pid = bkd.pid
        this._secretKey = bkd.secretKey

        await lock.read(['ReadyForQuery'])
        assert(this.serverParams.get('integer_datetimes') === 'on')
        assert(this.serverParams.get('client_encoding') === 'UTF8')
        lock.release()

        await this.reloadTypes()
    }

    async reloadTypes() {
        await this._typeRegistry.reload()
    }

    async _prepare(lock: Lock, name: string, text: string) {
        await lock.write([
            { type: 'Parse', dstStatement: name, query: text, paramTypes: [] },
            { type: 'Describe', what: 'statement', name },
            { type: 'Sync' },
        ])
        await lock.read(['ParseComplete'])

        const paramDesc = await lock.read(['ParameterDescription'])
        const params = paramDesc.typeOids.map(typeOid => ({ typeOid }))

        const rowDescOrNoData = await lock.read(['RowDescription', 'NoData'])
        const columns = rowDescOrNoData.type === 'RowDescription' ? rowDescOrNoData.fields : []

        await lock.read(['ReadyForQuery'])
        return new PreparedStatementImpl(this, name, params, columns)
    }

    async prepare(text: string) {
        const lock = await this._locks.read()
        const stmt = await this._prepare(lock, `pgc4d_${this._stmtCounter++}`, text)
        lock.release()
        return stmt
    }

    async queryStreaming(text: string, params: ColumnValue[] = []) {
        const lock = await this._locks.read()
        const stmt = await this._prepare(lock, '', text)
        return await stmt._executeStreamingConsumingExistingLock(lock, params)
    }

    async query(text: string, params?: ColumnValue[]) {
        return (await this.queryStreaming(text, params)).buffer()
    }

    async addListener(channel: string, listener: NotificationListener): Promise<void> {
        const existing = this._channels.get(channel)
        if (!existing) {
            assert(channel.match(/^[^\\"]+$/), 'Unsupported channel name') // todo: encoding
            const subscribed = new Deferred<void>()
            this._channels.set(channel, { listeners: new Set([listener]), subscribed })
            await this.query(`LISTEN "${channel}"`)
            subscribed.resolve()
        } else {
            assert(!existing.listeners.has(listener), 'Listener already added for channel.')
            existing.listeners.add(listener)
            return existing.subscribed
        }
    }

    async removeListener(channel: string, listener: NotificationListener): Promise<void> {
        const existing = this._channels.get(channel)
        assert(existing && existing.listeners.delete(listener), 'Listener not added for channel.')
        if (existing.listeners.size === 0) {
            assert(this._channels.delete(channel))
            await this.query(`UNLISTEN "${channel}"`)
        }
    }

    close() {
        this.done.resolve()
    }

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

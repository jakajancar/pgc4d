import { Notification, PgNotice } from './types.ts'
import { assert } from './deps.ts'

export interface ConnectPgOptions {
    /** Transport to use. Use 'tcp' for both unencrypted TCP as well as TLS ("SSL").
     * Default: `tcp` */
    transport?: 'tcp' | 'unix',

    /** A literal IP address or host name that can be resolved to an IP address.
     * Default: `127.0.0.1`. */
    hostname?: string

    /** When using transport `tcp`, the port to connect to.
     * Default: 5432. */
    port?: number

    /** When using transport `unix`, the filesystem path to the socket. */
    path?: string,
    
    /** Username to use when connecting. Required. */
    username?: string

    /** Password to authenticate with. Required unless `trust` method is enabled server-side. */
    password?: string

    /** Defaults to username. */
    database?: string

    /** Only applies to tcp transport. Defaults to 'verify-full'. See:
     * https://www.postgresql.org/docs/12/libpq-ssl.html#LIBPQ-SSL-SSLMODE-STATEMENTS
     */
    sslMode?: 'disable' | 'verify-full'

    /** Path to the CA certificate to verify server's certificate against. */
    certFile?: string

    /** Arbitrary connection parameters to send to the server, for example
     * `application_name`. */
    connectionParams?: { [name: string]: string | undefined }

    /** Notice received from server.
     *  Default implementation writes to `console.log`. */
    onNotice?: (notice: PgNotice) => Promise<void>

    /** Notification received from server as a result of LISTEN.
     *  Default implementation writes a reminder using `console.warn`. */
    onNotification?: (n: Notification) => Promise<void>

    /** Log debugging messages using `console.log`.
     *  Default: false. */
    debug?: boolean
}

function isPropertyDefined<T, K extends keyof T>(options: T, propertyName: K): options is Exclude<T, K> & Required<Pick<T, K>> {
    return options[propertyName] !== undefined
}

export function computeOptions(url: string | undefined, options: ConnectPgOptions) {
    const defaultOptions = {
        transport: 'tcp' as 'tcp',
        hostname: '127.0.0.1',
        port: 5432,
        sslMode: 'verify-full' as 'verify-full',
        connectionParams: {},
        onNotice: async (notice: PgNotice) => {
            console.log(`${notice.severity}: ${notice.message}`)
        },
        onNotification: async () => {
            console.warn('Received notification, but no handler. Please pass `onNotification` option to `connectPg()`.')
        },
        debug: false
    }
    const urlOptions = url ? parseDsn(url) : {}
    const effectiveOptions = { ...defaultOptions, ...urlOptions, ...options }
    assert(isPropertyDefined(effectiveOptions, 'username'), 'Username must be provided via `username` option or url.')
    
    return effectiveOptions
}

// See 33.1.1.2. Connection URIs in:
// https://www.postgresql.org/docs/12/libpq-connect.html#LIBPQ-CONNSTRING
export function parseDsn(dsn: string): ConnectPgOptions {
    let url
    if (dsn.startsWith('postgres://'))
        url = new URL('http' + dsn.slice('postgres'.length))
    else if (dsn.startsWith('postgresql://'))
        url = new URL('http' + dsn.slice('postgresql'.length))
    else
        throw new Error('Invalid DSN (must start with postgres:// or postgresql://): ' + dsn)
  
    return {
        username: url.username || undefined,
        password: url.password || undefined,
        hostname: url.hostname || undefined,
        port:     url.port ? parseInt(url.port, 10) : undefined,
        database: url.pathname.slice(1) || undefined,
        sslMode:  url.searchParams.get('sslmode') as any || undefined,
        certFile: url.searchParams.get('sslrootcert') || undefined,
        connectionParams: {
            application_name: url.searchParams.get('application_name') || undefined
        }
    }
}

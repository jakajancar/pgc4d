import { ConnectPgOptions, computeOptions} from '../src/connect_options.ts'

// Deno.startTls() and Deno.connect() with transport 'unix' currently require --unstable.
const skipUnstable = Deno.env.get('SKIP_UNSTABLE') == '1'

// If running on Mac and db is in Docker, cannot test Unix domain sockets
const skipUnix = Deno.env.get('SKIP_UNIX') == '1'

const certFile = new URL('rootCA.pem', import.meta.url).pathname

/**
 * To run tests, you need to have a PostgreSQL instance at postgres:5433,
 * configured with SSL and a bunch of predefined users to test authentication.
 *
 * You can prepare it yourself, or you can use Docker and the 'postgres'
 * service defined in `docker-compose.yml`. Either way, you need to use
 * /etc/hosts to point to the IP, unless you're running the tests within
 * Docker as well.
 *
 * For the exact list of requirements for the test database, see
 * `docker/postgres/*`, particularly `pg_hba.conf`.
 */
export const testOptions: ConnectPgOptions = {
    hostname: 'postgres', port: 5433, sslMode: 'disable', database: 'pgc4d', username: 'pgc4d', password: 'pgc4d_pass',
}

// Wait until server is ready to prevent tests failing because test instance
// did not yet start, or a bunch of failures due to server being unavailable.
for (let attempt=1; ; attempt++) {
    try {
        const { hostname, port } = computeOptions(undefined, testOptions)
        console.log(`Waiting for ${hostname}:${port} ...`)
        const conn = await Deno.connect({ hostname, port })
        conn.close()
        break
    } catch (e) {
        console.log(' `-> ' + e.message)
        if (attempt === 10) {
            console.error('Test database not avaialble.')
            Deno.exit(1)
        } else {
            await new Promise(resolve => setTimeout(resolve, 1000))
        }
    }
}

export const connMethodTestOptions: Record<string, ConnectPgOptions> = {
    local:     { transport: 'unix', path: '/socket/.s.PGSQL.5433',     database: 'pgc4d', username: 'pgc4d_local',       password: 'pgc4d_local_pass' },
    hostssl:   { hostname: 'postgres', port: 5433, certFile,           database: 'pgc4d', username: 'pgc4d_hostssl',     password: 'pgc4d_hostssl_pass' },
    hostnossl: { hostname: 'postgres', port: 5433, sslMode: 'disable', database: 'pgc4d', username: 'pgc4d_hostnossl',   password: 'pgc4d_hostnossl_pass' },
}
if (skipUnstable) {
    delete connMethodTestOptions['local']
    delete connMethodTestOptions['hostssl']
}
if (skipUnix)
    delete connMethodTestOptions['local']

export const authMethodTestOptions: Record<string, ConnectPgOptions> = {
    trust: { hostname: 'postgres', port: 5433, sslMode: 'disable', database: 'pgc4d', username: 'pgc4d_trust',  password: undefined },
    clear: { hostname: 'postgres', port: 5433, sslMode: 'disable', database: 'pgc4d', username: 'pgc4d_clear',  password: 'pgc4d_clear_pass' },
    md5:   { hostname: 'postgres', port: 5433, sslMode: 'disable', database: 'pgc4d', username: 'pgc4d_md5',    password: 'pgc4d_md5_pass' },
    // scram: { hostname: 'postgres', port: 5433, certFile, database: 'pgc4d', username: 'pgc4d_scram',  password: 'pgc4d_scram_pass' },
}

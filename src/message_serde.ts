// https://www.postgresql.org/docs/current/protocol-message-types.html

import { ServerMessage, ClientMessage, TransactionStatus, ErrorAndNoticeFields } from './message_types.ts'
import { BufReader, BufWriter, unreachable, assert, unimplemented, encode, decode } from './deps.ts'

export async function readMessage(reader: BufReader): Promise<ServerMessage> {
    // Read entire message
    const header = await reader.readFull(new Uint8Array(5));
    assert(header, 'Unexpected EOF')
    const msgType = decode(header.slice(0, 1));
    const msgBodyLength = new DataView(header.buffer).getInt32(1) - 4
    const msgBody = await reader.readFull(new Uint8Array(msgBodyLength));
    assert(msgBody, 'Unexpected EOF')
    
    // Utility functions for consuming data from body
    let processed = 0

    const view = new DataView(msgBody.buffer)
    function int16(): number { const v = view.getInt16(processed); processed += 2; return v }
    function int32(): number { const v = view.getInt32(processed); processed += 4; return v }

    function byten(length: number): Uint8Array {
        const v = msgBody!.slice(processed, processed + length)
        processed += length
        return v
    }

    function string(): string {
        const npos = msgBody!.indexOf(0, processed)
        assert(npos !== -1)
        const v = msgBody!.slice(processed, npos)
        processed = npos + 1
        return decode(v)
    }

    function array<T>(f: () => T) {
        return Array.from({length: int16()}).map(f)
    }

    function readErrorOrNotice<T extends string>(expectedSeverities: T[]): ErrorAndNoticeFields & { severity: T } {
        const map = new Map<string, string>()
        let key
        while ((key = decode(byten(1))) !== '\0')
            map.set(key, string())

        const severity = map.get('V')
        if (!isOneOf(severity, expectedSeverities))
            throw new Error('Unexpected severity: ' + severity)

        return {
            severity,
            severityLocal:      map.get('S')!,
            code:               map.get('C')!,
            message:            map.get('M')!,
            detail:             map.get('D'),
            hint:               map.get('H'),
            position:           map.get('P') ? parseInt(map.get('P')!, 10) : undefined,
            internalPosition:   map.get('p') ? parseInt(map.get('p')!, 10) : undefined,
            internalQuery:      map.get('q'),
            where:              map.get('W'),
            schemaName:         map.get('s'),
            tableName:          map.get('t'),
            columnName:         map.get('c'),
            dataTypeName:       map.get('d'),
            constraintName:     map.get('n'),
            file:               map.get('F'),
            line:               map.get('L') ? parseInt(map.get('L')!, 10) : undefined,
            routine:            map.get('R'),
        }
    }

    // Consume body
    function parse(): ServerMessage {
        switch (msgType) {
            case 'R': {
                const authMethod = int32()
                switch (authMethod) {
                    case 0:  return { type: 'AuthenticationOk' }
                    case 3:  return { type: 'AuthenticationCleartextPassword' }
                    case 5:  return { type: 'AuthenticationMD5Password', salt: byten(4) }
                    default: unimplemented('Unsupported auth method: ' + authMethod)
                }
            }
            case 'K': return { type: 'BackendKeyData', pid: int32(), secretKey: int32() }
            case '2': return { type: 'BindComplete' }
            case '3': return { type: 'CloseComplete' }
            case 'C': return { type: 'CommandComplete', tag: string() }
            case 'D':
                return { type: 'DataRow', values: array(() => {
                    const length = int32()
                    if (length === -1)
                        return null
                    else
                        return byten(length)
                }) }
            case 'I': return { type: 'EmptyQueryResponse' }
            case 'E': return { type: 'ErrorResponse', fields: readErrorOrNotice(['ERROR', 'FATAL', 'PANIC']) }
            case 'N': return { type: 'NoticeResponse', fields: readErrorOrNotice(['WARNING', 'NOTICE', 'DEBUG', 'INFO', 'LOG']) }
            case 'n': return { type: 'NoData' }
            case 'A': return { type: 'NotificationResponse', sender: int32(), channel: string(), payload: string() }
            case 't': return { type: 'ParameterDescription', typeOids: array(() => int32()) }
            case 'S': return { type: 'ParameterStatus', name: string(), value: string() }
            case '1': return { type: 'ParseComplete' }
            case 'Z': return { type: 'ReadyForQuery', status: decode(byten(1)) as TransactionStatus }
            case 'T':
                return { type: 'RowDescription', fields: array(() => ({
                    name: string(),
                    tableOid: int32(),
                    column: int16(),
                    typeOid: int32(),
                    typeSize: int16(),
                    typeMod: int32(),
                    format: int16(),
                }))
            }

            default:
                throw new Error('Unsupported message: ' + msgType)
        }
    }
    const message = parse()
    assert(processed === msgBodyLength, `Processed ${processed} bytes, message body has ${msgBodyLength} (message ${msgType})`)
    return message
}

export async function writeMessage(writer: BufWriter, msg: ClientMessage): Promise<void> {
    // Utility functions for writing body
    const bodyParts = new Array<Uint8Array>()

    function int16(n: number) { const x = new Uint8Array(2); (new DataView(x.buffer)).setInt16(0, n); bodyParts.push(x) }
    function int32(n: number) { const x = new Uint8Array(4); (new DataView(x.buffer)).setInt32(0, n); bodyParts.push(x) }

    function byten(bytes: Uint8Array) {
        bodyParts.push(bytes)
    }

    function string(s: string) {
        bodyParts.push(encode(s+'\0'))
    }

    function array<T>(xs: T[], f: (x: T) => void) {
        int16(xs.length)
        xs.map(f)
    }

    // Determine type and body parts
    // (StartupMessage and SSLRequest omit the type byte for BC, hence nullable)
    let type: string | null
    switch (msg.type) {
        case 'Bind': {
            type = 'B'
            string(msg.dstPortal)
            string(msg.srcStatement)
            array(msg.paramFormats, format => { int16(format) })
            array(msg.paramValues, value => {
                if (value === null) {
                    int32(-1)
                } else {
                    int32(value.length)
                    byten(value)
                }
            })
            array(msg.resultFormats, format => { int16(format) })
            break
        }
        case 'Close': {
            type = 'C'
            byten(encode(msg.what === 'statement' ? 'S' : 'P'))
            string(msg.name)
            break
        }
        case 'Describe': {
            type = 'D'
            byten(encode(msg.what === 'statement' ? 'S' : 'P'))
            string(msg.name)
            break
        }
        case 'Execute': {
            type = 'E'
            string(msg.portal)
            int32(msg.maxRows)
            break
        }
        case 'Flush': {
            type = 'H'
            break
        }
        case 'Parse': {
            type = 'P'
            string(msg.dstStatement)
            string(msg.query)
            array(msg.paramTypes, type => { int32(type) })
            break
        }
        case 'PasswordMessage': {
            type = 'p'
            string(msg.password)
            break
        }
        case 'SSLRequest': {
            type = null
            int32(80877103)
            break
        }
        case 'StartupMessage': {
            type = null
            int32(196608)
            for (const [name, value] of msg.params) {
                string(name)
                string(value)
            }
            byten(Uint8Array.of(0))
            break
        }
        case 'Sync': {
            type = 'S'
            break
        }
        case 'Terminate': {
            type = 'X'
            break
        }
    default:
            unreachable()
    }

    // Determine size
    const size = new Uint8Array(4);
    (new DataView(size.buffer)).setInt32(0, bodyParts.reduce((acc, part) => acc + part.length, 0) + size.length)

    // Write message
    if (type !== null)
        await writer.write(encode(type))
    await writer.write(size)
    for (let part of bodyParts)
        await writer.write(part)
}

function isOneOf<T>(x: unknown, allowedValues: T[]): x is T  {
    return allowedValues.includes(x as any)
}
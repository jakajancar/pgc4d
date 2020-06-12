// https://github.com/postgres/postgres/blob/master/src/include/catalog/pg_type.dat
// https://github.com/postgres/postgres/blob/master/src/backend/libpq/pqformat.c

import { encode, decode, assert } from './deps.ts'
import { ColumnValue } from './types.ts'
import { TypeRow, TypeRegistry } from './data_type_registry.ts'
import { arrayDimensions } from './utils.ts'

// This file contains functions named identically as those in PostgreSQL source
// and returned in typreceive/typsend columns in pg_type table.

export type TypeAwareSendFunc = (value: unknown, type: TypeRow, typeRegistry: TypeRegistry) => Uint8Array
export type TypeAwareRecvFunc = (array: Uint8Array, type: TypeRow, typeRegistry: TypeRegistry) => ColumnValue

type SendFunc = (value: unknown) => Uint8Array
type RecvFunc = (array: Uint8Array) => ColumnValue

export const textsend: SendFunc = (value: unknown) => {
    assert(typeof value === 'string', `Expected string, got ${debugValueDesc(value)}`)
    return encode(value)
}

export const textrecv: RecvFunc = (array: Uint8Array) => {
    return decode(array)
}

export const [varcharsend, varcharrecv] = [textsend, textrecv]
export const [bpcharsend, bpcharrecv] = [textsend, textrecv]
export const [namesend, namerecv] = [textsend, textrecv]
export const [enum_send, enum_recv] = [textsend, textrecv]

export const boolsend: SendFunc = (value: unknown) => {
    assert(typeof value === 'boolean', `Expected boolean, got ${debugValueDesc(value)}`)
    return Uint8Array.of(value ? 1 : 0)
}

export const boolrecv: RecvFunc = (array: Uint8Array) => {
    assert(array.length === 1)
    return array[0] === 1
}

function createNumericSendRecv(byteLength: number, dataViewGetter: any, dataViewSetter: any): [SendFunc, RecvFunc] {
    return [
        (value: unknown) => {
            assert(typeof value === 'number', `Expected number, got ${debugValueDesc(value)}`)
            const array = new Uint8Array(byteLength)
            dataViewSetter.call(new DataView(array.buffer), 0, value)
            return array
        },
        (array: Uint8Array) => {
            assert(array.length === byteLength)
            return dataViewGetter.call(new DataView(array.buffer), 0)
        }
    ]
}

export const [int2send, int2recv] = createNumericSendRecv(2, DataView.prototype.getInt16, DataView.prototype.setInt16)
export const [int4send, int4recv] = createNumericSendRecv(4, DataView.prototype.getInt32, DataView.prototype.setInt32)
export const [float4send, float4recv] = createNumericSendRecv(4, DataView.prototype.getFloat32, DataView.prototype.setFloat32)
export const [float8send, float8recv] = createNumericSendRecv(8, DataView.prototype.getFloat64, DataView.prototype.setFloat64)

export const int8send: SendFunc = (value: unknown) => {
    assert(typeof value === 'bigint' || typeof value === 'number', `Expected bigint or number, got ${debugValueDesc(value)}`)
    const array = new Uint8Array(8)
    new DataView(array.buffer).setBigInt64(0, BigInt(value))
    return array
}

export const int8recv: RecvFunc = (array: Uint8Array) => {
    assert(array.length === 8)
    return new DataView(array.buffer).getBigInt64(0)
}

export const [oidsend, oidrecv] = [int4send, int4recv]

export const byteasend: SendFunc = (value: unknown) => {
    assert(value instanceof Uint8Array, `Expected Uint8Array, got ${debugValueDesc(value)}`)
    return value
}

export const bytearecv: RecvFunc = (array: Uint8Array) => {
    return array
}

export const timestamp_send: SendFunc = (value: unknown) =>{
    assert(value instanceof Date, `Expected Date, got ${debugValueDesc(value)}`)
    return int8send((value.getTime() - 946684800000) * 1000)
}

export const timestamp_recv: RecvFunc = (array: Uint8Array) => {
    return new Date(Number(int8recv(array)/1000n) + 946684800000) // 2000 -> 1970
}

export const [timestamptz_send, timestamptz_recv] = [timestamp_send, timestamp_recv]

export const json_send: SendFunc = (value: unknown) => {
    return encode(JSON.stringify(value))
}

export const json_recv: RecvFunc = (array: Uint8Array) => {
    return JSON.parse(decode(array))
}

export const jsonb_send: SendFunc = (value: unknown) =>{
    const serialized = json_send(value)
    const ret = new Uint8Array(serialized.length+1)
    ret[0] = 1
    ret.set(serialized, 1)
    return ret
}

export const jsonb_recv: RecvFunc = (array: Uint8Array) => {
    assert(array[0] === 1)
    return json_recv(array.subarray(1))
}


export const void_send: SendFunc = () => {
    return new Uint8Array(0)
}

export const void_recv: RecvFunc = (array: Uint8Array) => {
    return undefined
}

export const array_send: TypeAwareSendFunc = (value, type, typeRegistry) =>{
    assert(value instanceof Array, `Expected array, got ${debugValueDesc(value)}`)
    const dims = arrayDimensions(value)

    const buffer = new Deno.Buffer()
    buffer.writeSync(int4send(dims.length))
    buffer.writeSync(int4send(1))
    buffer.writeSync(oidsend(type.typelem))
    for (const dim of dims) {
        buffer.writeSync(int4send(dim))
        buffer.writeSync(int4send(1)) // lower bound
    }

    function writeValues(x: unknown): void {
        if (x instanceof Array) {
            for (const elem of x)
                writeValues(elem)
        } else {
            if (x === null) {
                buffer.writeSync(int4send(-1))
            } else {
                const serialized = typeRegistry.send(type.typelem, x)
                buffer.writeSync(int4send(serialized.length))
                buffer.writeSync(serialized)
            }
        }
    }
    writeValues(value)
    return buffer.bytes()
}

export const array_recv: TypeAwareRecvFunc = (array, type, typeRegistry) => {
    const view = new DataView(array.buffer)
    let offset = 0
    const ndim = view.getInt32(offset); offset += 4
    const flags = view.getInt32(offset); offset += 4
    assert(flags === 0 || flags === 1) // has nulls, unused
    const elemTypeOid = view.getInt32(offset); offset += 4

    const dims = new Array<number>()
    for (let i = 0; i < ndim; i++) {
        const length = view.getInt32(offset); offset += 4
        const lowerBound = view.getInt32(offset); offset += 4
        assert(lowerBound === 1, 'Only arrays with lower bound 1 are supported.')
        dims.push(length)
    }

    function readValue(level: number): ColumnValue {
        if (level === 0 && dims.length === 0) {
            return []
        } else if (level === dims.length) {
            const length = view.getInt32(offset); offset += 4
            if (length === -1) {
                return null
            } else {
                const serialized = array.slice(offset, offset+length); offset += length
                return typeRegistry.recv(type.typelem, serialized)
            }
        } else {
            const arr = []
            for (let i = 0; i < dims[level]; i++) {
                const elem = readValue(level + 1)
                arr.push(elem)
            }
            return arr
        }
    }

    const value = readValue(0)
    assert(offset === array.length)
    return value
}

export const record_send: TypeAwareSendFunc = (value, type, typeRegistry) =>{
    assert(value instanceof Array, `Expected array, got ${debugValueDesc(value)}`)
    assert(type.typtype === 'c')
    assert(type.attrtypids.length > 0)
    assert(type.attrtypids.length === value.length, `Composite type '${type.typname}' requires ${type.attrtypids.length} elements, ${value.length} passed.`)

    const buffer = new Deno.Buffer()
    buffer.writeSync(int4send(value.length))
    for (let i=0; i<value.length; i++) {
        try {
            const elemType = type.attrtypids[i]
            const elemValue = value[i]
            buffer.writeSync(oidsend(elemType))
            if (elemValue === null) {
                buffer.writeSync(int4send(-1))
            } else {
                const serialized = typeRegistry.send(elemType, elemValue)
                buffer.writeSync(int4send(serialized.length))
                buffer.writeSync(serialized)
            }
        } catch (e) {
            e.message = `Record field ${i+1}: ${e.message}`
            throw e
        }
    }
    return buffer.bytes()
}

export const record_recv: TypeAwareRecvFunc = (array, type, typeRegistry) => {
    assert(type.typtype === 'c')
    assert(type.attrtypids.length > 0)

    const view = new DataView(array.buffer)
    let offset = 0
    const nelems = view.getInt32(offset); offset += 4
    assert(nelems === type.attrtypids.length)

    const value = new Array<ColumnValue>()
    for (let i=0; i<nelems; i++) {
        try {
            const elemType = view.getInt32(offset); offset += 4
            assert(elemType === type.attrtypids[i])

            const length = view.getInt32(offset); offset += 4
            if (length === -1) {
                value.push(null)
            } else {
                const serialized = array.slice(offset, offset+length); offset += length
                const parsed = typeRegistry.recv(elemType, serialized)
                value.push(parsed)
            }
        } catch (e) {
            e.message = `Record field ${i+1}: ${e.message}`
            throw e
        }
    }
    return value
}

function debugValueDesc(x: unknown) {
    if (x === null) return 'null'
    else if (typeof x === 'object') return `object (constructor: ${x?.constructor?.name})`
    else return typeof x
}

import { ErrorAndNoticeFields } from './message_types.ts'

/** A notification received as a consequence of using `LISTEN`. */
export interface Notification {
    channel: string,
    payload: string,
    /** Process ID of the server process that sent the notifications.
     * Equivalent to `pg_backend_pid()` SQL function on the sender. */
    sender: number
}

export interface PgError extends ErrorAndNoticeFields {
    severity: 'ERROR' | 'FATAL' | 'PANIC'
}

export class PgError extends Error {
    name = 'PgError'
    constructor(fields: ErrorAndNoticeFields & { severity: 'ERROR' | 'FATAL' | 'PANIC' }) {
        super(fields.message)
        Object.assign(this, fields)
    }
}

export interface PgNotice extends ErrorAndNoticeFields {
    severity: 'WARNING' | 'NOTICE' | 'DEBUG' | 'INFO' | 'LOG'
}

export interface ParameterMetadata {
    /** Specifies the object ID of the parameter data type. */
    typeOid: number
}

export interface ColumnMetadata {
    /** The field name. */
    name: string
    /** If the field can be identified as a column of a specific table, the object ID of the table; otherwise zero. */
    tableOid: number
    /** The object ID of the field's data type. */
    typeOid: number
    /** The data type size (see `pg_type.typlen`). Note that negative values denote variable-width types. */
    typeSize: number
    /** The type modifier (see `pg_attribute.atttypmod`). The meaning of the modifier is type-specific. */
    typeMod: number
}

export type ColumnValue = any

/** A row in the shape of a JavaScript array, without column names:
 *
 * ```ts
 * [ 'John', 'Doe', 33 ]
 * ```
 */
export type IndexedRow = ColumnValue[]

/** A row in the shape of a JavaScript object, with column names as keys:
 *
 * ```ts
 * { first_name: 'John', last_name: 'Doe', age: 33 }
 * ```
 */
export type KeyedRow = { [key: string]: ColumnValue }

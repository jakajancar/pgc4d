import { PgErrorFields } from './message_types.ts'

export interface Notification {
    channel: string,
    payload: string,
    sender: number
}

export class PgError extends Error implements PgErrorFields {
    name = 'PgError'
    constructor(fields: PgErrorFields) {
        super(fields.message)
        Object.assign(this, fields)
    }
    severity!: "ERROR" | "FATAL" | "PANIC"
    severityLocal!: string
    code!: string
    message!: string
    detail?: string
    hint?: string
    position?: number
    internalPosition?: number
    internalQuery?: string
    where?: string
    schemaName?: string
    tableName?: string
    columnName?: string
    dataTypeName?: string
    constraintName?: string
    file?: string
    line?: number
    routine?: string
}

export interface ColumnMetadata {
    name: string
    tableOid: number
    typeOid: number
    typeSize: number
    typeMod: number
}

export interface ParameterMetadata {
    typeOid: number
}

export type ColumnValue = any
export type IndexedRow = ColumnValue[]
export type KeyedRow = { [key: string]: ColumnValue }

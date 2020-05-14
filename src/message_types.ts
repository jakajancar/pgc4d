// https://www.postgresql.org/docs/current/protocol-message-formats.html
// https://www.postgresql.org/docs/current/protocol-message-types.html

export type ServerMessage =
    { type: 'AuthenticationOk' } |
    { type: 'AuthenticationCleartextPassword' } |
    { type: 'AuthenticationMD5Password', salt: Uint8Array } |
    { type: 'BackendKeyData', pid: number, secretKey: number } |
    { type: 'BindComplete' } |
    { type: 'CloseComplete' } |
    { type: 'CommandComplete', tag: string } |
    { type: 'DataRow', values: Array<Uint8Array|null> } |
    { type: 'EmptyQueryResponse' } |
    { type: 'ErrorResponse', error: PgErrorFields } |
    { type: 'NoticeResponse', notice: PgNotice } |
    { type: 'NoData' } |
    { type: 'NotificationResponse', sender: number, channel: string, payload: string } |
    { type: 'ParameterDescription', typeOids: number[] } |
    { type: 'ParameterStatus', name: string, value: string } |
    { type: 'ParseComplete' } |
    { type: 'ReadyForQuery', status: TransactionStatus } |
    { type: 'RowDescription', fields: Array<{ name: string, tableOid: number, column: number, typeOid: number, typeSize: number, typeMod: number, format: Format }>}


export type ClientMessage = 
    { type: 'Bind', dstPortal: string, srcStatement: string, paramFormats: Format[], paramValues: Array<Uint8Array|null>, resultFormats: Format[] } |
    { type: 'Close', what: 'statement' | 'portal', name: string } |
    { type: 'Describe', what: 'statement' | 'portal', name: string } |
    { type: 'Execute', portal: string, maxRows: number } |
    { type: 'Flush' } |
    { type: 'Parse', dstStatement: string, query: string, paramTypes: number[] } |
    { type: 'PasswordMessage', password: string } |
    { type: 'SSLRequest' } |
    { type: 'StartupMessage', params: Map<string, string> } |
    { type: 'Sync' } |
    { type: 'Terminate' }

export enum Format {
    Text = 0,
    Binary = 1
}

export enum TransactionStatus {
    Idle = 'I',
    Transaction = 'T',
    Failed = 'F'
}

export interface PgErrorOrNotice {
    severityLocal: string,
    severity: string,
    code: string,
    message: string,
    detail?: string,
    hint?: string,
    position?: number,
    internalPosition?: number,
    internalQuery?: string,
    where?: string,
    schemaName?: string,
    tableName?: string,
    columnName?: string,
    dataTypeName?: string,
    constraintName?: string,
    file?: string,
    line?: number,
    routine?: string,
}

export interface PgErrorFields extends PgErrorOrNotice {
    severity: 'ERROR' | 'FATAL' | 'PANIC'
}

export interface PgNotice extends PgErrorOrNotice {
    severity: 'WARNING' | 'NOTICE' | 'DEBUG' | 'INFO' | 'LOG'
}

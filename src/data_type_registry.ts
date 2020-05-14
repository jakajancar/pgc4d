import { TypeAwareRecvFunc, TypeAwareSendFunc } from './data_type_converters.ts'
import * as converters from './data_type_converters.ts'
import { ColumnValue } from './types.ts'
import { PgConn } from './connection.ts'

/**
 * Information about types, subset of `pg_type` system table:
 * https://www.postgresql.org/docs/12/catalog-pg-type.html
 */
export type TypeRow = {
    oid: number,
    typname: string,
    typtype: string,
    typelem: number,
    typreceive: string, // binary input function
    typsend: string,    // binary output function
    attrtypids: number[]
}

export class TypeRegistry {
    private types!: Map<number, TypeRow>

    constructor(private readonly db: PgConn) {
        // At runtime, we load types from `pg_type` to capture user-defined types.
        // This "bootstrap" types resolve the chicken and the egg problem of needing types to get types.
        this.loadTable([
            { oid: 23,   typname: 'int4',  typtype: 'b', typelem: 0,   typreceive: 'int4recv',   typsend: 'int4send',   attrtypids: [] },
            { oid: 25,   typname: 'text',  typtype: 'b', typelem: 0,   typreceive: 'textrecv',   typsend: 'textsend',   attrtypids: [] },
            { oid: 1007, typname: '_int4', typtype: 'b', typelem: 23,  typreceive: 'array_recv', typsend: 'array_send', attrtypids: [] },
            { oid: 1009, typname: 'text',  typtype: 'b', typelem: 25,  typreceive: 'array_recv', typsend: 'array_send', attrtypids: [] },
        ])
    }

    async reload() {
        const types = (await this.db.query(`
            SELECT
                oid::int4,
                typname::text,
                typtype::text,
                typbasetype::text,
                typdelim::text,
                typelem::int4,
                typinput::text,
                typoutput::text,
                typreceive::text,
                typsend::text,
                array(
                    SELECT atttypid::int4
                    FROM pg_attribute
                    WHERE attrelid = typrelid AND NOT attisdropped AND attnum > 0
                    ORDER BY attnum
                ) attrtypids
            FROM pg_type
            WHERE typisdefined
        `)).rows as TypeRow[]
        this.loadTable(types)
    }

    recv(typeOid: number, array: Uint8Array): ColumnValue {
        const type = this.types.get(typeOid)
        if (!type)
            throw new Error(`Unknown type: oid ${typeOid}`)
        if (type.typreceive in converters) {
            const converter: TypeAwareRecvFunc = (converters as any)[type.typreceive]
            return converter(array, type, this)
        } else {
            throw new Error(`Unsupported type: ${type.typname} (oid ${typeOid}, typreceive ${type.typreceive})`)
        }
    }

    send(typeOid: number, value: unknown): Uint8Array {
        const type = this.types.get(typeOid)
        if (!type)
            throw new Error(`Unknown type: oid ${typeOid}`)
        if (type.typsend in converters) {
            const converter: TypeAwareSendFunc = (converters as any)[type.typsend]
            return converter(value, type, this)
        } else {
            throw new Error(`Unsupported type: ${type.typname} (oid ${typeOid}, typsend ${type.typsend})`)
        }
    }

    private loadTable(rows: TypeRow[]) {
        // Index by oid
        this.types = new Map(rows.map( row => [ row.oid, row ]))
    }
}

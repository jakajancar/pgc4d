import { Hash, encode } from './deps.ts'

type Resolve<T> = (value?: T | PromiseLike<T> | undefined) => void
type Reject = (reason?: any) => void

/** A promise whose state can be inspected and can be manipulated from outside of the executor. */
export class Deferred<T> extends Promise<T> {
    state: 'pending' | 'fulfilled' | 'rejected'
    get pending(): boolean { return this.state === 'pending' }
    get settled(): boolean { return this.state !== 'pending' }

    value?: T
    rejectionReason?: any
    
    resolve: Resolve<T>
    reject: Reject

    constructor(executor?: (resolve: Resolve<T>, reject: Reject) => void) {
        let methods: { resolve: Resolve<T>, reject: Reject }
        super((resolve, reject) => {
            methods = { resolve, reject }
        })

        this.state = 'pending'
        this.resolve = (async (valuePromise: T | PromiseLike<T> | undefined) => {
            const value = valuePromise instanceof Promise ? await valuePromise : valuePromise
            this.state = 'fulfilled'
            this.value = value
            methods.resolve(value)
        }).bind(this)
        this.reject = (reason) => {
            this.state = 'rejected'
            this.rejectionReason = reason
            methods.reject(reason)
        }
        if (executor)
            executor(methods!.resolve, methods!.reject)
    }
    
    static resolve<T>(value?: T | undefined) {
        const d = new Deferred<T>()
        d.resolve(value)
        return d
    }
}

export class Pipe<T> {
    writes = new Array<[T, Deferred<void>]>()
    reads = new Array<Deferred<T>>()

    constructor() {}

    get delta() { return this.writes.length - this.reads.length }

    write(value: T): Promise<void> {
        const read = this.reads.shift()
        if (read) {
            read.resolve(value)
            return Promise.resolve()
        } else {
            const promise = new Deferred<void>()
            this.writes.push([value, promise])
            return promise
        }
    }

    read(): Promise<T> {
        const write = this.writes.shift()
        if (write) {
            const [value, done] = write
            done.resolve()
            return Promise.resolve(value)
        } else {
            const promise = new Deferred<T>()
            this.reads.push(promise)
            return promise
        }
    }
}

export function hashMd5Password(
    password: string,
    username: string,
    salt: Uint8Array,
 ): string {
    function md5(bytes: Uint8Array): string {
        return new Hash("md5").digest(bytes).hex();
    }

    const innerHash = md5(encode(password + username));
    const innerBytes = encode(innerHash);
    const outerBuffer = new Uint8Array(innerBytes.length + salt.length);
    outerBuffer.set(innerBytes);
    outerBuffer.set(salt, innerBytes.length);
    const outerHash = md5(outerBuffer);
    return "md5" + outerHash;
}

export function arrayDimensions(x: any): number[] {
    if (!(x instanceof Array)) {
        return []
    } else {
        if (x.length === 0) {
            return [0]
        } else {
            const elemResults = x.map(arrayDimensions)
            for (const elemResult of elemResults) {
                if (!elementsEqual(elemResult, elemResults[0]))
                    throw new Error('Multidimensional arrays must have sub-arrays with matching dimensions.')
            }
            return [x.length].concat(elemResults[0])
        }
    }
}

function elementsEqual(x: any[], y: any[]): boolean {
    if (x.length !== y.length)
        return false
    for (let i = 0; i < x.length; i++) {
        if (x[i] !== y[i])
            return false
    }
    return true
}

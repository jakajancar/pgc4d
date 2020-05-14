const { test } = Deno
import { assertEquals, assertStrContains, assert } from '../src/deps.ts'
import { Notification, PgError } from '../src/types.ts'
import { connectPg } from '../src/connection.ts'
import { Deferred } from '../src/utils.ts'
import { testOptions } from './env.ts'

test('server-initiated disconnect', async () => {
    const killer = await connectPg(testOptions)
    try {
        const victim = await connectPg(testOptions)
        try {
            let victimDone = false
            let victimError: Error | undefined
            victim.done.then(e => {
                victimDone = true
                victimError = e
            })
            await killer.query('SELECT pg_terminate_backend($1)', [victim.pid])
            await victim.done
            assert(victimError instanceof PgError)
            assertStrContains(victimError.message, 'terminating connection due to administrator command')
        } finally {
            victim.close()
        }
    } finally {
        killer.close()
    }
})

test('notifications work on same connection', async () => {
    const notifications: Notification[] = []
    const db = await connectPg({
        ...testOptions,
        onNotification: async n => { notifications.push(n) }
    })
    try {
        await db.query('LISTEN "my channel"')
        await db.query(`NOTIFY "my channel", 'my message 1'`)
        assertEquals(notifications, [{
            channel: 'my channel',
            payload: 'my message 1',
            sender: db.pid
        }])
        await db.query(`NOTIFY "my channel", 'my message 2'`)
        assertEquals(notifications.length, 2)
    } finally {
        db.close()
    }
})

test('notifications work across connections', async () => {
    let notification = new Deferred<Notification>()
    const listener = await connectPg({
        ...testOptions,
        onNotification: async n => { notification.resolve(n) }
    })
    try {
        const notifier = await connectPg(testOptions)
        try {
            await listener.query('LISTEN "my channel"')

            // Notification from another connection
            await notifier.query(`NOTIFY "my channel", 'message from notifier 1'`)
            assertEquals(await notification, {
                channel: 'my channel',
                payload: 'message from notifier 1',
                sender: notifier.pid
            })

            // Notification from self
            notification = new Deferred<Notification>()
            await listener.query(`NOTIFY "my channel", 'message from listener 1'`)
            assertEquals(await notification, {
                channel: 'my channel',
                payload: 'message from listener 1',
                sender: listener.pid
            })
        } finally {
            notifier.close()
        }
    } finally {
        listener.close()
    }
})

test('notifications warn if no handler', async () => {
    let notified = false
    const orig = console.warn
    console.warn = (...args: unknown[]): void => {
        if (args[0] === 'Received notification, but no handler. Please pass `onNotification` option to `connectPg()`.')
            notified = true
        else
            orig(...args)
    }
    try {
        const db = await connectPg(testOptions)
        try {
            await db.query('LISTEN "my channel"')
            await db.query(`NOTIFY "my channel", 'my message 1'`)
            assert(notified)
        } finally {
            db.close()
        }
    } finally {
        console.warn = orig
    }
})

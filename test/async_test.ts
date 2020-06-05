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
    const db = await connectPg(testOptions)
    try {
        await db.addListener('my channel', async n => { notifications.push(n) })

        // Using pg_notify
        await db.query(`SELECT pg_notify($1, $2)`, ["my channel", 'my message 1'])
        assertEquals(notifications, [{
            channel: 'my channel',
            payload: 'my message 1',
            sender: db.pid
        }])

        // Using NOTIFY
        await db.query(`NOTIFY "my channel", 'my message 2'`)
        assertEquals(notifications.length, 2)
    } finally {
        db.close()
    }
})

test('notifications work across connections', async () => {
    let notification = new Deferred<Notification>()
    const listener = await connectPg(testOptions)
    try {
        await listener.addListener('my channel', async n => { notification.resolve(n) })
        const notifier = await connectPg(testOptions)
        try {
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

test('notifications work with multiple listeners', async () => {
    const notifications1: Notification[] = []
    const notifications2: Notification[] = []
    const db = await connectPg(testOptions)
    try {
        // add two
        const listener1 = async (n: Notification) => { notifications1.push(n) }
        const listener2 = async (n: Notification) => { notifications2.push(n) }
        const listener3 = async (n: Notification) => { assert(false, 'not expected') }
        await db.addListener('my channel', listener1)
        await db.addListener('my channel', listener2)
        await db.addListener('my channel2', listener3)
        assertEquals((await db.query(`SELECT * FROM pg_listening_channels()`)).column, ['my channel', 'my channel2'])

        await db.query(`NOTIFY "my channel", 'my message 1'`)
        assertEquals(notifications1.length, 1)
        assertEquals(notifications2.length, 1)

        // remove first
        await db.removeListener('my channel', listener1)
        assertEquals((await db.query(`SELECT * FROM pg_listening_channels()`)).column, ['my channel', 'my channel2'])

        await db.query(`NOTIFY "my channel", 'my message 2'`)
        assertEquals(notifications1.length, 1)
        assertEquals(notifications2.length, 2)

        // remove second
        await db.removeListener('my channel', listener2)
        assertEquals((await db.query(`SELECT * FROM pg_listening_channels()`)).column, ['my channel2'])

        await db.query(`NOTIFY "my channel", 'my message 3'`)
        assertEquals(notifications1.length, 1)
        assertEquals(notifications2.length, 2)
    } finally {
        db.close()
    }
})

test('notifications not received after unsubscribed, before re-subscribed', async () => {
    const notifications1: Notification[] = []
    const notifications2: Notification[] = []
    const listener1 = async (n: Notification) => { notifications1.push(n) }
    const listener2 = async (n: Notification) => { notifications2.push(n) }

    const db = await connectPg(testOptions)
    try {
        // subscribe
        await db.addListener('my channel', listener1)

        // notify, but replace listener before really sent
        const notified = db.query(`NOTIFY "my channel", 'my message 1'`)
        const listener1Removed = db.removeListener('my channel', listener1)
        const listener2Added = db.addListener('my channel', listener2)

        await notified
        await listener1Removed
        await listener2Added

        assertEquals(notifications1.length, 0, `Listener 1 received message after unsubscribing`)
        assertEquals(notifications2.length, 0, `Listener 2 received message for previous LISTEN, before it's own LISTEN was executed`)
    } finally {
        db.close()
    }
})

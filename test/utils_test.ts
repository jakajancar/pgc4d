const { test } = Deno
import { assertEquals, assert, assertThrows } from '../src/deps.ts'
import { Deferred, arrayDimensions } from '../src/utils.ts'

test('deferred', async () => {
    // Create promise
    const def = new Deferred<number>()
    assertEquals(def.state, 'pending')
    assertEquals(def.value, undefined)
    assertEquals(def.rejectionReason, undefined)

    // Register callback before
    let then1Called = false
    const then1 = def.then(() => { then1Called = true })

    // Resolve
    def.resolve(42)
    assertEquals(def.state, 'fulfilled')
    assertEquals(def.value, 42)
    assertEquals(def.rejectionReason, undefined)

    // Register callback after resolved
    let then2Called = false
    const then2 = def.then(() => { then2Called = true })

    // Ensure both thens were called
    await then1
    await then2
    assert(then1Called)
    assert(then2Called)
})

test('arrayDimensions', () => {
    assertEquals(arrayDimensions(1), [])
    assertEquals(arrayDimensions([]), [0])
    assertEquals(arrayDimensions([[[]]]), [1,1,0])
    assertEquals(arrayDimensions([1,2,3]), [3])
    assertEquals(arrayDimensions([[],[],[]]), [3,0])
    assertEquals(arrayDimensions([[1,2],[3,4],[5,6]]), [3,2])
    
    assertThrows(() => { arrayDimensions([1, []]) }, undefined, 'matching dimensions')
    assertThrows(() => { arrayDimensions([[1], [1,2]]) }, undefined, 'matching dimensions')
})
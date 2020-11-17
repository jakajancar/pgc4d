const { test } = Deno
import { connectPg } from '../src/mod.ts'

// Tests for problems in `mod.ts`, such as:
//
//   TS1205 [ERROR]: Re-exporting a type when the '--isolatedModules' flag is provided requires using 'export type'.
//       KeyedRow,
//       ~~~~~~~~
//       at file:///app/src/mod.ts:33:5
//

test('dummy', async () => {
    // needed or deno test fails
})

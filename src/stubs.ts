/**
 * Where this package's stubs live.
 *
 * Its own module, as upstream keeps it: a stub root is a path a package
 * publishes, and a caller importing it should not have to import the
 * configure hook to get it.
 *
 * `import.meta.dirname` rather than a URL — upstream reads it straight off
 * `import.meta`, and deriving it from `import.meta.url` breaks wherever that
 * is not a `file://` URL, which is what a test runner serving modules over
 * HTTP hands you.
 */

import { join } from "node:path";

export const stubsRoot = join(import.meta.dirname, "..", "stubs");

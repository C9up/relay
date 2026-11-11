/**
 * The shipped source asserts no types: no `as T` (an `as const` is a literal,
 * not a claim), the rule the whole codebase keeps alongside no `any` and no
 * `!`. A cast is a statement the compiler cannot check, made in the one place
 * nothing else will.
 */

import { readdir, readFile } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { describe, expect, it } from "vitest";

const SRC = path.resolve(
	path.dirname(fileURLToPath(import.meta.url)),
	"..",
	"..",
	"src",
);

async function sources(directory: string): Promise<string[]> {
	const entries = await readdir(directory, { withFileTypes: true });
	const nested = await Promise.all(
		entries.map(async (entry) => {
			const full = path.join(directory, entry.name);
			if (entry.isDirectory()) return sources(full);
			return entry.name.endsWith(".ts") ? [full] : [];
		}),
	);
	return nested.flat();
}

/** `text` blanked out, its line breaks kept so line numbers still hold. */
const blank = (text: string): string => text.replace(/[^\n]/g, " ");

/** The code with comments and string literals blanked out. */
function codeOnly(text: string): string {
	return text
		.replace(/\/\*[\s\S]*?\*\//g, blank)
		.replace(/\/\/.*$/gm, blank)
		.replace(/"(?:\\.|[^"\\\n])*"/g, blank)
		.replace(/'(?:\\.|[^'\\\n])*'/g, blank)
		.replace(/`(?:\\.|[^`\\])*`/g, blank);
}

/** `import { a as b }`, `export { default as X }`: renames, not casts. */
const RENAME =
	/^\s*(?:(?:import|export)\b.*|(?:type\s+)?[\w$]+\s+as\s+[\w$]+,?\s*)$/;

describe("relay > source rules", () => {
	it("holds no `as T` cast in src", async () => {
		const offenders: string[] = [];
		for (const file of await sources(SRC)) {
			const lines = codeOnly(await readFile(file, "utf8")).split("\n");
			lines.forEach((line, index) => {
				if (RENAME.test(line)) return;
				if (/[\w)\]}>]\s+as\s+(?!const\b)[\w{([]/.test(line)) {
					offenders.push(`${path.relative(SRC, file)}:${index + 1}`);
				}
			});
		}
		expect(offenders).toEqual([]);
	});
});

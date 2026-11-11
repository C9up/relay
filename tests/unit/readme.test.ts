/**
 * The README describes relay on its own terms: it borrows the upstream
 * framework's spirit, never its name.
 */

import { readFile } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { describe, expect, it } from "vitest";

const README = path.resolve(
	path.dirname(fileURLToPath(import.meta.url)),
	"..",
	"..",
	"README.md",
);

describe("relay > README", () => {
	it("names no upstream framework or package", async () => {
		const text = await readFile(README, "utf8");
		expect(text).not.toMatch(/adonis|transmit|lucid|japa/i);
	});
});

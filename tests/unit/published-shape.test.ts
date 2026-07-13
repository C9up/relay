/**
 * Verifies the published tarball shape for @c9up/relay.
 *
 * `publishConfig` rewrites the entrypoints from `src/*` to `dist/*` at publish
 * time, so a missing/stale dist export is invisible to workspace-mode installs
 * — the first path that sees the published shape is `npm install` in a consumer,
 * by which time the broken tarball is already on the registry. This test packs
 * into a tmp dir and asserts every advertised export target lands in the tarball.
 */

import { execFileSync } from "node:child_process";
import { existsSync, mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { afterAll, beforeAll, describe, expect, it } from "vitest";

type ExportEntry = { import: string; types: string };
type PackJson = { exports: Record<string, ExportEntry> };

function isExportEntry(value: unknown): value is ExportEntry {
	if (value === null || typeof value !== "object") return false;
	if (!("import" in value) || !("types" in value)) return false;
	return typeof value.import === "string" && typeof value.types === "string";
}

function isPackJson(value: unknown): value is PackJson {
	if (value === null || typeof value !== "object" || !("exports" in value)) {
		return false;
	}
	const { exports } = value;
	if (exports === null || typeof exports !== "object") return false;
	for (const key of Object.keys(exports)) {
		if (!isExportEntry(Reflect.get(exports, key))) return false;
	}
	return true;
}

const HERE = path.dirname(fileURLToPath(import.meta.url));
const PKG_ROOT = path.resolve(HERE, "..", "..");

describe("@c9up/relay published shape", () => {
	let tmpDir = "";
	let tarballPath = "";

	beforeAll(() => {
		// Fail loudly when build hasn't run — never skip.
		if (!existsSync(path.join(PKG_ROOT, "dist/index.js"))) {
			throw new Error(
				"Run `pnpm --filter @c9up/relay build` before this test — " +
					"published-shape verification requires the built dist.",
			);
		}
		tmpDir = mkdtempSync(path.join(tmpdir(), "relay-pack-"));
		const isWin = process.platform === "win32";
		const stdout = execFileSync(
			isWin ? "pnpm.cmd" : "pnpm",
			["pack", "--pack-destination", tmpDir],
			{ cwd: PKG_ROOT, encoding: "utf8", shell: isWin },
		);
		const lastLine = stdout
			.trim()
			.split(/\r?\n/)
			.map((line) => line.trim())
			.filter(Boolean)
			.pop();
		if (lastLine && existsSync(lastLine)) {
			tarballPath = lastLine;
		} else {
			throw new Error(
				`pnpm pack did not produce a discoverable tarball; stdout was:\n${stdout}`,
			);
		}
	});

	afterAll(() => {
		if (tmpDir) rmSync(tmpDir, { recursive: true, force: true });
	});

	it("advertises the 4 sub-paths and every export target lands in the tarball", () => {
		const tarLocal = process.platform === "win32" ? ["--force-local"] : [];
		const pkgJsonRaw = execFileSync(
			"tar",
			[...tarLocal, "-xzOf", tarballPath, "package/package.json"],
			{ encoding: "utf8" },
		);
		const parsed: unknown = JSON.parse(pkgJsonRaw);
		if (!isPackJson(parsed)) {
			throw new Error("tarball package.json shape unexpected");
		}

		expect(Object.keys(parsed.exports).sort()).toEqual([
			".",
			"./provider",
			"./services/main",
			"./testing",
		]);

		const inTarball = new Set(
			execFileSync("tar", [...tarLocal, "-tzf", tarballPath], {
				encoding: "utf8",
			})
				.split("\n")
				.filter(Boolean),
		);
		for (const entry of Object.values(parsed.exports)) {
			const importTarget = entry.import.replace(/^\.\//, "package/");
			const typesTarget = entry.types.replace(/^\.\//, "package/");
			expect(inTarball.has(importTarget), importTarget).toBe(true);
			expect(inTarball.has(typesTarget), typesTarget).toBe(true);
		}
	});
});

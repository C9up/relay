/**
 * `ream configure @c9up/relay`.
 *
 * The hook is what makes `ream add` mean installed AND working: the provider
 * alone reads a config file that would not exist.
 */
import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import { describe, expect, it } from "vitest";
import { configure } from "../../src/configure.js";

/**
 * Read a stub the way `codemods.makeUsingStub` does.
 *
 * The real file, not a fixture: a test that stubbed this out would pass with
 * a stub that does not exist.
 */
function renderStub(
	stubsRoot: string,
	stubPath: string,
	state: Record<string, string | number | boolean>,
): { to: string; body: string } {
	const raw = readFileSync(resolve(stubsRoot, stubPath), "utf8");
	const [, front = "", body = ""] = raw.split(/^---\r?\n/m, 3);
	const declared = /^to:\s*(.+)$/m.exec(front)?.[1]?.trim() ?? "";
	const render = (text: string): string =>
		text.replace(/\{\{\s*([\w.]+)\s*\}\}/g, (match, key: string) =>
			state[key] === undefined ? match : String(state[key]),
		);
	return { to: render(declared), body: render(body) };
}

function fakeCodemods() {
	const providers: string[] = [];
	const files: Array<{ path: string; content: string }> = [];
	return {
		providers,
		files,
		codemods: {
			async addProvider(importPath: string) {
				providers.push(importPath);
			},
			async makeUsingStub(
				stubsRoot: string,
				stubPath: string,
				state: Record<string, string | number | boolean> = {},
			) {
				const { to, body } = renderStub(stubsRoot, stubPath, state);
				await this.writeFile(to, body);
				return { path: to, contents: body };
			},
			async writeFile(path: string, content: string) {
				files.push({ path, content });
			},
		},
	};
}

describe("relay > configure", () => {
	it("registers the provider and writes the config it reads", async () => {
		const { providers, files, codemods } = fakeCodemods();

		await configure(codemods);

		expect(providers).toEqual(["@c9up/relay/provider"]);
		expect(files.map((f) => f.path)).toEqual(["config/relay.ts"]);
	});

	it("writes a config that imports from the package it configures", async () => {
		const { files, codemods } = fakeCodemods();

		await configure(codemods);

		// A stub importing the wrong package typechecks nowhere and is the one
		// mistake a generated file must not make.
		expect(files[0]?.content).toContain("from '@c9up/relay'");
	});
});

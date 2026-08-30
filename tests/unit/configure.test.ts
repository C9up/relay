/**
 * `ream configure @c9up/relay`.
 *
 * The hook is what makes `ream add` mean installed AND working: the provider
 * alone reads a config file that would not exist.
 */
import { describe, expect, it } from "vitest";
import { configure } from "../../src/configure.js";

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

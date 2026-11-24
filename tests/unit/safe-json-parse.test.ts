/**
 * Hub frames are client JSON, and an invocation's arguments reach the hub
 * method as parsed: a `__proto__` key must not get there.
 */

import { describe, expect, it } from "vitest";
import { safeJsonParse } from "../../src/safeJsonParse.js";

describe("safeJsonParse", () => {
	it("removes a __proto__ key, so a merge keeps its prototype", () => {
		const patch = safeJsonParse(
			'{"theme":"dark","__proto__":{"isAdmin":true}}',
		);

		expect(Object.getOwnPropertyNames(patch)).toEqual(["theme"]);
		const settings = Object.assign({ theme: "light" }, patch);
		expect(Reflect.get(settings, "isAdmin")).toBeUndefined();
		expect(Object.getPrototypeOf(settings)).toBe(Object.prototype);
	});

	it("removes it at any depth, inside arrays too", () => {
		const parsed = safeJsonParse(
			'{"a":[{"b":{"__proto__":{"x":1},"keep":true}}],"__proto__":{"y":2}}',
		);

		expect(JSON.stringify(parsed)).toBe('{"a":[{"b":{"keep":true}}]}');
	});

	it("removes a constructor that carries a prototype, and keeps one that is data", () => {
		expect(
			JSON.stringify(
				safeJsonParse('{"constructor":{"prototype":{"polluted":true}},"n":1}'),
			),
		).toBe('{"n":1}');
		expect(safeJsonParse('{"constructor":"Ford"}')).toEqual({
			constructor: "Ford",
		});
		expect(
			JSON.stringify(safeJsonParse('{"constructor":{"name":"Ford"}}')),
		).toBe('{"constructor":{"name":"Ford"}}');
		expect(JSON.stringify(safeJsonParse('{"constructor":null}'))).toBe(
			'{"constructor":null}',
		);
	});

	it("does not pollute Object.prototype through a recursive merge", () => {
		const merge = (target: object, source: object): void => {
			for (const key of Object.keys(source)) {
				const value: unknown = Reflect.get(source, key);
				const into: unknown = Reflect.get(target, key);
				if (
					typeof value === "object" &&
					value !== null &&
					typeof into === "object" &&
					into !== null
				) {
					merge(into, value);
				} else {
					Reflect.set(target, key, value);
				}
			}
		};

		const parsed = safeJsonParse(
			'{"__proto__":{"pollutedByHubArgument":true}}',
		);
		if (typeof parsed !== "object" || parsed === null) {
			throw new Error("not an object");
		}
		merge({ theme: "light" }, parsed);

		expect(Reflect.get({}, "pollutedByHubArgument")).toBeUndefined();
	});

	it("answers scalars and null as JSON.parse does", () => {
		expect(safeJsonParse("42")).toBe(42);
		expect(safeJsonParse('"text"')).toBe("text");
		expect(safeJsonParse("null")).toBeNull();
		expect(safeJsonParse("[1,[2]]")).toEqual([1, [2]]);
	});

	it("walks a deeply nested value without recursion", () => {
		const depth = 20_000;
		const parsed = safeJsonParse(
			`${'{"a":'.repeat(depth)}{"__proto__":{"x":1}}${"}".repeat(depth)}`,
		);

		let node: unknown = parsed;
		for (let i = 0; i < depth; i++) node = Reflect.get(Object(node), "a");
		expect(Object.getOwnPropertyNames(node)).toEqual([]);
	});

	it("throws on malformed input", () => {
		expect(() => safeJsonParse("{not json")).toThrow(SyntaxError);
	});
});

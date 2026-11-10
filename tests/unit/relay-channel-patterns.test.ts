/**
 * The channel pattern grammar an authorizer is registered under.
 *
 * Transmit matches secured channels with matchit: static segments, `:param`,
 * `:param?` (optional) and a trailing `*` after any prefix, with the params of
 * the matching pattern handed to its authorizer. Relay read `orgs/:org/*` as the
 * literal prefix `orgs/:org/`, and `:id?` as a mandatory param named `id?`, so
 * an authorizer the application wrote was silently never called.
 */

import { describe, expect, it } from "vitest";
import { Relay, type RelaySseStream } from "../../src/Relay.js";

function stream(): RelaySseStream {
	return {
		id: `s-${Math.random()}`,
		isOpen: () => true,
		send: async () => true,
		onClose: () => {},
		end: async () => {},
	};
}

/** A relay with one authorizer under `pattern`, recording the params it sees. */
function guarded(pattern: string, answer = true) {
	const relay = new Relay();
	const seen: Array<Record<string, string>> = [];
	relay.authorize(pattern, (_ctx, params) => {
		seen.push(params);
		return answer;
	});
	const connected = relay.connect(stream(), {});
	if (connected.outcome !== "ok") throw new Error("capped");
	const subscribe = (channel: string) =>
		relay.subscribe(connected.uid, channel, {});
	return { relay, seen, subscribe };
}

const NO_AUTHORIZER = {
	ok: false,
	status: 403,
	code: "E_RELAY_CHANNEL_NO_AUTHORIZER",
};

describe("relay > channel patterns", () => {
	it("matches a param before a trailing wildcard and hands over its params", async () => {
		const { seen, subscribe } = guarded("orgs/:org/*");

		expect(await subscribe("orgs/acme/projects")).toEqual({ ok: true });
		expect(await subscribe("orgs/acme/projects/42/tasks")).toEqual({
			ok: true,
		});
		expect(seen).toEqual([
			{ org: "acme", "*": "projects" },
			{ org: "acme", "*": "projects/42/tasks" },
		]);
	});

	it("requires something after the wildcard's prefix", async () => {
		const { seen, subscribe } = guarded("orgs/:org/*");

		expect(await subscribe("orgs/acme")).toEqual(NO_AUTHORIZER);
		expect(await subscribe("teams/acme/projects")).toEqual(NO_AUTHORIZER);
		expect(seen).toEqual([]);
	});

	it("matches a static prefix before a wildcard", async () => {
		const { seen, subscribe } = guarded("projects/*");

		expect(await subscribe("projects/1")).toEqual({ ok: true });
		expect(await subscribe("projectsX/1")).toEqual(NO_AUTHORIZER);
		expect(seen).toEqual([{ "*": "1" }]);
	});

	it("matches an optional param with and without its segment", async () => {
		const { seen, subscribe } = guarded("posts/:id?");

		expect(await subscribe("posts/42")).toEqual({ ok: true });
		expect(await subscribe("posts")).toEqual({ ok: true });
		expect(await subscribe("posts/42/comments")).toEqual(NO_AUTHORIZER);
		expect(seen).toEqual([{ id: "42" }, {}]);
	});

	it("names the param without its question mark", async () => {
		const { seen, subscribe } = guarded("rooms/:room/:thread?");

		await subscribe("rooms/a/b");
		await subscribe("rooms/a");

		expect(seen).toEqual([{ room: "a", thread: "b" }, { room: "a" }]);
	});

	it("matches a param carrying a static suffix", async () => {
		const { seen, subscribe } = guarded("exports/:name.csv");

		expect(await subscribe("exports/report.csv")).toEqual({ ok: true });
		expect(await subscribe("exports/report.pdf")).toEqual(NO_AUTHORIZER);
		expect(seen).toEqual([{ name: "report" }]);
	});

	it("still requires every plain param", async () => {
		const { seen, subscribe } = guarded("users/:id/notifications");

		expect(await subscribe("users/7/notifications")).toEqual({ ok: true });
		expect(await subscribe("users/notifications")).toEqual(NO_AUTHORIZER);
		expect(await subscribe("users/7/notifications/x")).toEqual(NO_AUTHORIZER);
		expect(seen).toEqual([{ id: "7" }]);
	});

	it("keeps an exact registration ahead of a pattern, with no params", async () => {
		const relay = new Relay();
		const calls: string[] = [];
		relay.authorize("users/:id", (_ctx, params) => {
			calls.push(`pattern ${JSON.stringify(params)}`);
			return true;
		});
		relay.authorize("users/me", (_ctx, params) => {
			calls.push(`exact ${JSON.stringify(params)}`);
			return true;
		});
		const c = relay.connect(stream(), {});
		if (c.outcome !== "ok") throw new Error("capped");

		await relay.subscribe(c.uid, "users/me", {});
		await relay.subscribe(c.uid, "users/7", {});

		expect(calls).toEqual(["exact {}", 'pattern {"id":"7"}']);
	});

	it("hands a channel spelled like a pattern to the pattern, with its params", async () => {
		// A channel literally named `users/:id` must not reach the authorizer
		// with no `id` at all — the shape an authorizer is least ready for.
		const { seen, subscribe } = guarded("users/:id");

		await subscribe("users/:id");

		expect(seen).toEqual([{ id: ":id" }]);
	});

	it("refuses at registration a pattern that could never match as written", () => {
		const relay = new Relay();
		const noop = () => true;

		expect(() => relay.authorize("files/*/meta", noop)).toThrow(
			/wildcard.*last/i,
		);
		expect(() => relay.authorize("files/*x", noop)).toThrow(/wildcard/i);
		expect(() => relay.authorize("users/:", noop)).toThrow(/param/i);
		expect(() => relay.authorize("users/:?", noop)).toThrow(/param/i);
		expect(() => relay.authorize("a/:b?/c", noop)).toThrow(/optional/i);
		expect(() => relay.authorize("a/:id/:id", noop)).toThrow(/twice/i);
		expect(() => relay.authorize("", noop)).toThrow(/empty/i);
		// Nothing half-registered.
		expect(relay.authorizedChannels()).toEqual([]);
	});
});

/**
 * A subscription is accepted only from the session that opened the stream.
 *
 * The uid is the stream's credential, and the user id binds it for an
 * authenticated stream. An anonymous one still has a session — a guest cart, a
 * locale, anything an authorizer reads off `ctx.session` — and a request from
 * another session naming its uid would have the authorizer approve, with THAT
 * session, frames that go down a stream someone else holds. The vector is a
 * forged `connected` frame: the victim's own client re-posts its channels under
 * the attacker's uid, with the victim's cookies.
 *
 * An established session is one the request came in with (`fresh === false`).
 * A fresh one is a new random id on every request until a cookie pins it, so it
 * identifies nobody and counts as no session at all.
 */

import { describe, expect, it } from "vitest";
import {
	Relay,
	type RelayContext,
	type RelaySseStream,
} from "../../src/Relay.js";

function stream(): RelaySseStream {
	return {
		id: `s-${Math.random()}`,
		isOpen: () => true,
		send: async () => true,
		onClose: () => {},
		end: async () => {},
	};
}

const established = (sessionId: string): RelayContext => ({
	session: { sessionId, fresh: false },
});
const fresh = (): RelayContext => ({
	session: { sessionId: `new-${Math.random()}`, fresh: true },
});

const NOT_OWNER = { ok: false, status: 403, code: "E_RELAY_NOT_OWNER" };

/** A relay whose one authorizer reads the session, as a guest-cart channel would. */
function sessionGuarded() {
	const relay = new Relay();
	const asked: string[] = [];
	relay.authorize<{ id: string }>("carts/:id", (_ctx, { id }) => {
		asked.push(id);
		return true;
	});
	return { relay, asked };
}

describe("relay > subscriptions are bound to the session that opened the stream", () => {
	it("refuses another session naming the uid, before any authorizer runs", async () => {
		const { relay, asked } = sessionGuarded();
		const c = relay.connect(stream(), established("attacker-session"));
		if (c.outcome !== "ok") throw new Error("capped");

		const result = await relay.subscribe(
			c.uid,
			"carts/victim",
			established("victim-session"),
		);

		expect(result).toEqual(NOT_OWNER);
		expect(asked).toEqual([]);
		expect(relay.channelSubscribers("carts/victim")).toBe(0);
	});

	it("refuses a request with a session on a stream opened without one", async () => {
		// The attacker simply opens the stream with no cookie at all.
		const { relay, asked } = sessionGuarded();
		const c = relay.connect(stream(), fresh());
		if (c.outcome !== "ok") throw new Error("capped");

		const result = await relay.subscribe(
			c.uid,
			"carts/victim",
			established("victim-session"),
		);

		expect(result).toEqual(NOT_OWNER);
		expect(asked).toEqual([]);
	});

	it("accepts the session that opened the stream", async () => {
		const { relay } = sessionGuarded();
		const c = relay.connect(stream(), established("s1"));
		if (c.outcome !== "ok") throw new Error("capped");

		expect(await relay.subscribe(c.uid, "carts/s1", established("s1"))).toEqual(
			{ ok: true },
		);
	});

	it("accepts a session-less client on every request, fresh ids and all", async () => {
		const { relay } = sessionGuarded();
		const c = relay.connect(stream(), fresh());
		if (c.outcome !== "ok") throw new Error("capped");

		expect(await relay.subscribe(c.uid, "carts/1", fresh())).toEqual({
			ok: true,
		});
		expect(await relay.subscribe(c.uid, "carts/2", {})).toEqual({ ok: true });
	});

	it("binds an authenticated stream to its session too", async () => {
		const { relay } = sessionGuarded();
		const auth = { isAuthenticated: true, user: { id: "u1" } };
		const c = relay.connect(stream(), {
			auth,
			session: { sessionId: "s1", fresh: false },
		});
		if (c.outcome !== "ok") throw new Error("capped");

		expect(
			await relay.subscribe(c.uid, "carts/1", {
				auth,
				session: { sessionId: "s2", fresh: false },
			}),
		).toEqual(NOT_OWNER);
		expect(
			await relay.subscribe(c.uid, "carts/1", {
				auth,
				session: { sessionId: "s1", fresh: false },
			}),
		).toEqual({ ok: true });
	});

	it("refuses an unsubscribe from another session", async () => {
		const { relay } = sessionGuarded();
		const c = relay.connect(stream(), established("s1"));
		if (c.outcome !== "ok") throw new Error("capped");
		await relay.subscribe(c.uid, "carts/s1", established("s1"));

		expect(relay.unsubscribe(c.uid, "carts/s1", established("s2"))).toBe(
			"forbidden",
		);
		expect(relay.channelSubscribers("carts/s1")).toBe(1);
		expect(relay.unsubscribe(c.uid, "carts/s1", established("s1"))).toBe("ok");
		expect(relay.channelSubscribers("carts/s1")).toBe(0);
	});
});

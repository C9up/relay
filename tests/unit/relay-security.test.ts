/**
 * Regression tests for the SSE uid-hijack security fixes.
 *
 * Pre-fix: client picked its own uid via `?uid=` query string; subscribe/
 * unsubscribe trusted any caller knowing a uid. An attacker who guessed
 * (or learned) another user's uid could connect, hijack, subscribe to
 * private channels, or disconnect the legit client.
 *
 * Post-fix:
 *   - connect() server-issues a randomUUID for every connection; the
 *     client never picks it, and one user's tabs each get their own.
 *   - subscribe()/unsubscribe() verify the requester's auth identity
 *     matches the identity recorded at connect-time (when the client
 *     was authenticated). Anonymous clients use uid-as-secret.
 */

import { describe, expect, it } from "vitest";
import { Relay, type RelaySseStream } from "../../src/Relay.js";

function fakeSse(id = `s_${Math.random()}`): RelaySseStream {
	let open = true;
	let closeCb: (() => void) | undefined;
	return {
		id,
		isOpen: () => open,
		async send() {
			return open;
		},
		onClose: (cb) => {
			closeCb = cb;
		},
		async end() {
			open = false;
			closeCb?.();
		},
	};
}

describe("relay-security > connect() identity binding", () => {
	const UUID =
		/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

	it("issues a server uid for every connection, authenticated or not", () => {
		const r = new Relay();
		const authed = r.connect(fakeSse(), {
			auth: { isAuthenticated: true, user: { id: "user-42" } },
		});
		const anonymous = r.connect(fakeSse(), {
			auth: { isAuthenticated: false },
		});
		if (authed.outcome !== "ok" || anonymous.outcome !== "ok") {
			throw new Error("unreachable");
		}
		// Never the user id: that made every tab of one account the same uid.
		expect(authed.uid).not.toBe("user-42");
		expect(authed.uid).toMatch(UUID);
		expect(anonymous.uid).toMatch(UUID);
		expect(authed.uid).not.toBe(anonymous.uid);
	});

	it("keeps two tabs of the same user open side by side", () => {
		const r = new Relay();
		const first = fakeSse("first");
		const second = fakeSse("second");
		const auth = { isAuthenticated: true, user: { id: "u1" } };
		r.connect(first, { auth });
		r.connect(second, { auth });
		expect(first.isOpen()).toBe(true);
		expect(second.isOpen()).toBe(true);
		expect(r.clientCount()).toBe(2);
	});

	it("returns 'capped' once maxClients connections are open", () => {
		const r = new Relay({ maxClients: 1 });
		expect(
			r.connect(fakeSse(), { auth: { isAuthenticated: false } }).outcome,
		).toBe("ok");
		expect(r.connect(fakeSse(), { auth: { isAuthenticated: false } })).toEqual({
			outcome: "capped",
		});
	});

	it("frees the slot when a connection closes", async () => {
		const r = new Relay({ maxClients: 1 });
		const sse = fakeSse();
		r.connect(sse, { auth: { isAuthenticated: false } });
		await sse.end();
		expect(r.clientCount()).toBe(0);
		expect(
			r.connect(fakeSse(), { auth: { isAuthenticated: false } }).outcome,
		).toBe("ok");
	});
});

describe("relay-security > subscribe() ownership", () => {
	it("rejects subscribe from a request whose auth doesn't match the connected client", async () => {
		const r = new Relay({ allowUnauthorizedChannels: true });
		// Victim connects authenticated as user-42.
		const victim = r.connect(fakeSse(), {
			auth: { isAuthenticated: true, user: { id: "user-42" } },
		});
		if (victim.outcome !== "ok") throw new Error("unreachable");
		// Attacker (authenticated as someone else, or anonymous) learns the
		// victim's uid from logs / a leaked SSE frame. Tries to piggy-back.
		const attackerResult = await r.subscribe(victim.uid, "private/feed", {
			auth: { isAuthenticated: true, user: { id: "attacker-1" } },
		});
		expect(attackerResult).toEqual({
			ok: false,
			status: 403,
			code: "E_RELAY_NOT_OWNER",
		});
		// Even an unauthenticated attacker is rejected.
		const anonResult = await r.subscribe(victim.uid, "private/feed", {
			auth: { isAuthenticated: false },
		});
		expect(anonResult).toEqual({
			ok: false,
			status: 403,
			code: "E_RELAY_NOT_OWNER",
		});
	});

	it("accepts subscribe from the SAME authenticated identity as the connect", async () => {
		const r = new Relay({ allowUnauthorizedChannels: true });
		const owner = r.connect(fakeSse(), {
			auth: { isAuthenticated: true, user: { id: "user-42" } },
		});
		if (owner.outcome !== "ok") throw new Error("unreachable");
		const result = await r.subscribe(owner.uid, "private/feed", {
			auth: { isAuthenticated: true, user: { id: "user-42" } },
		});
		expect(result).toEqual({ ok: true });
	});

	it("accepts subscribe on an anonymous client — uid is the only secret (uid-as-bearer)", async () => {
		const r = new Relay({ allowUnauthorizedChannels: true });
		const anon = r.connect(fakeSse(), {
			auth: { isAuthenticated: false },
		});
		if (anon.outcome !== "ok") throw new Error("unreachable");
		// Without an auth identity at connect-time, the server-issued
		// random uid IS the proof of ownership. A requester who has it can
		// subscribe; one who doesn't can't (E_RELAY_NOT_CONNECTED).
		const result = await r.subscribe(anon.uid, "public/feed", {
			auth: { isAuthenticated: false },
		});
		expect(result).toEqual({ ok: true });
	});

	it("refuses an authenticated subscribe on an anonymous connection, before any authorizer runs", async () => {
		// Whoever holds the anonymous stream would receive what the victim's
		// credentials authorized: a forged `connected` frame makes the victim's
		// own client re-post its channels under the attacker's uid, with valid
		// cookies.
		const r = new Relay();
		const asked: string[] = [];
		r.authorize<{ id: string }>("users/:id/notifications", (ctx, { id }) => {
			asked.push(id);
			return ctx.auth?.user?.id === id;
		});
		const sent: unknown[] = [];
		const attackerStream = fakeSse("attacker");
		attackerStream.send = async (_event, data) => {
			sent.push(data);
			return true;
		};
		const attacker = r.connect(attackerStream, {});
		if (attacker.outcome !== "ok") throw new Error("unreachable");

		const result = await r.subscribe(
			attacker.uid,
			"users/victim/notifications",
			{
				auth: { isAuthenticated: true, user: { id: "victim" } },
			},
		);
		r.broadcast("users/victim/notifications", { secret: "victim-only" });

		expect(result).toEqual({
			ok: false,
			status: 403,
			code: "E_RELAY_NOT_OWNER",
		});
		expect(asked).toEqual([]);
		expect(r.channelSubscribers("users/victim/notifications")).toBe(0);
		expect(JSON.stringify(sent)).not.toContain("victim-only");
	});

	it("refuses an authenticated unsubscribe on an anonymous connection", async () => {
		const r = new Relay({ allowUnauthorizedChannels: true });
		const anon = r.connect(fakeSse(), {});
		if (anon.outcome !== "ok") throw new Error("unreachable");
		await r.subscribe(anon.uid, "public/feed", {});

		const result = r.unsubscribe(anon.uid, "public/feed", {
			auth: { isAuthenticated: true, user: { id: "someone" } },
		});

		expect(result).toBe("forbidden");
		expect(r.channelSubscribers("public/feed")).toBe(1);
	});

	it("is idempotent: re-subscribing to an already-held channel returns ok (no 429 at quota)", async () => {
		const r = new Relay({
			allowUnauthorizedChannels: true,
			maxChannelsPerClient: 1,
		});
		const owner = r.connect(fakeSse(), {
			auth: { isAuthenticated: true, user: { id: "u1" } },
		});
		if (owner.outcome !== "ok") throw new Error("unreachable");
		// First subscribe fills the quota.
		const first = await r.subscribe(owner.uid, "feed", {
			auth: { isAuthenticated: true, user: { id: "u1" } },
		});
		expect(first).toEqual({ ok: true });
		// Re-subscribe — would 429 if the quota check fired first.
		const replay = await r.subscribe(owner.uid, "feed", {
			auth: { isAuthenticated: true, user: { id: "u1" } },
		});
		expect(replay).toEqual({ ok: true });
		expect(r.channelSubscribers("feed")).toBe(1);
		// A DIFFERENT channel still hits the quota wall.
		const overQuota = await r.subscribe(owner.uid, "other", {
			auth: { isAuthenticated: true, user: { id: "u1" } },
		});
		expect(overQuota).toEqual({
			ok: false,
			status: 429,
			code: "E_RELAY_MAX_CHANNELS",
		});
	});
});

describe("relay-security > unsubscribe() ownership", () => {
	it("rejects unsubscribe from a non-owner authenticated requester", async () => {
		const r = new Relay({ allowUnauthorizedChannels: true });
		const owner = r.connect(fakeSse(), {
			auth: { isAuthenticated: true, user: { id: "user-42" } },
		});
		if (owner.outcome !== "ok") throw new Error("unreachable");
		await r.subscribe(owner.uid, "feed", {
			auth: { isAuthenticated: true, user: { id: "user-42" } },
		});
		// Attacker can no longer silently DoS by mass-unsubscribing victims.
		const r2 = r.unsubscribe(owner.uid, "feed", {
			auth: { isAuthenticated: true, user: { id: "attacker-1" } },
		});
		expect(r2).toBe("forbidden");
		// Victim is still subscribed (broadcast reaches them).
		expect(r.channelSubscribers("feed")).toBe(1);
	});

	it("accepts unsubscribe from the SAME authenticated identity as the connect", async () => {
		const r = new Relay({ allowUnauthorizedChannels: true });
		const owner = r.connect(fakeSse(), {
			auth: { isAuthenticated: true, user: { id: "user-42" } },
		});
		if (owner.outcome !== "ok") throw new Error("unreachable");
		await r.subscribe(owner.uid, "feed", {
			auth: { isAuthenticated: true, user: { id: "user-42" } },
		});
		const r2 = r.unsubscribe(owner.uid, "feed", {
			auth: { isAuthenticated: true, user: { id: "user-42" } },
		});
		expect(r2).toBe("ok");
		expect(r.channelSubscribers("feed")).toBe(0);
	});

	it("is a no-op (returns 'ok') for a uid that was never connected", () => {
		const r = new Relay();
		const result = r.unsubscribe("ghost-uid", "feed", {
			auth: { isAuthenticated: false },
		});
		expect(result).toBe("ok");
	});
});

describe("relay-security > a body is not a shape just because the type says so", () => {
	it("refuses a channel that is not a string, instead of throwing out of the route", async () => {
		const r = new Relay();
		r.authorize("room/:id", () => true);
		const c = r.connect(fakeSse(), {
			auth: { isAuthenticated: false },
		});
		if (c.outcome !== "ok") throw new Error("unreachable");

		// `{}.split` is not a function: this reached the pattern matcher and
		// answered a posted body with a 500 and a stack trace.
		const result = await r.subscribe(c.uid, { evil: 1 }, {});

		expect(result).toEqual({
			ok: false,
			status: 400,
			code: "E_RELAY_BAD_REQUEST",
		});
	});

	it("refuses a uid that is not a string", async () => {
		const r = new Relay();
		expect(await r.subscribe(["a"], "room/1", {})).toEqual({
			ok: false,
			status: 400,
			code: "E_RELAY_BAD_REQUEST",
		});
	});

	it("treats a non-string unsubscribe as the no-op it is", () => {
		const r = new Relay();
		expect(r.unsubscribe(7, "feed", {})).toBe("ok");
	});
});

describe("relay-security > a subscription outliving its client", () => {
	it("does not index a client that disconnected while the authorizer ran", async () => {
		const r = new Relay();
		let release: (() => void) | undefined;
		r.authorize("room/:id", async () => {
			await new Promise<void>((resolve) => {
				release = resolve;
			});
			return true;
		});
		const sse = fakeSse();
		const c = r.connect(sse, { auth: { isAuthenticated: false } });
		if (c.outcome !== "ok") throw new Error("unreachable");

		const pending = r.subscribe(c.uid, "room/7", {});
		await new Promise((resolve) => setImmediate(resolve));
		// The socket dies mid-authorization.
		await sse.end();
		release?.();

		expect(await pending).toEqual({
			ok: false,
			status: 400,
			code: "E_RELAY_NOT_CONNECTED",
		});
		// The entry that used to stay for the life of the process: counted by
		// channelSubscribers, returned by getSubscribersFor, walked on every
		// broadcast, and removable by nothing.
		expect(r.channelSubscribers("room/7")).toBe(0);
		expect(r.getSubscribersFor("room/7")).toEqual([]);
	});

	it("hands an authorizer the params of the pattern that selected it", async () => {
		const r = new Relay();
		const seen: Array<Record<string, string>> = [];
		// Registered first, so the old two-walk lookup found ITS params for a
		// channel the exact registration below was the one to answer.
		r.authorize("users/:id", (_ctx, params) => {
			seen.push({ pattern: "users/:id", ...params });
			return true;
		});
		r.authorize("users/me", (_ctx, params) => {
			seen.push({ pattern: "users/me", ...params });
			return true;
		});
		const c = r.connect(fakeSse(), {});
		if (c.outcome !== "ok") throw new Error("unreachable");

		await r.subscribe(c.uid, "users/me", {});

		expect(seen).toEqual([{ pattern: "users/me" }]);
	});
});

describe("relay-security > a channel name is an SSE event name", () => {
	async function subscribed(channel: string) {
		const r = new Relay();
		r.authorize("rooms/:slug", () => true);
		r.authorize("open/*", () => true);
		const c = r.connect(fakeSse(), { auth: { isAuthenticated: false } });
		if (c.outcome !== "ok") throw new Error("unreachable");
		return r.subscribe(c.uid, channel, {});
	}

	it.each([
		["a CR/LF that would start a new SSE field", "rooms/a\r\nevent: error"],
		["a bare LF", "rooms/a\nb"],
		["a NUL", "rooms/a\u0000"],
		["a tab", "rooms/a\tb"],
		["a DEL", "rooms/a\u007f"],
	])("refuses %s", async (_label, channel) => {
		expect(await subscribed(channel)).toEqual({
			ok: false,
			status: 400,
			code: "E_RELAY_BAD_REQUEST",
		});
	});

	it.each(["connected", "error", "$$relay/ping"])(
		"refuses the protocol's own frame name %s",
		async (reserved) => {
			const r = new Relay();
			r.authorize("*", () => true);
			const c = r.connect(fakeSse(), { auth: { isAuthenticated: false } });
			if (c.outcome !== "ok") throw new Error("unreachable");

			expect(await r.subscribe(c.uid, reserved, {})).toEqual({
				ok: false,
				status: 400,
				code: "E_RELAY_BAD_REQUEST",
			});
		},
	);

	it("still takes an ordinary channel, unicode included", async () => {
		expect(await subscribed("rooms/café-42")).toEqual({ ok: true });
	});

	it.each([
		"rooms\n/:slug",
		"rooms/a\u0000",
		"connected",
		"error",
		"$$relay/ping",
	])("refuses to authorize the pattern %j", (pattern) => {
		const r = new Relay();
		expect(() => r.authorize(pattern, () => true)).toThrow(
			/invalid channel pattern/,
		);
	});
});

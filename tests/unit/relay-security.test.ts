/**
 * Regression tests for the SSE uid-hijack security fixes.
 *
 * Pre-fix: client picked its own uid via `?uid=` query string; subscribe/
 * unsubscribe trusted any caller knowing a uid. An attacker who guessed
 * (or learned) another user's uid could connect, hijack, subscribe to
 * private channels, or disconnect the legit client.
 *
 * Post-fix:
 *   - connect() derives uid from ctx.auth.user.id when authenticated;
 *     server-issues a randomUUID for anonymous clients; rejects hint
 *     mismatches with 'forbidden'.
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
	it("forces uid = ctx.auth.user.id when authenticated, ignores hint when omitted", () => {
		const r = new Relay();
		const outcome = r.connect(undefined, fakeSse(), {
			auth: { isAuthenticated: true, user: { id: "user-42" } },
		});
		expect(outcome).toEqual({ outcome: "ok", uid: "user-42" });
	});

	it("accepts a matching hint and uses the auth-derived uid", () => {
		const r = new Relay();
		const outcome = r.connect("user-42", fakeSse(), {
			auth: { isAuthenticated: true, user: { id: "user-42" } },
		});
		expect(outcome).toEqual({ outcome: "ok", uid: "user-42" });
	});

	it("REJECTS a mismatched hint — authenticated client cannot claim another id", () => {
		const r = new Relay();
		const outcome = r.connect("user-other", fakeSse(), {
			auth: { isAuthenticated: true, user: { id: "user-42" } },
		});
		expect(outcome.outcome).toBe("forbidden");
	});

	it("server-issues a randomUUID for anonymous clients, ignoring the hint", () => {
		const r = new Relay();
		const outcome = r.connect("attacker-picked-uid", fakeSse(), {
			auth: { isAuthenticated: false },
		});
		expect(outcome.outcome).toBe("ok");
		// outcome is narrowed to the 'ok' branch
		if (outcome.outcome !== "ok") throw new Error("unreachable");
		expect(outcome.uid).not.toBe("attacker-picked-uid");
		// UUID v4 shape (8-4-4-4-12 hex segments).
		expect(outcome.uid).toMatch(
			/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i,
		);
	});

	it("issues distinct uids across two anonymous connects (no collision/hint reuse)", () => {
		const r = new Relay();
		const a = r.connect(undefined, fakeSse(), {
			auth: { isAuthenticated: false },
		});
		const b = r.connect(undefined, fakeSse(), {
			auth: { isAuthenticated: false },
		});
		if (a.outcome !== "ok" || b.outcome !== "ok")
			throw new Error("unreachable");
		expect(a.uid).not.toBe(b.uid);
	});

	it("returns 'capped' when the maxClients ceiling is reached by a NEW uid", () => {
		const r = new Relay({ maxClients: 1 });
		const first = r.connect(undefined, fakeSse(), {
			auth: { isAuthenticated: true, user: { id: "u1" } },
		});
		expect(first.outcome).toBe("ok");
		const second = r.connect(undefined, fakeSse(), {
			auth: { isAuthenticated: true, user: { id: "u2" } },
		});
		expect(second).toEqual({ outcome: "capped" });
	});

	it("reconnect by the SAME uid at full capacity replaces the prior writer (not 'capped')", () => {
		const r = new Relay({ maxClients: 1 });
		const first = r.connect(undefined, fakeSse("s-first"), {
			auth: { isAuthenticated: true, user: { id: "u1" } },
		});
		expect(first).toEqual({ outcome: "ok", uid: "u1" });
		// User refresh / network reconnect: same uid, cap is full BUT the slot
		// is already this user's — must not 503.
		const reconnect = r.connect(undefined, fakeSse("s-second"), {
			auth: { isAuthenticated: true, user: { id: "u1" } },
		});
		expect(reconnect).toEqual({ outcome: "ok", uid: "u1" });
		expect(r.clientCount()).toBe(1);
	});

	// Audit 2026-06-13: reconnect dropped the prior client from #clients but never
	// end()-ed its stream → an orphaned writer leaked per refresh. It must be
	// closed, and the stale onClose must NOT drop the new client (count stays 1).
	it("ends the prior SSE stream on reconnect and keeps the new one", () => {
		const r = new Relay({ maxClients: 5 });
		const oldSse = fakeSse("s-old");
		r.connect(undefined, oldSse, {
			auth: { isAuthenticated: true, user: { id: "u1" } },
		});
		expect(oldSse.isOpen()).toBe(true);
		const newSse = fakeSse("s-new");
		r.connect(undefined, newSse, {
			auth: { isAuthenticated: true, user: { id: "u1" } },
		});
		expect(oldSse.isOpen()).toBe(false); // orphaned stream closed (no leak)
		expect(newSse.isOpen()).toBe(true); // new stream survives the stale onClose
		expect(r.clientCount()).toBe(1);
	});
});

describe("relay-security > subscribe() ownership", () => {
	it("rejects subscribe from a request whose auth doesn't match the connected client", async () => {
		const r = new Relay({ allowUnauthorizedChannels: true });
		// Victim connects authenticated as user-42.
		const victim = r.connect(undefined, fakeSse(), {
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
			code: "E_NOT_OWNER",
		});
		// Even an unauthenticated attacker is rejected.
		const anonResult = await r.subscribe(victim.uid, "private/feed", {
			auth: { isAuthenticated: false },
		});
		expect(anonResult).toEqual({
			ok: false,
			status: 403,
			code: "E_NOT_OWNER",
		});
	});

	it("accepts subscribe from the SAME authenticated identity as the connect", async () => {
		const r = new Relay({ allowUnauthorizedChannels: true });
		const owner = r.connect(undefined, fakeSse(), {
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
		const anon = r.connect(undefined, fakeSse(), {
			auth: { isAuthenticated: false },
		});
		if (anon.outcome !== "ok") throw new Error("unreachable");
		// Without an auth identity at connect-time, the server-issued
		// random uid IS the proof of ownership. A requester who has it can
		// subscribe; one who doesn't can't (E_NOT_CONNECTED).
		const result = await r.subscribe(anon.uid, "public/feed", {
			auth: { isAuthenticated: false },
		});
		expect(result).toEqual({ ok: true });
	});

	it("is idempotent: re-subscribing to an already-held channel returns ok (no 429 at quota)", async () => {
		const r = new Relay({
			allowUnauthorizedChannels: true,
			maxChannelsPerClient: 1,
		});
		const owner = r.connect(undefined, fakeSse(), {
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
			code: "E_MAX_CHANNELS",
		});
	});
});

describe("relay-security > unsubscribe() ownership", () => {
	it("rejects unsubscribe from a non-owner authenticated requester", async () => {
		const r = new Relay({ allowUnauthorizedChannels: true });
		const owner = r.connect(undefined, fakeSse(), {
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
		const owner = r.connect(undefined, fakeSse(), {
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

/**
 * Unit suite for Hub — bidirectional WS hub with auth guards, groups,
 * and handler allowlist. Covers registerClient / removeClient / dispatch
 * (auth + role + permission + strategy gates) / groups (join/leave/send)
 * / broadcast / stats and the auto-allowlist of `on*` methods.
 */
import { describe, expect, it } from "vitest";
import { Hub, type HubContext } from "../../src/Hub.js";

interface SentMessage {
	event: string;
	data: unknown;
}

function makeClient(id: string, auth: HubContext["auth"]) {
	const sent: SentMessage[] = [];
	return {
		id,
		groups: new Set<string>(),
		auth,
		send: (event: string, data: unknown) => sent.push({ event, data }),
		sent,
	};
}

class TestHub extends Hub {
	calls: Array<{ event: string; ctx: HubContext; data: unknown }> = [];

	async onPing(ctx: HubContext, data: unknown): Promise<void> {
		this.calls.push({ event: "ping", ctx, data });
		ctx.send("pong", data);
	}

	async onJoinRoom(ctx: HubContext, data: unknown): Promise<void> {
		this.calls.push({ event: "joinRoom", ctx, data });
		const { room } = data as { room: string };
		ctx.joinGroup(room);
	}

	async onBroadcast(ctx: HubContext, data: unknown): Promise<void> {
		this.calls.push({ event: "broadcast", ctx, data });
		ctx.broadcast("msg", data);
	}

	async onBoom(_ctx: HubContext, _data: unknown): Promise<void> {
		throw new Error("intentional");
	}
}

describe("relay > Hub > construction + allowlist", () => {
	it("auto-registers on*-prefixed methods (excluding onConnect/onDisconnect)", () => {
		const hub = new TestHub();
		// Indirectly verifies via dispatch — an unknown event returns UNKNOWN_EVENT.
		const c = makeClient("c1", { isAuthenticated: true });
		hub.registerClient(c);
		return hub.dispatch("c1", "nonsense", {}).then(() => {
			expect(c.sent).toContainEqual({
				event: "error",
				data: { code: "UNKNOWN_EVENT", message: "No handler for: nonsense" },
			});
		});
	});
});

describe("relay > Hub > registerClient / removeClient / stats", () => {
	it("registerClient + stats report client count", () => {
		const hub = new TestHub();
		hub.registerClient(makeClient("a", { isAuthenticated: true }));
		hub.registerClient(makeClient("b", { isAuthenticated: true }));
		expect(hub.stats()).toEqual({ clients: 2, groups: 0 });
	});

	it("removeClient drops a client and its group memberships", async () => {
		const hub = new TestHub();
		const c = makeClient("a", { isAuthenticated: true });
		const ctx = hub.registerClient(c);
		ctx.joinGroup("room:1");
		expect(hub.stats()).toEqual({ clients: 1, groups: 1 });

		hub.removeClient("a");
		expect(hub.stats()).toEqual({ clients: 0, groups: 0 });
	});
});

describe("relay > Hub > dispatch — handler routing", () => {
	it("routes an event to the matching on* handler", async () => {
		const hub = new TestHub();
		const c = makeClient("a", { isAuthenticated: true });
		hub.registerClient(c);
		await hub.dispatch("a", "ping", { x: 1 });
		expect(c.sent).toContainEqual({ event: "pong", data: { x: 1 } });
	});

	it("returns false (handled=no) for an unknown client without throwing", async () => {
		const hub = new TestHub();
		// dispatch() reports outcome via its boolean return so a correlated
		// transport (SignalR) can answer with an error Completion; an unknown
		// client is a non-handled dispatch → false, but never throws.
		await expect(hub.dispatch("ghost", "ping", {})).resolves.toBe(false);
	});

	it("returns UNKNOWN_EVENT for unmapped events", async () => {
		const hub = new TestHub();
		const c = makeClient("a", { isAuthenticated: true });
		hub.registerClient(c);
		await hub.dispatch("a", "unknown", {});
		expect(c.sent[0]?.event).toBe("error");
		expect((c.sent[0]?.data as { code: string }).code).toBe("UNKNOWN_EVENT");
	});

	it("catches handler errors and emits HANDLER_ERROR", async () => {
		const hub = new TestHub();
		const c = makeClient("a", { isAuthenticated: true });
		hub.registerClient(c);
		await hub.dispatch("a", "boom", {});
		expect((c.sent[0]?.data as { code: string }).code).toBe("HANDLER_ERROR");
	});
});

describe("relay > Hub > guards — auth / strategy / roles / permissions", () => {
	it("rejects unauthenticated client when guards require auth", async () => {
		const hub = new TestHub();
		hub.useGuards({ guard: "jwt" });
		const c = makeClient("a", { isAuthenticated: false });
		hub.registerClient(c);
		await hub.dispatch("a", "ping", {});
		expect((c.sent[0]?.data as { code: string }).code).toBe("UNAUTHORIZED");
	});

	it("rejects mismatched auth strategy", async () => {
		const hub = new TestHub();
		hub.useGuards({ guards: ["jwt"] });
		const c = makeClient("a", { isAuthenticated: true, strategy: "api-key" });
		hub.registerClient(c);
		await hub.dispatch("a", "ping", {});
		expect((c.sent[0]?.data as { code: string }).code).toBe("UNAUTHORIZED");
	});

	it("rejects missing required role", async () => {
		const hub = new TestHub();
		hub.useGuards({ roles: ["admin"] });
		const c = makeClient("a", { isAuthenticated: true, roles: ["user"] });
		hub.registerClient(c);
		await hub.dispatch("a", "ping", {});
		expect((c.sent[0]?.data as { code: string }).code).toBe("FORBIDDEN");
	});

	it("rejects missing required permission", async () => {
		const hub = new TestHub();
		hub.useGuards({ permissions: ["tasks:write"] });
		const c = makeClient("a", {
			isAuthenticated: true,
			permissions: ["tasks:read"],
		});
		hub.registerClient(c);
		await hub.dispatch("a", "ping", {});
		expect((c.sent[0]?.data as { code: string }).code).toBe("FORBIDDEN");
	});

	it("admits a client that meets every guard", async () => {
		const hub = new TestHub();
		hub.useGuards({
			guards: ["jwt"],
			roles: ["admin"],
			permissions: ["tasks:write"],
		});
		const c = makeClient("a", {
			isAuthenticated: true,
			strategy: "jwt",
			roles: ["admin"],
			permissions: ["tasks:write", "tasks:read"],
		});
		hub.registerClient(c);
		await hub.dispatch("a", "ping", { ok: true });
		expect(c.sent).toContainEqual({ event: "pong", data: { ok: true } });
	});
});

describe("relay > Hub > groups + broadcast", () => {
	it("ctx.joinGroup adds the client + updates stats", async () => {
		const hub = new TestHub();
		const c = makeClient("a", { isAuthenticated: true });
		hub.registerClient(c);
		await hub.dispatch("a", "joinRoom", { room: "lobby" });
		expect(hub.stats().groups).toBe(1);
	});

	it("ctx.group(name).send delivers to every group member", async () => {
		const hub = new TestHub();
		const c1 = makeClient("a", { isAuthenticated: true });
		const c2 = makeClient("b", { isAuthenticated: true });
		const ctx1 = hub.registerClient(c1);
		const ctx2 = hub.registerClient(c2);
		ctx1.joinGroup("lobby");
		ctx2.joinGroup("lobby");
		ctx1.group("lobby").send("hello", { msg: "hi" });
		expect(c1.sent).toContainEqual({ event: "hello", data: { msg: "hi" } });
		expect(c2.sent).toContainEqual({ event: "hello", data: { msg: "hi" } });
	});

	it("ctx.leaveGroup removes the client + cleans empty groups", async () => {
		const hub = new TestHub();
		const c = makeClient("a", { isAuthenticated: true });
		const ctx = hub.registerClient(c);
		ctx.joinGroup("room");
		ctx.leaveGroup("room");
		expect(hub.stats().groups).toBe(0);
	});

	it("ctx.broadcast sends to every connected client", async () => {
		const hub = new TestHub();
		const c1 = makeClient("a", { isAuthenticated: true });
		const c2 = makeClient("b", { isAuthenticated: true });
		hub.registerClient(c1);
		hub.registerClient(c2);
		await hub.dispatch("a", "broadcast", { ping: true });
		expect(c1.sent).toContainEqual({ event: "msg", data: { ping: true } });
		expect(c2.sent).toContainEqual({ event: "msg", data: { ping: true } });
	});

	it("sendToGroup with no members is a no-op", async () => {
		const hub = new TestHub();
		const c = makeClient("a", { isAuthenticated: true });
		const ctx = hub.registerClient(c);
		ctx.group("ghost-room").send("x", {});
		// No error, no send.
		expect(c.sent).toEqual([]);
	});
});

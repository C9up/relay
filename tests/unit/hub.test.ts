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

/**
 * The error code of the nth message a client received.
 *
 * `(c.sent[0]?.data as { code: string }).code` was the shape here: an optional
 * chain that yields `undefined`, then an assertion claiming it is an object,
 * then a property read on it. The chain does not protect anything — the read
 * throws exactly as it would without it, and the assertion is what hides that
 * from the compiler. Proving the message is there says the same thing and
 * fails on this line when it is not.
 */
function errorCode(sent: SentMessage[], nth = 0): string {
	const message = sent[nth];
	if (message === undefined) throw new Error(`no message #${nth}`);
	const data = message.data;
	if (typeof data !== "object" || data === null) {
		throw new Error(`message #${nth} carried no object`);
	}
	const code = Reflect.get(data, "code");
	if (typeof code !== "string")
		throw new Error(`message #${nth} carried no code`);
	return code;
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
				data: {
					code: "E_RELAY_UNKNOWN_EVENT",
					message: "No handler for: nonsense",
				},
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
		expect(errorCode(c.sent, 0)).toBe("E_RELAY_UNKNOWN_EVENT");
	});

	it("catches handler errors and emits HANDLER_ERROR", async () => {
		const hub = new TestHub();
		const c = makeClient("a", { isAuthenticated: true });
		hub.registerClient(c);
		await hub.dispatch("a", "boom", {});
		expect(errorCode(c.sent, 0)).toBe("E_RELAY_HANDLER_ERROR");
	});
});

describe("relay > Hub > guards — auth / strategy / roles / permissions", () => {
	it("rejects unauthenticated client when guards require auth", async () => {
		const hub = new TestHub();
		hub.useGuards({ guard: "jwt" });
		const c = makeClient("a", { isAuthenticated: false });
		hub.registerClient(c);
		await hub.dispatch("a", "ping", {});
		expect(errorCode(c.sent, 0)).toBe("E_RELAY_UNAUTHORIZED");
	});

	it("rejects mismatched auth strategy", async () => {
		const hub = new TestHub();
		hub.useGuards({ guards: ["jwt"] });
		const c = makeClient("a", { isAuthenticated: true, strategy: "api-key" });
		hub.registerClient(c);
		await hub.dispatch("a", "ping", {});
		expect(errorCode(c.sent, 0)).toBe("E_RELAY_UNAUTHORIZED");
	});

	it("rejects missing required role", async () => {
		const hub = new TestHub();
		hub.useGuards({ roles: ["admin"] });
		const c = makeClient("a", { isAuthenticated: true, roles: ["user"] });
		hub.registerClient(c);
		await hub.dispatch("a", "ping", {});
		expect(errorCode(c.sent, 0)).toBe("E_RELAY_FORBIDDEN");
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
		expect(errorCode(c.sent, 0)).toBe("E_RELAY_FORBIDDEN");
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

describe("Hub > handlers a subclass inherits", () => {
	class BaseHub extends Hub {
		ran: string[] = [];
		async onPing(): Promise<void> {
			this.ran.push("base:ping");
		}
		async onGreet(): Promise<void> {
			this.ran.push("base:greet");
		}
	}
	class ChildHub extends BaseHub {
		async onPong(): Promise<void> {
			this.ran.push("child:pong");
		}
		override async onGreet(): Promise<void> {
			this.ran.push("child:greet");
		}
	}

	function connected(hub: Hub): Array<{ event: string; data: unknown }> {
		const sent: Array<{ event: string; data: unknown }> = [];
		hub.registerClient({
			id: "c1",
			groups: new Set(),
			auth: { isAuthenticated: true },
			send: (event, data) => {
				sent.push({ event, data });
			},
		});
		return sent;
	}

	// Reading one prototype level answered these with E_RELAY_UNKNOWN_EVENT:
	// a handler that exists, is spelled right, and never runs.
	it("dispatches to a handler declared on a base class", async () => {
		const hub = new ChildHub();
		const sent = connected(hub);

		expect(await hub.dispatch("c1", "ping")).toBe(true);
		expect(hub.ran).toEqual(["base:ping"]);
		expect(sent).toEqual([]);
	});

	it("lets the subclass win, as method resolution already does", async () => {
		const hub = new ChildHub();
		connected(hub);

		await hub.dispatch("c1", "greet");

		expect(hub.ran).toEqual(["child:greet"]);
	});

	// The point of the allowlist: the walk stops at Hub.prototype, so nothing
	// on Hub or Object is reachable from the wire.
	it("still refuses everything the base class Hub itself declares", async () => {
		const hub = new ChildHub();
		const sent = connected(hub);

		expect(await hub.dispatch("c1", "connect")).toBe(false);
		expect(await hub.dispatch("c1", "disconnect")).toBe(false);
		expect(sent.map((s) => s.event)).toEqual(["error", "error"]);
	});
});

describe("Hub > a client id that connects twice", () => {
	class QuietHub extends Hub {}

	it("does not leave the first connection's groups on the second", () => {
		const hub = new QuietHub();
		const first = hub.registerClient({
			id: "c1",
			groups: new Set(),
			auth: { isAuthenticated: true, user: { id: "alice" } },
			send: () => {},
		});
		first.joinGroup("secret");

		const received: unknown[] = [];
		hub.registerClient({
			id: "c1",
			groups: new Set(),
			auth: { isAuthenticated: true, user: { id: "alice" } },
			send: (_event, data) => {
				received.push(data);
			},
		});
		first.group("secret").send("leak", { from: "the old membership" });

		// The new connection never joined that group and was never checked for it.
		expect(received).toEqual([]);
	});

	it("leaves no group behind once the client is removed", () => {
		const hub = new QuietHub();
		hub
			.registerClient({
				id: "c1",
				groups: new Set(),
				auth: { isAuthenticated: true },
				send: () => {},
			})
			.joinGroup("room");
		hub.registerClient({
			id: "c1",
			groups: new Set(),
			auth: { isAuthenticated: true },
			send: () => {},
		});

		hub.removeClient("c1");

		// `removeClient` walks the client's own group set, so a membership left
		// on a client it no longer holds could never be cleaned up again.
		expect(hub.stats()).toEqual({ clients: 0, groups: 0 });
	});
});

describe("Hub > the singular guard reaches the check", () => {
	class GuardedHub extends Hub {
		ran = 0;
		async onPing(_ctx: HubContext): Promise<void> {
			this.ran++;
		}
	}

	function connect(
		hub: Hub,
		auth: HubContext["auth"],
	): { sent: Array<{ event: string; data: unknown }> } {
		const sent: Array<{ event: string; data: unknown }> = [];
		hub.registerClient({
			id: "c1",
			groups: new Set(),
			auth,
			send: (event, data) => {
				sent.push({ event, data });
			},
		});
		return { sent };
	}

	it("refuses a strategy that does not match `guard`", async () => {
		const hub = new GuardedHub();
		hub.useGuards({ guard: "session" });
		const { sent } = connect(hub, { isAuthenticated: true, strategy: "jwt" });

		// `useGuards` folds the singular into `guards`, and the check reads that
		// one list. It used to read a `guard` key nothing ever wrote as well.
		expect(await hub.dispatch("c1", "ping")).toBe(false);
		expect(sent[0]?.data).toMatchObject({ code: "E_RELAY_UNAUTHORIZED" });
		expect(hub.ran).toBe(0);
	});

	it("accepts the strategy `guard` names", async () => {
		const hub = new GuardedHub();
		hub.useGuards({ guard: "session" });
		connect(hub, { isAuthenticated: true, strategy: "session" });

		expect(await hub.dispatch("c1", "ping")).toBe(true);
		expect(hub.ran).toBe(1);
	});

	it("accepts either of `guard` and `guards` together", async () => {
		const hub = new GuardedHub();
		hub.useGuards({ guard: "session", guards: ["jwt"] });
		connect(hub, { isAuthenticated: true, strategy: "jwt" });

		expect(await hub.dispatch("c1", "ping")).toBe(true);
	});

	it("needs every permission and only one of the roles", async () => {
		const hub = new GuardedHub();
		hub.useGuards({
			roles: ["admin", "owner"],
			permissions: ["read", "write"],
		});
		const { sent } = connect(hub, {
			isAuthenticated: true,
			roles: ["owner"],
			permissions: ["read"],
		});

		// Roles are any-of, permissions are all-of — the rule every other entry
		// point in the framework applies.
		expect(await hub.dispatch("c1", "ping")).toBe(false);
		expect(sent[0]?.data).toMatchObject({
			message: "Insufficient permissions",
		});
	});
});

/**
 * The hub over a WebSocket: negotiate announces it only when the server can
 * upgrade, and the socket route admits, authorizes, frames and releases a
 * connection exactly as the SSE stream does — driven here against a fake
 * router and a fake socket.
 */

import { afterEach, describe, expect, it, vi } from "vitest";
import { Hub, type HubContext } from "../../src/Hub.js";
import { SignalRAdapter } from "../../src/SignalRAdapter.js";
import {
	type HubHttpContext,
	type HubRouteOptions,
	type HubSocket,
	registerHubRoutes,
} from "../../src/SignalRTransport.js";

const RS = "\x1e";
const HANDSHAKE = `${JSON.stringify({ protocol: "json", version: 1 })}${RS}`;

class RoomHub extends Hub {
	readonly seen: string[] = [];
	connected = 0;
	readonly disconnected: string[] = [];

	override async onConnect(ctx: HubContext): Promise<void> {
		this.connected += 1;
		ctx.send("welcome", { id: ctx.clientId });
	}

	override async onDisconnect(clientId: string): Promise<void> {
		this.disconnected.push(clientId);
	}

	async onJoin(ctx: HubContext, room: string): Promise<void> {
		ctx.joinGroup(`room:${room}`);
	}

	async onSay(ctx: HubContext, room: string, text: string): Promise<void> {
		this.seen.push(text);
		ctx.group(`room:${room}`).send("said", { text });
	}
}

/** A socket that records what the server wrote and lets a test speak. */
function fakeSocket() {
	const sent: string[] = [];
	const closes: Array<{ code?: number; reason?: string }> = [];
	const messageListeners: Array<
		(data: string | Uint8Array, isBinary: boolean) => void
	> = [];
	const closeListeners: Array<(code: number, reason: string) => void> = [];
	let state: HubSocket["readyState"] = "open";
	const socket: HubSocket = {
		get readyState() {
			return state;
		},
		send(data) {
			if (state !== "open") return false;
			sent.push(data);
			return true;
		},
		close(code, reason) {
			if (state !== "open") return;
			state = "closing";
			closes.push({ code, reason });
		},
		on(
			event: "message" | "close",
			listener:
				| ((data: string | Uint8Array, isBinary: boolean) => void)
				| ((code: number, reason: string) => void),
		) {
			if (event === "message" && isMessageListener(listener)) {
				messageListeners.push(listener);
			}
			if (event === "close" && isCloseListener(listener)) {
				closeListeners.push(listener);
			}
			return socket;
		},
	};
	return {
		socket,
		sent,
		closes,
		/** The client sends a text message; resolves once the hub has answered. */
		async say(text: string) {
			for (const listener of messageListeners) listener(text, false);
			await settle();
		},
		async sayBinary(bytes: Uint8Array) {
			for (const listener of messageListeners) listener(bytes, true);
			await settle();
		},
		/** The connection ends, whoever closed it. */
		ended(code = 1000) {
			state = "closed";
			for (const listener of closeListeners) listener(code, "");
		},
	};
}

function isMessageListener(
	value: unknown,
): value is (data: string | Uint8Array, isBinary: boolean) => void {
	return typeof value === "function";
}
function isCloseListener(
	value: unknown,
): value is (code: number, reason: string) => void {
	return typeof value === "function";
}

/** Let the hub's message queue run. */
async function settle(): Promise<void> {
	for (let i = 0; i < 20; i++) await Promise.resolve();
	await new Promise((resolve) => setImmediate(resolve));
}

type WsHandler = (
	ctx: HubHttpContext & { socket: HubSocket },
) => Promise<void> | void;

function fakeRouter(withWs: boolean) {
	const routes = new Map<string, (ctx: HubHttpContext) => Promise<void>>();
	const sockets = new Map<string, WsHandler>();
	const router = {
		routes,
		sockets,
		get(path: string, handler: (ctx: HubHttpContext) => Promise<void>) {
			routes.set(`GET ${path}`, handler);
			return { path };
		},
		post(path: string, handler: (ctx: HubHttpContext) => Promise<void>) {
			routes.set(`POST ${path}`, handler);
			return { path };
		},
		...(withWs
			? {
					ws(path: string, handler: WsHandler) {
						sockets.set(path, handler);
						return { path, websocket: true };
					},
				}
			: {}),
	};
	return router;
}

interface Answer {
	code: number;
	body: unknown;
}

function context(
	url: string,
	user: string | undefined = "u1",
): HubHttpContext & Answer {
	const ctx: HubHttpContext & Answer = {
		code: 200,
		body: undefined,
		request: {
			url: (includeQueryString?: boolean) =>
				includeQueryString === true ? url : (url.split("?")[0] ?? url),
			raw: () => "",
			ip: () => "203.0.113.7",
		},
		response: {
			status(code: number) {
				ctx.code = code;
				return {
					json(body: unknown) {
						ctx.body = body;
					},
				};
			},
			json(body: unknown) {
				ctx.body = body;
			},
			sse: () => Promise.reject(new Error("no SSE in this test")),
		},
		auth:
			user === undefined
				? { isAuthenticated: false }
				: { isAuthenticated: true, user: { id: user } },
	};
	return ctx;
}

function field(value: unknown, name: string): unknown {
	return typeof value === "object" && value !== null
		? Reflect.get(value, name)
		: undefined;
}

function mount(options: HubRouteOptions & { withWs?: boolean } = {}) {
	const hub = new RoomHub();
	const router = fakeRouter(options.withWs ?? true);
	const adapter = new SignalRAdapter(hub, { handshakeTimeoutMs: 50 });
	const routes = registerHubRoutes(
		router,
		{ path: "/hubs/rooms", hub, adapter },
		{ webSockets: options.webSockets ?? (() => true) },
	);
	return { hub, router, adapter, routes };
}

async function negotiate(
	router: ReturnType<typeof fakeRouter>,
	user?: string,
): Promise<{ token: string; transports: unknown }> {
	const ctx = context("/hubs/rooms/negotiate", user);
	await router.routes.get("POST /hubs/rooms/negotiate")?.(ctx);
	const token = field(ctx.body, "connectionToken");
	if (typeof token !== "string") throw new Error("negotiate gave no token");
	return { token, transports: field(ctx.body, "availableTransports") };
}

/** Open the socket route for `token`, as `user`. */
async function openSocket(
	router: ReturnType<typeof fakeRouter>,
	token: string,
	user?: string,
) {
	const handler = router.sockets.get("/hubs/rooms");
	if (handler === undefined) throw new Error("no WebSocket route");
	const ws = fakeSocket();
	const ctx = Object.assign(context(`/hubs/rooms?id=${token}`, user), {
		socket: ws.socket,
	});
	await handler(ctx);
	return { ws, ctx };
}

afterEach(() => {
	vi.useRealTimers();
});

describe("relay > SignalR over WebSockets > negotiate", () => {
	it("announces WebSockets first when the server can upgrade", async () => {
		const { router } = mount();
		const { transports } = await negotiate(router);
		expect(transports).toEqual([
			{ transport: "WebSockets", transferFormats: ["Text"] },
			{ transport: "ServerSentEvents", transferFormats: ["Text"] },
		]);
	});

	it("announces Server-Sent Events only when the server cannot upgrade", async () => {
		let upgrades = false;
		const { router } = mount({ webSockets: () => upgrades });
		expect((await negotiate(router)).transports).toEqual([
			{ transport: "ServerSentEvents", transferFormats: ["Text"] },
		]);
		// Asked on every negotiate: the server starts after the hub is declared.
		upgrades = true;
		expect((await negotiate(router)).transports).toEqual([
			{ transport: "WebSockets", transferFormats: ["Text"] },
			{ transport: "ServerSentEvents", transferFormats: ["Text"] },
		]);
	});

	it("announces Server-Sent Events only on a router without WebSocket routes", async () => {
		const { router, routes } = mount({ withWs: false });
		expect(routes.socket).toBeUndefined();
		expect((await negotiate(router)).transports).toEqual([
			{ transport: "ServerSentEvents", transferFormats: ["Text"] },
		]);
	});
});

describe("relay > SignalR over WebSockets > the socket", () => {
	it("answers the handshake, then lets the hub speak, then carries invocations both ways", async () => {
		const { hub, router } = mount();
		const { token } = await negotiate(router);
		const { ws, ctx } = await openSocket(router, token);
		expect(ctx.code).toBe(200);
		expect(hub.connected).toBe(0);
		expect(ws.sent).toEqual([]);

		await ws.say(HANDSHAKE);
		expect(ws.sent[0]).toBe(`{}${RS}`);
		expect(ws.sent[1]).toContain('"welcome"');
		expect(hub.connected).toBe(1);

		await ws.say(
			`${JSON.stringify({ type: 1, target: "join", arguments: ["a"] })}${RS}`,
		);
		await ws.say(
			`${JSON.stringify({ type: 1, target: "say", arguments: ["a", "bonjour"] })}${RS}`,
		);
		expect(hub.seen).toEqual(["bonjour"]);
		expect(ws.sent.some((frame) => frame.includes('"said"'))).toBe(true);
	});

	it("handles a handshake and an invocation sent together, in order", async () => {
		const { hub, router } = mount();
		const { token } = await negotiate(router);
		const { ws } = await openSocket(router, token);
		await ws.say(
			`${HANDSHAKE}${JSON.stringify({ type: 1, target: "join", arguments: ["b"] })}${RS}`,
		);
		expect(ws.sent[0]).toBe(`{}${RS}`);
		expect(hub.connected).toBe(1);
	});

	it("delivers a group send to every socket in the group", async () => {
		const { router } = mount();
		const first = await negotiate(router, "u1");
		const second = await negotiate(router, "u2");
		const a = await openSocket(router, first.token, "u1");
		const b = await openSocket(router, second.token, "u2");
		for (const peer of [a.ws, b.ws]) {
			await peer.say(HANDSHAKE);
			await peer.say(
				`${JSON.stringify({ type: 1, target: "join", arguments: ["c"] })}${RS}`,
			);
		}
		await a.ws.say(
			`${JSON.stringify({ type: 1, target: "say", arguments: ["c", "hi"] })}${RS}`,
		);
		expect(b.ws.sent.some((frame) => frame.includes('"said"'))).toBe(true);
	});

	it("refuses a socket with no negotiated token before it opens", async () => {
		const { hub, router } = mount();
		const { ws, ctx } = await openSocket(router, "not-a-token");
		expect(ctx.code).toBe(400);
		expect(field(field(ctx.body, "error"), "code")).toBe(
			"E_RELAY_UNKNOWN_CONNECTION",
		);
		await ws.say(HANDSHAKE);
		expect(ws.sent).toEqual([]);
		expect(hub.stats().clients).toBe(0);
	});

	it("refuses a socket opened on someone else's token", async () => {
		const { hub, router } = mount();
		const { token } = await negotiate(router, "u1");
		await openSocket(router, token, "u1");
		const { ctx } = await openSocket(router, token, "mallory");
		expect(ctx.code).toBe(403);
		expect(hub.stats().clients).toBe(1);
	});

	it("counts sockets against the per-owner cap, and gives the slot back on close", async () => {
		const hub = new RoomHub();
		const router = fakeRouter(true);
		const adapter = new SignalRAdapter(hub, { maxConnectionsPerOwner: 1 });
		registerHubRoutes(
			router,
			{ path: "/hubs/rooms", hub, adapter },
			{ webSockets: () => true },
		);
		const first = await negotiate(router);
		const second = await negotiate(router);
		const open = await openSocket(router, first.token);
		const refused = await openSocket(router, second.token);
		expect(refused.ctx.code).toBe(503);
		open.ws.ended();
		const retried = await openSocket(router, second.token);
		expect(retried.ctx.code).toBe(200);
	});

	it("refuses a binary message with 1003: the JSON protocol is text", async () => {
		const { router } = mount();
		const { token } = await negotiate(router);
		const { ws } = await openSocket(router, token);
		await ws.sayBinary(new Uint8Array([1, 2, 3]));
		expect(ws.closes).toEqual([{ code: 1003, reason: "text frames only" }]);
	});

	it("closes the socket when the client sends a Close, and releases the client", async () => {
		const { hub, router } = mount();
		const { token } = await negotiate(router);
		const { ws } = await openSocket(router, token);
		await ws.say(HANDSHAKE);
		await ws.say(`${JSON.stringify({ type: 7 })}${RS}`);
		expect(ws.closes).toEqual([{ code: 1000, reason: undefined }]);
		ws.ended(1000);
		expect(hub.stats().clients).toBe(0);
		expect(hub.disconnected).toHaveLength(1);
	});

	it("closes a socket that never completes the handshake", async () => {
		vi.useFakeTimers();
		const { router } = mount();
		const { token } = await negotiate(router);
		const { ws } = await openSocket(router, token);
		vi.advanceTimersByTime(60);
		expect(ws.closes).toEqual([{ code: 1000, reason: undefined }]);
	});

	it("closes every socket with 1001 on shutdown, and refuses new ones until reopened", async () => {
		const { hub, router, routes } = mount();
		const { token } = await negotiate(router);
		const { ws } = await openSocket(router, token);
		await routes.close();
		expect(ws.closes).toEqual([{ code: 1001, reason: undefined }]);
		expect(hub.stats().clients).toBe(0);
		const refused = await openSocket(router, token);
		expect(refused.ctx.code).toBe(503);
		routes.reopen();
		const again = await negotiate(router);
		expect((await openSocket(router, again.token)).ctx.code).toBe(200);
	});

	it("does not let a replaced socket's close tear down the one that replaced it", async () => {
		const { hub, router } = mount();
		const { token } = await negotiate(router);
		const old = await openSocket(router, token);
		const fresh = await openSocket(router, token);
		// The old socket was ended when the same token opened again.
		expect(old.ws.closes).toEqual([{ code: 1000, reason: undefined }]);
		old.ws.ended();
		expect(hub.stats().clients).toBe(1);
		await fresh.ws.say(HANDSHAKE);
		expect(fresh.ws.sent[0]).toBe(`{}${RS}`);
	});
});

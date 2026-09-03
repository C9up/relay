/**
 * The transport that makes a Hub reachable.
 *
 * `Hub` and `SignalRAdapter` were exported and could not run on anything: the
 * adapter parses frames but owns no socket, and nothing registered a negotiate
 * endpoint, a downstream channel or an upstream route. This drives the three
 * routes end to end against a fake router and a fake SSE stream.
 */

import { describe, expect, it } from "vitest";
import { Hub, type HubContext } from "../../src/Hub.js";
import { SignalRAdapter } from "../../src/SignalRAdapter.js";
import {
	type HubHttpContext,
	type HubSseStream,
	registerHubRoutes,
	splitFrames,
} from "../../src/SignalRTransport.js";

const RS = "\x1e";

class ChatHub extends Hub {
	readonly seen: string[] = [];
	connected = 0;
	disconnected: string[] = [];

	override async onConnect(ctx: HubContext): Promise<void> {
		this.connected += 1;
		ctx.send("welcome", { id: ctx.clientId });
	}

	override async onDisconnect(clientId: string): Promise<void> {
		this.disconnected.push(clientId);
	}

	async onSendMessage(ctx: HubContext, data: unknown): Promise<void> {
		this.seen.push(JSON.stringify(data));
		ctx.send("echo", data);
	}
}

/** A router that records handlers so a test can call them directly. */
function fakeRouter() {
	const routes = new Map<string, (ctx: HubHttpContext) => Promise<void>>();
	return {
		routes,
		get(path: string, handler: (ctx: HubHttpContext) => Promise<void>) {
			routes.set(`GET ${path}`, handler);
			return { path };
		},
		post(path: string, handler: (ctx: HubHttpContext) => Promise<void>) {
			routes.set(`POST ${path}`, handler);
			return { path };
		},
	};
}

/** An SSE stream that records what was written. */
function fakeSse(): HubSseStream & {
	frames: string[];
	closers: Array<() => void>;
} {
	const frames: string[] = [];
	const closers: Array<() => void> = [];
	let open = true;
	return {
		id: "sse-1",
		frames,
		closers,
		isOpen: () => open,
		async send(_event, data) {
			frames.push(String(data));
			return true;
		},
		onClose(cb) {
			closers.push(cb);
		},
		async end() {
			open = false;
			for (const cb of closers) cb();
		},
	};
}

function context(
	url: string,
	raw = "",
): HubHttpContext & {
	body: unknown;
	code: number;
	sse: ReturnType<typeof fakeSse>;
} {
	const sse = fakeSse();
	const ctx = {
		body: undefined as unknown,
		code: 200,
		sse,
		request: { url: () => url, raw: () => raw },
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
			async sse() {
				return sse;
			},
		},
		auth: { isAuthenticated: true, user: { id: "u1" } },
	};
	return ctx;
}

function mount(hub: ChatHub) {
	const router = fakeRouter();
	const adapter = new SignalRAdapter(hub);
	registerHubRoutes(router, { path: "/hubs/chat", hub, adapter });
	return { router, adapter };
}

const HANDSHAKE = `${JSON.stringify({ protocol: "json", version: 1 })}${RS}`;

async function connectedClient(hub: ChatHub) {
	const { router, adapter } = mount(hub);
	const neg = context("/hubs/chat/negotiate");
	await router.routes.get("POST /hubs/chat/negotiate")?.(neg);
	const { connectionToken } = neg.body as { connectionToken: string };

	const stream = context(`/hubs/chat?id=${connectionToken}`);
	await router.routes.get("GET /hubs/chat")?.(stream);
	return { router, adapter, connectionToken, stream };
}

describe("relay > SignalR transport", () => {
	it("negotiates a connection token", async () => {
		const { router } = mount(new ChatHub());
		const ctx = context("/hubs/chat/negotiate");

		await router.routes.get("POST /hubs/chat/negotiate")?.(ctx);

		const body = ctx.body as { connectionId: string; connectionToken: string };
		expect(body.connectionId).toBeTruthy();
		expect(body.connectionToken).toBeTruthy();
		expect(body.connectionToken).not.toBe(body.connectionId);
	});

	it("opens the stream and registers the client on the hub", async () => {
		const hub = new ChatHub();
		const { stream } = await connectedClient(hub);

		expect(hub.connected).toBe(1);
		// onConnect pushed a frame down the stream, record-separated.
		expect(stream.sse.frames).toHaveLength(1);
		expect(stream.sse.frames[0]?.endsWith(RS)).toBe(true);
		expect(stream.sse.frames[0]).toContain("welcome");
	});

	it("refuses a stream with no negotiated token, before upgrading", async () => {
		const { router } = mount(new ChatHub());
		const ctx = context("/hubs/chat?id=not-a-token");

		await router.routes.get("GET /hubs/chat")?.(ctx);

		expect(ctx.code).toBe(400);
		expect(ctx.sse.frames).toHaveLength(0);
	});

	it("carries an invocation up and the answer back down", async () => {
		const hub = new ChatHub();
		const { router, connectionToken, stream } = await connectedClient(hub);
		const invocation =
			JSON.stringify({
				type: 1,
				target: "sendMessage",
				arguments: [{ text: "bonjour" }],
			}) + RS;

		const post = context(
			`/hubs/chat?id=${connectionToken}`,
			HANDSHAKE + invocation,
		);
		await router.routes.get("POST /hubs/chat")?.(post);

		expect(post.code).toBe(200);
		expect(hub.seen).toEqual(['{"text":"bonjour"}']);
		// welcome + handshake ack + the echo the handler pushed
		expect(stream.sse.frames.join("")).toContain("echo");
	});

	it("drops the client when the stream closes", async () => {
		const hub = new ChatHub();
		const { stream } = await connectedClient(hub);

		await stream.sse.end();

		expect(hub.disconnected).toHaveLength(1);
		expect(hub.stats().clients).toBe(0);
	});

	it("refuses an upstream post with no negotiated token", async () => {
		const { router } = mount(new ChatHub());
		const ctx = context("/hubs/chat", HANDSHAKE);

		await router.routes.get("POST /hubs/chat")?.(ctx);

		expect(ctx.code).toBe(400);
	});
});

describe("relay > splitFrames", () => {
	it("splits a batch on the record separator, keeping it", () => {
		expect(splitFrames(`{"a":1}${RS}{"b":2}${RS}`)).toEqual([
			`{"a":1}${RS}`,
			`{"b":2}${RS}`,
		]);
	});

	it("treats a separator-less body as one frame rather than dropping it", () => {
		// The adapter reports the protocol error better than silence would.
		expect(splitFrames('{"a":1}')).toEqual([`{"a":1}${RS}`]);
	});

	it("returns nothing for an empty body", () => {
		expect(splitFrames("")).toEqual([]);
	});
});

describe("relay > SignalR transport ownership", () => {
	/**
	 * The connectionToken travels in a query string — SignalR's own design — so
	 * it reaches access logs, proxies and `Referer` headers. The frames a POST
	 * carries are dispatched with the auth recorded when the STREAM opened, so
	 * holding the token was an invocation as somebody else.
	 */
	it("refuses an upstream POST from another user", async () => {
		const hub = new ChatHub();
		const { router, connectionToken } = await connectedClient(hub);

		const attacker = context(`/hubs/chat?id=${connectionToken}`, HANDSHAKE);
		attacker.auth = { isAuthenticated: true, user: { id: "mallory" } };
		await router.routes.get("POST /hubs/chat")?.(attacker);

		expect(attacker.code).toBe(403);
		expect(attacker.body).toEqual({
			error: {
				code: "E_NOT_OWNER",
				message: "This connection belongs to another user.",
			},
		});
		expect(hub.seen).toEqual([]);
	});

	it("refuses a stream opened on someone else's token", async () => {
		const hub = new ChatHub();
		const { router, connectionToken } = await connectedClient(hub);

		const attacker = context(`/hubs/chat?id=${connectionToken}`);
		attacker.auth = { isAuthenticated: true, user: { id: "mallory" } };
		await router.routes.get("GET /hubs/chat")?.(attacker);

		expect(attacker.code).toBe(403);
		expect(attacker.sse.frames).toEqual([]);
	});

	it("lets the owner keep talking on their own connection", async () => {
		const hub = new ChatHub();
		const { router, connectionToken } = await connectedClient(hub);

		const own = context(
			`/hubs/chat?id=${connectionToken}`,
			`${HANDSHAKE}${JSON.stringify({
				type: 1,
				target: "sendMessage",
				arguments: [{ text: "hi" }],
			})}${RS}`,
		);
		await router.routes.get("POST /hubs/chat")?.(own);

		expect(own.code).toBe(200);
		expect(hub.seen).toEqual([JSON.stringify({ text: "hi" })]);
	});
});

describe("relay > SignalR transport connection lifecycle", () => {
	it("does not let a stale stream's close tear down the one that replaced it", async () => {
		const hub = new ChatHub();
		const { router, connectionToken, stream } = await connectedClient(hub);

		// A reconnect that opens before the old socket finished closing.
		const reconnect = context(`/hubs/chat?id=${connectionToken}`);
		await router.routes.get("GET /hubs/chat")?.(reconnect);
		await stream.sse.end();

		// Unguarded, the stale close unregistered the live client and dropped
		// its token, leaving an open socket that could receive nothing.
		expect(hub.stats().clients).toBe(1);
		expect(reconnect.sse.isOpen()).toBe(true);
	});

	/**
	 * A Close from the client is the client saying it is done. The adapter
	 * answers it with nothing — correctly; the protocol asks for no reply — so
	 * watching only the outbound frames left the stream open until the socket
	 * happened to drop.
	 */
	it("closes the stream when the client sends a Close", async () => {
		const hub = new ChatHub();
		const { router, connectionToken, stream } = await connectedClient(hub);

		const bye = context(
			`/hubs/chat?id=${connectionToken}`,
			`${JSON.stringify({ type: 7 })}${RS}`,
		);
		await router.routes.get("POST /hubs/chat")?.(bye);

		expect(stream.sse.isOpen()).toBe(false);
		expect(hub.stats().clients).toBe(0);
		expect(hub.disconnected).toHaveLength(1);
	});
});

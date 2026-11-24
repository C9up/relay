/**
 * What a connection has sent and the hub has not started is bounded.
 *
 * A connection's messages run one after another, so a client that sends
 * faster than a hub method answers builds a backlog on the server. It used to
 * be a promise chain with no end: every message stayed in memory until all
 * the earlier ones had been answered, and one message could pack thousands
 * of invocations. The sizes here are small on purpose: they prove the bound,
 * not the attack.
 */

import { describe, expect, it } from "vitest";
import { Hub, type HubContext } from "../../src/Hub.js";
import { SignalRAdapter } from "../../src/SignalRAdapter.js";
import {
	type HubHttpContext,
	type HubSocket,
	type HubSseStream,
	registerHubRoutes,
} from "../../src/SignalRTransport.js";

const RS = "\x1e";
const HANDSHAKE = `${JSON.stringify({ protocol: "json", version: 1 })}${RS}`;

/** A hub whose `work` waits until the test lets it go. */
class WorkHub extends Hub {
	readonly started: string[] = [];
	readonly finished: string[] = [];
	readonly left: string[] = [];
	failOnConnect = false;
	#gate: Promise<void> = Promise.resolve();
	#open: () => void = () => {};

	/** Make every `work` from now on wait for {@link letGo}. */
	hold(): void {
		this.#gate = new Promise<void>((resolve) => {
			this.#open = resolve;
		});
	}

	letGo(): void {
		this.#open();
	}

	override async onConnect(): Promise<void> {
		if (this.failOnConnect) throw new Error("onConnect failed");
	}

	override async onDisconnect(clientId: string): Promise<void> {
		this.left.push(clientId);
	}

	async onWork(_ctx: HubContext, label: string): Promise<string> {
		this.started.push(label);
		await this.#gate;
		this.finished.push(label);
		return label;
	}
}

function work(label: string, id?: string): string {
	return `${JSON.stringify({
		type: 1,
		...(id === undefined ? {} : { invocationId: id }),
		target: "work",
		arguments: [label],
	})}${RS}`;
}

type Handler = (ctx: HubHttpContext) => Promise<void> | void;
type WsHandler = (
	ctx: HubHttpContext & { socket: HubSocket },
) => Promise<void> | void;

function fakeSocket() {
	const sent: string[] = [];
	const closes: Array<{ code?: number; reason?: string }> = [];
	const onMessage: Array<(data: string | Uint8Array, binary: boolean) => void> =
		[];
	const onClose: Array<(code: number, reason: string) => void> = [];
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
				| ((data: string | Uint8Array, binary: boolean) => void)
				| ((code: number, reason: string) => void),
		) {
			if (event === "message" && isMessageListener(listener)) {
				onMessage.push(listener);
			}
			if (event === "close" && isCloseListener(listener)) {
				onClose.push(listener);
			}
			return socket;
		},
	};
	return {
		socket,
		sent,
		closes,
		/** The client sends a text message; the hub is left to run. */
		async say(text: string) {
			for (const listener of onMessage) listener(text, false);
			await settle();
		},
		/** The connection is gone, whoever closed it. */
		ended(code = 1000) {
			state = "closed";
			for (const listener of onClose) listener(code, "");
		},
	};
}

function isMessageListener(
	value: unknown,
): value is (data: string | Uint8Array, binary: boolean) => void {
	return typeof value === "function";
}
function isCloseListener(
	value: unknown,
): value is (code: number, reason: string) => void {
	return typeof value === "function";
}

/** Let the hub's queue run as far as it can. */
async function settle(): Promise<void> {
	for (let i = 0; i < 20; i++) await Promise.resolve();
	await new Promise((resolve) => setImmediate(resolve));
}

function fakeSse(): HubSseStream & { frames: string[] } {
	const frames: string[] = [];
	const closers: Array<() => void> = [];
	let open = true;
	return {
		id: "s",
		frames,
		isOpen: () => open,
		send: async (_event, data) => {
			if (open) frames.push(String(data));
			return open;
		},
		onClose: (cb) => {
			closers.push(cb);
		},
		end: async () => {
			if (!open) return;
			open = false;
			for (const cb of closers) cb();
		},
	};
}

interface Answer {
	code: number;
	body: unknown;
}

function context(
	url: string,
	raw = "",
	sse: HubSseStream = fakeSse(),
): HubHttpContext & Answer {
	const ctx: HubHttpContext & Answer = {
		code: 200,
		body: undefined,
		request: {
			url: (qs?: boolean) => (qs === true ? url : (url.split("?")[0] ?? url)),
			raw: () => raw,
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
			sse: async () => sse,
		},
		auth: { isAuthenticated: false },
	};
	return ctx;
}

function mount(maxQueuedMessages: number) {
	const hub = new WorkHub();
	const routes = new Map<string, Handler>();
	let ws: WsHandler | undefined;
	const adapter = new SignalRAdapter(hub, { maxQueuedMessages });
	registerHubRoutes(
		{
			get(path: string, handler: Handler) {
				routes.set(`GET ${path}`, handler);
				return {};
			},
			post(path: string, handler: Handler) {
				routes.set(`POST ${path}`, handler);
				return {};
			},
			ws(_path: string, handler: WsHandler) {
				ws = handler;
				return {};
			},
		},
		{ path: "/hubs/work", hub, adapter },
		{ webSockets: () => true },
	);

	async function token(): Promise<string> {
		const ctx = context("/hubs/work/negotiate");
		await routes.get("POST /hubs/work/negotiate")?.(ctx);
		const value: unknown = Reflect.get(Object(ctx.body), "connectionToken");
		if (typeof value !== "string") throw new Error("negotiate gave no token");
		return value;
	}

	/** A socket past its handshake. */
	async function openSocket() {
		const client = fakeSocket();
		const url = `/hubs/work?id=${await token()}`;
		await ws?.({ ...context(url), socket: client.socket });
		await client.say(HANDSHAKE);
		client.sent.length = 0;
		return client;
	}

	/** An SSE stream past its handshake, and how to post to it. */
	async function openStream() {
		const sse = fakeSse();
		const url = `/hubs/work?id=${await token()}`;
		await routes.get("GET /hubs/work")?.(context(url, "", sse));
		const post = async (raw: string): Promise<Answer> => {
			const ctx = context(url, raw, sse);
			await routes.get("POST /hubs/work")?.(ctx);
			return { code: ctx.code, body: ctx.body };
		};
		await post(HANDSHAKE);
		sse.frames.length = 0;
		return { sse, post };
	}

	return { hub, adapter, openSocket, openStream };
}

describe("hub socket > a bounded backlog", () => {
	it("closes with 1013 once more than maxQueuedMessages wait, and drops them", async () => {
		const { hub, openSocket } = mount(3);
		const client = await openSocket();
		hub.hold();

		await client.say(work("running"));
		for (const label of ["w1", "w2", "w3"]) await client.say(work(label));
		// Three wait behind the one running: the queue is full, not over.
		expect(client.closes).toEqual([]);

		await client.say(work("one too many"));

		expect(client.closes).toEqual([
			{ code: 1013, reason: "Too many hub messages waiting" },
		]);
		// Off the hub at once, before the close is acknowledged.
		expect(hub.stats().clients).toBe(0);
		expect(hub.left).toHaveLength(1);

		hub.letGo();
		await settle();
		// What had not started never does.
		expect(hub.started).toEqual(["running"]);
		expect(hub.finished).toEqual(["running"]);
	});

	it("counts every record of a message, however many one message packs", async () => {
		const { hub, openSocket } = mount(3);
		const client = await openSocket();

		// One WebSocket message, four invocations: more than may wait.
		await client.say(["a", "b", "c", "d"].map((l) => work(l)).join(""));

		expect(client.closes).toEqual([
			{ code: 1013, reason: "Too many hub messages waiting" },
		]);
		expect(hub.started).toEqual([]);
	});

	it("runs a message within the bound, in the order it was sent", async () => {
		const { hub, openSocket } = mount(3);
		const client = await openSocket();

		await client.say(work("a", "1") + work("b", "2") + work("c", "3"));
		await client.say(work("d", "4"));

		expect(hub.finished).toEqual(["a", "b", "c", "d"]);
		expect(client.closes).toEqual([]);
		expect(
			client.sent.map((frame) => JSON.parse(frame.replace(RS, ""))),
		).toEqual([
			{ type: 3, invocationId: "1", result: "a" },
			{ type: 3, invocationId: "2", result: "b" },
			{ type: 3, invocationId: "3", result: "c" },
			{ type: 3, invocationId: "4", result: "d" },
		]);
	});

	it("runs one invocation at a time", async () => {
		const { hub, openSocket } = mount(8);
		const client = await openSocket();
		hub.hold();

		await client.say(work("first"));
		await client.say(work("second"));

		expect(hub.started).toEqual(["first"]);
		hub.letGo();
		await settle();
		expect(hub.finished).toEqual(["first", "second"]);
	});

	it("frees the room a finished message took", async () => {
		const { hub, openSocket } = mount(2);
		const client = await openSocket();

		// Ten messages, never more than one waiting: no backlog, no close.
		for (let i = 0; i < 10; i++) await client.say(work(`m${i}`));

		expect(hub.finished).toHaveLength(10);
		expect(client.closes).toEqual([]);
	});

	it("drops what waits when the client goes away", async () => {
		const { hub, openSocket } = mount(8);
		const client = await openSocket();
		hub.hold();
		await client.say(work("running"));
		await client.say(work("waiting") + work("waiting too"));

		client.ended(1001);
		hub.letGo();
		await settle();

		expect(hub.started).toEqual(["running"]);
		// A message that arrives after the close is not taken either.
		await client.say(work("late"));
		expect(hub.started).toEqual(["running"]);
	});

	it("closes with 1011, off the hub, when handling a frame fails", async () => {
		const { hub, openSocket } = mount(8);
		hub.failOnConnect = true;

		const client = await openSocket();

		expect(client.closes.at(-1)).toEqual({ code: 1011, reason: undefined });
		expect(hub.stats().clients).toBe(0);
	});
});

describe("hub upstream POST > the same queue, the same bound", () => {
	it("refuses a POST past the bound with 429 and runs none of it", async () => {
		const { hub, openStream } = mount(2);
		const { post, sse } = await openStream();
		hub.hold();

		const running = post(work("running", "1"));
		await settle();
		const waiting = post(work("w1", "2") + work("w2", "3"));
		await settle();

		const refused = await post(work("one too many", "4"));

		expect(refused).toEqual({
			code: 429,
			body: {
				error: {
					code: "E_RELAY_MAX_QUEUED",
					message: "Too many messages are waiting on this connection.",
				},
			},
		});
		// The connection is still there, and what it had queued still runs.
		expect(sse.isOpen()).toBe(true);
		hub.letGo();
		expect(await running).toMatchObject({ code: 200 });
		expect(await waiting).toMatchObject({ code: 200 });
		expect(hub.finished).toEqual(["running", "w1", "w2"]);
	});

	it("refuses a body that packs more records than may wait", async () => {
		const { hub, openStream } = mount(2);
		const { post } = await openStream();

		const refused = await post(work("a") + work("b") + work("c"));

		expect(refused.code).toBe(429);
		expect(hub.started).toEqual([]);
	});

	it("runs the POSTs of one connection one after another", async () => {
		const { hub, openStream } = mount(8);
		const { post } = await openStream();
		hub.hold();

		const first = post(work("first"));
		const second = post(work("second"));
		await settle();

		// Two requests in flight, one invocation: the second waits its turn.
		expect(hub.started).toEqual(["first"]);
		hub.letGo();
		await Promise.all([first, second]);
		expect(hub.finished).toEqual(["first", "second"]);
	});

	it("answers a POST still waiting when its connection ends, without running it", async () => {
		const { hub, openStream } = mount(8);
		const { post, sse } = await openStream();
		hub.hold();
		const running = post(work("running"));
		await settle();
		const waiting = post(work("never"));
		await settle();

		await sse.end();

		expect(await waiting).toMatchObject({ code: 200 });
		hub.letGo();
		expect(await running).toMatchObject({ code: 200 });
		expect(hub.started).toEqual(["running"]);
	});
});

describe("SignalRAdapter > maxQueuedMessages", () => {
	it("defaults to 256", () => {
		expect(new SignalRAdapter(new WorkHub()).maxQueuedMessages).toBe(256);
	});

	it("keeps the default for a value that would be no bound", () => {
		for (const value of [0, -1, 1.5, Number.NaN, Number.POSITIVE_INFINITY]) {
			expect(
				new SignalRAdapter(new WorkHub(), { maxQueuedMessages: value })
					.maxQueuedMessages,
			).toBe(256);
		}
		expect(
			new SignalRAdapter(new WorkHub(), { maxQueuedMessages: 32 })
				.maxQueuedMessages,
		).toBe(32);
	});
});

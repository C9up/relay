/**
 * The 2026-09-29 audit, one reproduction per finding: SignalR against the
 * real Ream Request and the handshake order, the bus echo, the channel cap
 * under concurrency, tabs of one user, Warden's guard name, async observers,
 * reopened tokens and streams that never handshake, handler results, and the
 * ping interval as a duration.
 */
import { Request } from "@c9up/ream";
import { afterEach, describe, expect, it, vi } from "vitest";
import { Hub, type HubContext } from "../../src/Hub.js";
import { RedisRelayTransport } from "../../src/RedisRelayTransport.js";
import { Relay, type RelaySseStream } from "../../src/Relay.js";
import { SignalRAdapter } from "../../src/SignalRAdapter.js";
import {
	type HubHttpContext,
	type HubSseStream,
	registerHubRoutes,
} from "../../src/SignalRTransport.js";

const RS = "\x1e";
const HANDSHAKE = `${JSON.stringify({ protocol: "json", version: 1 })}${RS}`;
const AUTH = { isAuthenticated: true, user: { id: "alice" } };

afterEach(() => {
	vi.useRealTimers();
});

function stream(
	id = `s-${Math.random()}`,
): RelaySseStream &
	HubSseStream & { frames: Array<{ event: string; data: unknown }> } {
	let open = true;
	const closers: Array<() => void> = [];
	const frames: Array<{ event: string; data: unknown }> = [];
	return {
		id,
		frames,
		isOpen: () => open,
		async send(event: string, data: unknown) {
			frames.push({ event, data });
			return open;
		},
		onClose(cb: () => void) {
			closers.push(cb);
		},
		async end() {
			open = false;
			for (const cb of closers) cb();
		},
	};
}

function hubRoutes(hub: Hub, adapter = new SignalRAdapter(hub)) {
	const routes = new Map<string, (ctx: HubHttpContext) => Promise<void>>();
	const router = {
		get: (path: string, handler: (ctx: HubHttpContext) => Promise<void>) =>
			routes.set(`GET ${path}`, handler),
		post: (path: string, handler: (ctx: HubHttpContext) => Promise<void>) =>
			routes.set(`POST ${path}`, handler),
	};
	registerHubRoutes(router, { path: "/hub", hub, adapter });
	return { routes, adapter };
}

function hubContext(
	token: string,
	sse: HubSseStream,
	options: { raw?: string; auth?: HubHttpContext["auth"] } = {},
): HubHttpContext & { code: number; body?: unknown } {
	const ctx: HubHttpContext & { code: number; body?: unknown } = {
		code: 200,
		auth: options.auth ?? AUTH,
		request: {
			url: (includeQueryString?: boolean) =>
				includeQueryString === true ? `/hub?id=${token}` : "/hub",
			raw: () => options.raw ?? "",
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
	};
	return ctx;
}

describe("relay > SignalR with Ream's own Request (045)", () => {
	it("finds the connection token although url() leaves the query out", async () => {
		const { routes, adapter } = hubRoutes(new (class extends Hub {})());
		const { connectionToken } = adapter.negotiate("c");
		const request = new Request({
			method: "GET",
			path: "/hub",
			query: `id=${connectionToken}`,
			headers: {},
			body: "",
		});
		expect(request.url()).toBe("/hub");
		const ctx = hubContext(connectionToken, stream());
		ctx.request = { url: (full) => request.url(full), raw: () => "" };
		await routes.get("GET /hub")?.(ctx);
		expect(ctx.code).toBe(200);
	});
});

describe("relay > the bus does not echo a broadcast (047)", () => {
	it("delivers once to local clients when Redis hands the publication back", async () => {
		const listeners = new Set<(message: string, channel: string) => void>();
		const client = {
			subscribe(
				_channel: string,
				fn: (message: string, channel: string) => void,
				options: { onSubscription(count: number): void },
			) {
				listeners.add(fn);
				options.onSubscription(1);
			},
			unsubscribe(
				_channel: string,
				fn: (message: string, channel: string) => void,
			) {
				listeners.delete(fn);
			},
			// Redis Pub/Sub delivers to every subscriber, the publisher included.
			publish(channel: string, message: string) {
				for (const fn of listeners) fn(message, channel);
			},
		};
		const relay = new Relay({
			allowUnauthorizedChannels: true,
			transport: new RedisRelayTransport(client),
		});
		await relay.startTransport();
		const sse = stream();
		const connected = relay.connect(sse, { auth: AUTH });
		if (connected.outcome !== "ok") throw new Error("not connected");
		await relay.subscribe(connected.uid, "news", { auth: AUTH });
		sse.frames.length = 0;
		relay.broadcast("news", { once: true });
		await new Promise((resolve) => setImmediate(resolve));
		expect(sse.frames).toHaveLength(1);
		await relay.shutdown();
	});
});

describe("relay > the channel cap under concurrency (048)", () => {
	it("keeps maxChannelsPerClient when subscriptions race an async authorizer", async () => {
		const relay = new Relay({ maxChannelsPerClient: 1 });
		relay.authorize("rooms/:id", async () => true);
		const connected = relay.connect(stream(), { auth: AUTH });
		if (connected.outcome !== "ok") throw new Error("not connected");
		const results = await Promise.all(
			["a", "b", "c"].map((id) =>
				relay.subscribe(connected.uid, `rooms/${id}`, { auth: AUTH }),
			),
		);
		expect(results.filter((result) => result.ok)).toHaveLength(1);
	});
});

describe("relay > connections of one user are independent (049, 050)", () => {
	it("a failed write on one tab leaves the other open", async () => {
		const relay = new Relay();
		const first = stream("first");
		let reject: ((error: Error) => void) | undefined;
		first.send = () =>
			new Promise((_, fail) => {
				reject = fail;
			});
		relay.connect(first, { auth: AUTH });
		const second = stream("second");
		relay.connect(second, { auth: AUTH });
		reject?.(new Error("old writer failed"));
		await new Promise((resolve) => setImmediate(resolve));
		expect(second.isOpen()).toBe(true);
		expect(relay.clientCount()).toBe(1);
	});
});

describe("relay > async observers are isolated (052)", () => {
	it("catches a rejecting listener instead of leaving it unhandled", async () => {
		const unhandled: unknown[] = [];
		const record = (reason: unknown) => unhandled.push(reason);
		process.on("unhandledRejection", record);
		const warn = vi.spyOn(process.stderr, "write").mockReturnValue(true);
		try {
			const relay = new Relay();
			relay.on("broadcast", async () => {
				throw new Error("observer rejected");
			});
			relay.broadcast("news", {});
			await new Promise((resolve) => setImmediate(resolve));
			expect(unhandled).toEqual([]);
			expect(String(warn.mock.calls[0]?.[0])).toContain("observer rejected");
		} finally {
			process.off("unhandledRejection", record);
			warn.mockRestore();
		}
	});
});

describe("relay > SignalR hubs (051, 053, results)", () => {
	it("recognises the guard Warden authenticated through", async () => {
		class Guarded extends Hub {
			calls = 0;
			async onWrite(): Promise<void> {
				this.calls++;
			}
		}
		const hub = new Guarded();
		hub.useGuards({ guards: ["web"] });
		const { routes, adapter } = hubRoutes(hub);
		const { connectionToken } = adapter.negotiate("c");
		await routes.get("GET /hub")?.(
			hubContext(connectionToken, stream(), {
				auth: { ...AUTH, authenticatedViaGuard: "web" },
			}),
		);
		await adapter.handleFrame("c", HANDSHAKE);
		await adapter.handleFrame(
			"c",
			`${JSON.stringify({ type: 1, invocationId: "1", target: "write", arguments: [] })}${RS}`,
		);
		expect(hub.calls).toBe(1);
	});

	it("closes the older stream when the same token is opened again", async () => {
		const { routes, adapter } = hubRoutes(new (class extends Hub {})());
		const { connectionToken } = adapter.negotiate("c");
		const first = stream("first");
		const second = stream("second");
		await routes.get("GET /hub")?.(hubContext(connectionToken, first));
		await routes.get("GET /hub")?.(hubContext(connectionToken, second));
		expect(first.isOpen()).toBe(false);
		expect(second.isOpen()).toBe(true);
	});

	it("closes a stream that never completes the handshake", async () => {
		vi.useFakeTimers();
		const hub = new (class extends Hub {})();
		const adapter = new SignalRAdapter(hub, { handshakeTimeoutMs: 1000 });
		const { routes } = hubRoutes(hub, adapter);
		const { connectionToken } = adapter.negotiate("c");
		const sse = stream();
		await routes.get("GET /hub")?.(hubContext(connectionToken, sse));
		vi.advanceTimersByTime(1001);
		expect(sse.isOpen()).toBe(false);
	});

	it("refuses a stream past maxConnections", async () => {
		const hub = new (class extends Hub {})();
		const adapter = new SignalRAdapter(hub, { maxConnections: 1 });
		const { routes } = hubRoutes(hub, adapter);
		const one = adapter.negotiate("one").connectionToken;
		const two = adapter.negotiate("two").connectionToken;
		await routes.get("GET /hub")?.(hubContext(one, stream()));
		const refused = hubContext(two, stream());
		await routes.get("GET /hub")?.(refused);
		expect(refused.code).toBe(503);
	});

	it("resolves an invocation with the handler's return value", async () => {
		class Maths extends Hub {
			async onAdd(_ctx: HubContext, a: number, b: number): Promise<number> {
				return a + b;
			}
		}
		const hub = new Maths();
		const { routes, adapter } = hubRoutes(hub);
		const { connectionToken } = adapter.negotiate("c");
		await routes.get("GET /hub")?.(hubContext(connectionToken, stream()));
		await adapter.handleFrame("c", HANDSHAKE);
		const [completion] = await adapter.handleFrame(
			"c",
			`${JSON.stringify({ type: 1, invocationId: "7", target: "add", arguments: [2, 3] })}${RS}`,
		);
		expect(JSON.parse(String(completion).slice(0, -1))).toEqual({
			type: 3,
			invocationId: "7",
			result: 5,
		});
	});
});

describe("relay > pingInterval takes a duration", () => {
	it("reads '30s' as thirty seconds and refuses what it cannot read", async () => {
		vi.useFakeTimers();
		const relay = new Relay({ pingInterval: "30s" });
		const sse = stream();
		relay.connect(sse, { auth: AUTH });
		await relay.startTransport();
		sse.frames.length = 0;
		vi.advanceTimersByTime(30_000);
		expect(sse.frames.map((frame) => frame.event)).toContain("$$relay/ping");
		await relay.shutdown();
		expect(() => new Relay({ pingInterval: "soon" })).toThrow(/pingInterval/);
	});
});

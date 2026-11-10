/**
 * What a shutdown does to the streams.
 *
 * Transmit's `shutdown()` clears its ping interval, then `closeAllStreams()` —
 * which destroys every response and makes `createStream` refuse any later one —
 * then disconnects the bus. Relay stopped the timer and the bus and left every
 * stream open, still accepting new ones, on the premise that this was what
 * Transmit did.
 */

import { describe, expect, it } from "vitest";
import { Hub, type HubContext } from "../../src/Hub.js";
import {
	Relay,
	type RelayRouteBuilder,
	type RelaySseStream,
} from "../../src/Relay.js";
import { SignalRAdapter } from "../../src/SignalRAdapter.js";
import {
	type HubHttpContext,
	type HubSseStream,
	registerHubRoutes,
} from "../../src/SignalRTransport.js";

function stream(): RelaySseStream &
	HubSseStream & {
		ended: () => boolean;
		frames: Array<{ event: string; data: unknown }>;
	} {
	let open = true;
	const closers: Array<() => void> = [];
	const frames: Array<{ event: string; data: unknown }> = [];
	return {
		id: `s-${Math.random()}`,
		frames,
		ended: () => !open,
		isOpen: () => open,
		send: async (event, data) => {
			frames.push({ event, data });
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

describe("relay > shutdown ends the streams", () => {
	it("ends every open stream and lets go of its clients", async () => {
		const relay = new Relay({ allowUnauthorizedChannels: true });
		const disconnected: string[] = [];
		relay.on("disconnect", ({ uid }) => {
			disconnected.push(uid);
		});
		const a = stream();
		const b = stream();
		const first = relay.connect(a, {});
		const second = relay.connect(b, {});
		if (first.outcome !== "ok" || second.outcome !== "ok") {
			throw new Error("capped");
		}
		await relay.subscribe(first.uid, "news", {});

		await relay.shutdown();

		expect(a.ended()).toBe(true);
		expect(b.ended()).toBe(true);
		expect(relay.clientCount()).toBe(0);
		expect(relay.channelSubscribers("news")).toBe(0);
		expect(disconnected.sort()).toEqual([first.uid, second.uid].sort());
	});

	it("refuses a connection after the shutdown, and touches nothing of it", async () => {
		const relay = new Relay();
		await relay.shutdown();

		const late = stream();
		expect(relay.connect(late, {})).toEqual({ outcome: "closed" });
		expect(relay.clientCount()).toBe(0);
		// No `connected` frame: the client has no uid to subscribe with.
		expect(late.frames).toEqual([]);
	});

	it("accepts connections again once restarted", async () => {
		const relay = new Relay();
		await relay.shutdown();

		await relay.startTransport();

		expect(relay.connect(stream(), {}).outcome).toBe("ok");
	});
});

type Handler = (ctx: HubHttpContext) => Promise<void> | void;

function recordingRouter() {
	const routes = new Map<string, Handler>();
	const builder: RelayRouteBuilder = {
		middleware: () => builder,
		use: () => builder,
		guard: () => builder,
		role: () => builder,
		permission: () => builder,
	};
	return {
		routes,
		get(path: string, handler: Handler) {
			routes.set(`GET ${path}`, handler);
			return builder;
		},
		post(path: string, handler: Handler) {
			routes.set(`POST ${path}`, handler);
			return builder;
		},
	};
}

interface Exchange {
	ctx: HubHttpContext & {
		request: HubHttpContext["request"] & { body(): unknown };
		response: HubHttpContext["response"] & {
			header(): unknown;
			noContent(): void;
		};
	};
	status: () => number;
	body: () => unknown;
}

function exchange(url: string, raw: string, sse: HubSseStream): Exchange {
	let status = 200;
	let body: unknown;
	const response = {
		status(code: number) {
			status = code;
			return response;
		},
		header: () => response,
		json(value: unknown) {
			body = value;
		},
		noContent() {},
		sse: async () => sse,
	};
	return {
		ctx: {
			request: {
				url: (qs?: boolean) => (qs === true ? url : (url.split("?")[0] ?? url)),
				raw: () => raw,
				body: () => ({}),
			},
			response,
		},
		status: () => status,
		body: () => body,
	};
}

class ChatHub extends Hub {
	readonly gone: string[] = [];
	override async onDisconnect(clientId: string): Promise<void> {
		this.gone.push(clientId);
	}
	async onPing(_ctx: HubContext): Promise<string> {
		return "pong";
	}
}

/** Negotiate and open a hub stream through the routes. */
async function openHubStream(routes: Map<string, Handler>, path: string) {
	const call = async (key: string, ex: Exchange) => {
		const handler = routes.get(key);
		if (handler === undefined) throw new Error(`no route ${key}`);
		await handler(ex.ctx);
		return ex;
	};
	const sse = stream();
	const negotiated = await call(
		`POST ${path}/negotiate`,
		exchange(`${path}/negotiate`, "", sse),
	);
	const token = String(
		Reflect.get(Object(negotiated.body()), "connectionToken"),
	);
	const opened = await call(
		`GET ${path}`,
		exchange(`${path}?id=${token}`, "", sse),
	);
	return { sse, negotiated, opened, call };
}

describe("relay > shutdown ends the hub streams", () => {
	it("closes every hub stream and refuses new ones until reopened", async () => {
		const router = recordingRouter();
		const hub = new ChatHub();
		const mounted = registerHubRoutes(router, {
			path: "/hubs/chat",
			hub,
			adapter: new SignalRAdapter(hub),
		});
		const { sse, call } = await openHubStream(router.routes, "/hubs/chat");

		await mounted.close();

		expect(sse.ended()).toBe(true);
		expect(hub.stats().clients).toBe(0);
		expect(hub.gone).toHaveLength(1);
		const refused = await call(
			"POST /hubs/chat/negotiate",
			exchange("/hubs/chat/negotiate", "", stream()),
		);
		expect(refused.status()).toBe(503);
		expect(refused.body()).toEqual({
			error: {
				code: "E_RELAY_SHUT_DOWN",
				message: "The realtime service is shutting down.",
			},
		});

		mounted.reopen();
		const again = await openHubStream(router.routes, "/hubs/chat");
		expect(again.opened.status()).toBe(200);
		expect(again.sse.ended()).toBe(false);
		expect(hub.stats().clients).toBe(1);
	});

	it("refuses a GET on a token negotiated before the shutdown", async () => {
		const router = recordingRouter();
		const hub = new ChatHub();
		const mounted = registerHubRoutes(router, {
			path: "/hubs/chat",
			hub,
			adapter: new SignalRAdapter(hub),
		});
		const negotiate = router.routes.get("POST /hubs/chat/negotiate");
		const negotiated = exchange("/hubs/chat/negotiate", "", stream());
		await negotiate?.(negotiated.ctx);
		const token = String(
			Reflect.get(Object(negotiated.body()), "connectionToken"),
		);

		await mounted.close();
		const late = stream();
		const get = exchange(`/hubs/chat?id=${token}`, "", late);
		await router.routes.get("GET /hubs/chat")?.(get.ctx);

		expect(get.status()).toBe(503);
		expect(hub.stats().clients).toBe(0);
	});
});

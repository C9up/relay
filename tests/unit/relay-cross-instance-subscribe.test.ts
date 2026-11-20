/**
 * With a bus, a subscribe or an unsubscribe may reach an instance that does
 * not hold the stream — a round-robin balancer sends the POST anywhere. The
 * receiving instance authorizes it with the request it has, then relays it on
 * the bus; the instance holding the uid applies it after checking that the
 * requester owns the stream and that the per-client channel cap still holds.
 */

import { describe, expect, it } from "vitest";
import {
	Relay,
	type RelayContext,
	type RelaySseStream,
	type RelayTransport,
} from "../../src/Relay.js";

function fakeSse(): RelaySseStream & { sent: Array<{ event: string }> } {
	let open = true;
	let closeCb: (() => void) | undefined;
	const sent: Array<{ event: string }> = [];
	return {
		id: `s-${Math.random()}`,
		sent,
		isOpen: () => open,
		async send(event) {
			sent.push({ event });
			return open;
		},
		onClose(cb) {
			closeCb = cb;
		},
		async end() {
			open = false;
			closeCb?.();
		},
	};
}

/** One in-process bus delivering every publication to every subscriber. */
function sharedBus(): RelayTransport & { published: unknown[] } {
	const handlers: Array<(message: unknown) => void> = [];
	const published: unknown[] = [];
	return {
		published,
		publish: (_channel, message) => {
			published.push(message);
			// Through JSON, as a real bus carries it: undefined keys vanish.
			const wire: unknown = JSON.parse(JSON.stringify(message));
			for (const handler of handlers) handler(wire);
		},
		subscribe: (_channel, handler) => {
			handlers.push(handler);
		},
	};
}

const user = (id: string): RelayContext => ({
	auth: { isAuthenticated: true, user: { id } },
});

async function twoInstances(config: { maxChannelsPerClient?: number } = {}) {
	const bus = sharedBus();
	const a = new Relay({ transport: bus, ...config });
	const b = new Relay({ transport: bus, ...config });
	const asked: Array<{ instance: string; channel: string }> = [];
	a.authorize("news/*", (_ctx, params) => {
		asked.push({ instance: "a", channel: params["*"] ?? "" });
		return true;
	});
	b.authorize("news/*", (_ctx, params) => {
		asked.push({ instance: "b", channel: params["*"] ?? "" });
		return true;
	});
	b.authorize("secret", () => false);
	await a.startTransport();
	await b.startTransport();
	return { bus, a, b, asked };
}

describe("relay > a subscribe that lands on another instance", () => {
	it("is authorized where it lands and applied by the instance holding the stream", async () => {
		const { a, b, asked } = await twoInstances();
		const sse = fakeSse();
		const connected = a.connect(sse, user("u1"));
		if (connected.outcome !== "ok") throw new Error("connect failed");

		const result = await b.subscribe(connected.uid, "news/1", user("u1"));

		expect(result).toEqual({ ok: true });
		expect(asked).toEqual([{ instance: "b", channel: "1" }]);
		expect(a.getSubscribersFor("news/1")).toEqual([connected.uid]);
		expect(b.getSubscribersFor("news/1")).toEqual([]);
		b.broadcast("news/1", { hello: 1 });
		expect(sse.sent.map((frame) => frame.event)).toEqual([
			"connected",
			"news/1",
		]);
	});

	it("is refused where it lands when the authorizer there refuses, and nothing is relayed", async () => {
		const { bus, a, b } = await twoInstances();
		const connected = a.connect(fakeSse(), user("u1"));
		if (connected.outcome !== "ok") throw new Error("connect failed");

		const result = await b.subscribe(connected.uid, "secret", user("u1"));

		expect(result).toEqual({
			ok: false,
			status: 403,
			code: "E_RELAY_CHANNEL_FORBIDDEN",
		});
		expect(bus.published).toEqual([]);
		expect(a.getSubscribersFor("secret")).toEqual([]);
	});

	it("is dropped by the holder when the requester is not the stream's owner", async () => {
		const { a, b } = await twoInstances();
		const connected = a.connect(fakeSse(), user("victim"));
		if (connected.outcome !== "ok") throw new Error("connect failed");

		await b.subscribe(connected.uid, "news/1", user("attacker"));
		await b.subscribe(connected.uid, "news/2", {});

		expect(a.getSubscribersFor("news/1")).toEqual([]);
		expect(a.getSubscribersFor("news/2")).toEqual([]);
	});

	it("binds the session too: another established session cannot subscribe an anonymous stream", async () => {
		const { a, b } = await twoInstances();
		const session = (sessionId: string): RelayContext => ({
			session: { sessionId, fresh: false },
		});
		const connected = a.connect(fakeSse(), session("s1"));
		if (connected.outcome !== "ok") throw new Error("connect failed");

		await b.subscribe(connected.uid, "news/1", session("s2"));
		expect(a.getSubscribersFor("news/1")).toEqual([]);

		await b.subscribe(connected.uid, "news/1", session("s1"));
		expect(a.getSubscribersFor("news/1")).toEqual([connected.uid]);
	});

	it("respects maxChannelsPerClient on the holder", async () => {
		const { a, b } = await twoInstances({ maxChannelsPerClient: 1 });
		const connected = a.connect(fakeSse(), user("u1"));
		if (connected.outcome !== "ok") throw new Error("connect failed");

		await b.subscribe(connected.uid, "news/1", user("u1"));
		await b.subscribe(connected.uid, "news/2", user("u1"));

		expect(a.getSubscribersFor("news/1")).toEqual([connected.uid]);
		expect(a.getSubscribersFor("news/2")).toEqual([]);
	});

	it("emits `subscribe` on the holder, once applied, with the stream's context", async () => {
		const { a, b } = await twoInstances();
		const seenOnA: string[] = [];
		const seenOnB: string[] = [];
		a.on("subscribe", ({ channel }) => seenOnA.push(channel));
		b.on("subscribe", ({ channel }) => seenOnB.push(channel));
		const connected = a.connect(fakeSse(), user("u1"));
		if (connected.outcome !== "ok") throw new Error("connect failed");

		await b.subscribe(connected.uid, "news/1", user("u1"));
		await b.subscribe(connected.uid, "news/9", user("intruder"));

		expect(seenOnA).toEqual(["news/1"]);
		expect(seenOnB).toEqual([]);
	});

	it("answers E_RELAY_NOT_CONNECTED without a bus, as before", async () => {
		const r = new Relay({ allowUnauthorizedChannels: true });
		expect(await r.subscribe("nobody", "news/1", {})).toEqual({
			ok: false,
			status: 400,
			code: "E_RELAY_NOT_CONNECTED",
		});
	});

	it("answers 503 E_RELAY_BUS_UNAVAILABLE when the bus refuses the relay", async () => {
		const transport: RelayTransport = {
			publish: async () => {
				throw new Error("bus down");
			},
			subscribe: () => {},
		};
		const r = new Relay({ allowUnauthorizedChannels: true, transport });
		await r.startTransport();

		expect(await r.subscribe("elsewhere", "news/1", {})).toEqual({
			ok: false,
			status: 503,
			code: "E_RELAY_BUS_UNAVAILABLE",
		});
	});

	it("ignores a malformed subscription envelope from the bus", async () => {
		let busHandler: ((message: unknown) => void) | undefined;
		const transport: RelayTransport = {
			publish: () => {},
			subscribe: (_channel, handler) => {
				busHandler = handler;
			},
		};
		const r = new Relay({ allowUnauthorizedChannels: true, transport });
		await r.startTransport();
		const connected = r.connect(fakeSse(), {});
		if (connected.outcome !== "ok") throw new Error("connect failed");

		busHandler?.({ type: "subscribe", uid: connected.uid, channel: 7 });
		busHandler?.({
			type: "subscribe",
			uid: connected.uid,
			channel: "x".repeat(257),
		});
		busHandler?.({
			type: "subscribe",
			uid: connected.uid,
			channel: "ok",
			user: 42,
		});

		expect(r.getSubscribersFor("ok")).toEqual([]);
	});
});

describe("relay > an unsubscribe that lands on another instance", () => {
	it("is relayed to the holder and applied there", async () => {
		const { a, b } = await twoInstances();
		const seen: string[] = [];
		a.on("unsubscribe", ({ channel }) => seen.push(channel));
		const connected = a.connect(fakeSse(), user("u1"));
		if (connected.outcome !== "ok") throw new Error("connect failed");
		await a.subscribe(connected.uid, "news/1", user("u1"));

		expect(await b.unsubscribe(connected.uid, "news/1", user("u1"))).toBe("ok");

		expect(a.getSubscribersFor("news/1")).toEqual([]);
		expect(seen).toEqual(["news/1"]);
	});

	it("is dropped by the holder when the requester is not the owner", async () => {
		const { a, b } = await twoInstances();
		const connected = a.connect(fakeSse(), user("victim"));
		if (connected.outcome !== "ok") throw new Error("connect failed");
		await a.subscribe(connected.uid, "news/1", user("victim"));

		await b.unsubscribe(connected.uid, "news/1", user("attacker"));

		expect(a.getSubscribersFor("news/1")).toEqual([connected.uid]);
	});
});

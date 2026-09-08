/**
 * A subscribe is asynchronous, so a shutdown or a restart can land while one is
 * still in flight. These are the two ways that used to go wrong:
 *
 *   - a subscription completing AFTER a shutdown stayed on the bus, because
 *     the shutdown had already looked for a handler and found none;
 *   - a failure reported after a restart tore down the state of the attempt
 *     that replaced it.
 */

import { describe, expect, it } from "vitest";
import {
	Relay,
	type RelaySseStream,
	type RelayTransport,
} from "../../src/Relay.js";

/** An SSE double that records every frame the relay pushes to it. */
function fakeSse(): RelaySseStream & {
	sent: Array<{ event: string; data: unknown }>;
} {
	const sent: Array<{ event: string; data: unknown }> = [];
	return {
		id: "s-1",
		sent,
		isOpen: () => true,
		async send(event, data) {
			sent.push({ event, data });
			return true;
		},
		onClose() {},
		async end() {},
	};
}

/** A client of `relay` listening on `channel`, with its connect frame dropped. */
async function subscribedClient(
	relay: Relay,
	channel: string,
): Promise<ReturnType<typeof fakeSse>> {
	const sse = fakeSse();
	const auth = { isAuthenticated: true, user: { id: "u1" } };
	const outcome = relay.connect(undefined, sse, { auth });
	if (outcome.outcome !== "ok") throw new Error("connect failed");
	const sub = await relay.subscribe(outcome.uid, channel, { auth });
	if (!sub.ok) throw new Error("subscribe failed");
	sse.sent.length = 0;
	return sse;
}

function deferred(): { promise: Promise<void>; resolve: () => void } {
	let resolve!: () => void;
	const promise = new Promise<void>((r) => {
		resolve = r;
	});
	return { promise, resolve };
}

/**
 * A bus that models the one relay actually runs on.
 *
 * Handlers STACK, exactly as `@c9up/quasar` stacks them in a `Set` — the
 * earlier fake kept one handler per channel in a `Map`, so a second subscribe
 * silently replaced the first and hid the duplicate-delivery this exists to
 * catch. `unsubscribe` drops one named handler, or all of them when it is not
 * told which.
 */
function fakeBus(options: {
	hold?: Promise<void>;
	holdCall?: number;
	failFirst?: Promise<void>;
	holdDisconnect?: Promise<void>;
	/** Model a transport whose `disconnect` really does close the bus. */
	closesOnDisconnect?: boolean;
}) {
	const handlers = new Map<string, Set<(message: unknown) => void>>();
	let calls = 0;
	let disconnected = 0;
	const transport: RelayTransport = {
		publish: async () => {},
		subscribe: async (channel, handler) => {
			calls += 1;
			if (calls === 1 && options.failFirst) {
				await options.failFirst;
				throw new Error("the bus refused the subscription");
			}
			if (calls === (options.holdCall ?? 1) && options.hold) {
				await options.hold;
			}
			const set = handlers.get(channel) ?? new Set();
			set.add(handler);
			handlers.set(channel, set);
		},
		unsubscribe: async (channel, handler) => {
			const set = handlers.get(channel);
			if (!set) return;
			if (handler === undefined) set.clear();
			else set.delete(handler);
			if (set.size === 0) handlers.delete(channel);
		},
		disconnect: async () => {
			if (options.holdDisconnect) await options.holdDisconnect;
			disconnected += 1;
			// A closed bus delivers nothing, whoever is still listening.
			if (options.closesOnDisconnect) handlers.clear();
		},
	};
	return {
		transport,
		calls: () => calls,
		disconnects: () => disconnected,
		live: () => handlers.get("relay::broadcast")?.size ?? 0,
		isLive: () => (handlers.get("relay::broadcast")?.size ?? 0) > 0,
		emit: (channel: string, payload: unknown) => {
			for (const handler of handlers.get("relay::broadcast") ?? []) {
				handler({ type: "broadcast", channel, payload });
			}
		},
	};
}

describe("relay > a start that lands after a shutdown", () => {
	it("takes its own subscription back down", async () => {
		// The shutdown ran while the subscribe was still in flight, so it found
		// nothing to unsubscribe. Left alone, the handler landed a moment later
		// and stayed on the bus for the rest of the process — untracked, so no
		// later shutdown could name it either.
		const held = deferred();
		const bus = fakeBus({ hold: held.promise });
		const relay = new Relay({ transport: bus.transport });

		const starting = relay.startTransport();
		const stopping = relay.shutdown();
		held.resolve();
		await starting.catch(() => {});
		await stopping;

		expect(bus.isLive()).toBe(false);
	});

	it("stops delivering to the clients it was serving", async () => {
		// The consequence: a relay that was shut down still re-emitting every
		// remote broadcast to the streams its shutdown was meant to release.
		const held = deferred();
		const bus = fakeBus({ hold: held.promise });
		const relay = new Relay({
			allowUnauthorizedChannels: true,
			transport: bus.transport,
		});
		const sse = await subscribedClient(relay, "news");

		const starting = relay.startTransport();
		const stopping = relay.shutdown();
		held.resolve();
		await starting.catch(() => {});
		await stopping;
		bus.emit("news", { after: "shutdown" });

		expect(sse.sent).toEqual([]);
	});
});

describe("relay > a failure reported after a restart", () => {
	it("leaves the attempt that replaced it alone", async () => {
		// The stale catch used to clear the NEW attempt's ping timer and mark
		// the relay stopped while its subscription was live.
		const failure = deferred();
		const bus = fakeBus({ failFirst: failure.promise });
		const relay = new Relay({ transport: bus.transport });

		const first = relay.startTransport();
		const stopping = relay.shutdown();
		const second = relay.startTransport();
		await second;
		failure.resolve();
		await first.catch(() => {});
		await stopping;

		expect(relay.hasStartedTransport()).toBe(true);
	});

	it("does not make the next start subscribe a second time", async () => {
		// Believing itself stopped, the relay subscribed again to a channel it
		// was already on, and every broadcast arrived twice.
		const failure = deferred();
		const bus = fakeBus({ failFirst: failure.promise });
		const relay = new Relay({ transport: bus.transport });

		const first = relay.startTransport();
		const stopping = relay.shutdown();
		const second = relay.startTransport();
		await second;
		failure.resolve();
		await first.catch(() => {});
		await stopping;
		await relay.startTransport();

		expect(bus.calls()).toBe(2);
	});
});

describe("relay > one live subscription, whatever the restart looked like", () => {
	it("does not let a superseded success double every broadcast", async () => {
		const held = deferred();
		const bus = fakeBus({ hold: held.promise });
		const relay = new Relay({
			allowUnauthorizedChannels: true,
			transport: bus.transport,
		});
		const sse = await subscribedClient(relay, "news");

		const first = relay.startTransport();
		const stopping = relay.shutdown();
		const second = relay.startTransport();
		held.resolve();
		await first.catch(() => {});
		await second.catch(() => {});
		await stopping;
		await relay.startTransport();

		bus.emit("news", { n: 1 });
		expect(sse.sent).toHaveLength(1);
	});

	it("does not disconnect the bus a restart has just re-subscribed to", async () => {
		// `shutdown()` marks the relay restartable before its own unsubscribe
		// and disconnect have run, so a start issued in that window came up on
		// a transport the shutdown then closed underneath it.
		const bus = fakeBus({});
		const relay = new Relay({ transport: bus.transport });

		await relay.startTransport();
		const stopping = relay.shutdown();
		const restarting = relay.startTransport();
		await stopping;
		await restarting;

		expect(bus.isLive()).toBe(true);
		expect(bus.disconnects()).toBe(0);
	});

	it("does not unsubscribe the other instance sharing the bus", async () => {
		// Two Relay instances on one transport is the shape a test harness and
		// a multi-tenant host both take. "Drop everything on this channel" is
		// not this attempt's to say: it silenced an instance that had nothing
		// to do with the shutdown.
		const held = deferred();
		const bus = fakeBus({ hold: held.promise, holdCall: 2 });
		const config = {
			allowUnauthorizedChannels: true,
			transport: bus.transport,
		};
		const staying = new Relay(config);
		const leaving = new Relay(config);
		const sse = await subscribedClient(staying, "news");
		await staying.startTransport();

		const starting = leaving.startTransport();
		const stopping = leaving.shutdown();
		held.resolve();
		await starting.catch(() => {});
		await stopping;

		expect(bus.live()).toBe(1);
		bus.emit("news", { n: 1 });
		expect(sse.sent).toHaveLength(1);
	});

	it("does not wipe a restart with the shutdown it overlapped", async () => {
		// With nothing live to name, a shutdown removes every handler relay put
		// on the channel — which is right, unless a restart has already put a
		// new one there. The restart waits for the teardown instead of racing
		// it.
		const held = deferred();
		const bus = fakeBus({ hold: held.promise });
		const relay = new Relay({
			allowUnauthorizedChannels: true,
			transport: bus.transport,
		});
		const sse = await subscribedClient(relay, "news");

		const starting = relay.startTransport();
		const stopping = relay.shutdown();
		const restarting = relay.startTransport();
		held.resolve();
		await starting.catch(() => {});
		await stopping;
		await restarting;

		expect(bus.live()).toBe(1);
		bus.emit("news", { n: 1 });
		expect(sse.sent).toHaveLength(1);
	});

	it("is not closed by a shutdown that was already disconnecting", async () => {
		// The generation is checked BEFORE `disconnect()` is awaited. A restart
		// that lands during that await passes the check that already happened,
		// so the old shutdown went on to close the connection the new
		// generation had just subscribed on: relay reports itself started, its
		// handler is live, and nothing reaches it.
		const closing = deferred();
		const bus = fakeBus({
			holdDisconnect: closing.promise,
			closesOnDisconnect: true,
		});
		const relay = new Relay({
			allowUnauthorizedChannels: true,
			transport: bus.transport,
		});
		const sse = await subscribedClient(relay, "news");

		await relay.startTransport();
		const stopping = relay.shutdown();
		// Let the shutdown reach its disconnect before restarting.
		await Promise.resolve();
		await Promise.resolve();
		const restarting = relay.startTransport();
		closing.resolve();
		await stopping;
		await restarting;

		expect(relay.hasStartedTransport()).toBe(true);
		bus.emit("news", { n: 1 });
		expect(sse.sent).toHaveLength(1);
	});

	it("refuses to start again when the teardown left a handler behind", async () => {
		// `await teardown.catch(() => {})` swallowed the failure and subscribed
		// anyway. With the old handler still on the bus, every message was then
		// delivered twice — and the next shutdown could only name the new one.
		const handlers = new Set<(message: unknown) => void>();
		let refuse = true;
		const transport: RelayTransport = {
			publish: async () => {},
			subscribe: async (_channel, handler) => {
				handlers.add(handler);
			},
			unsubscribe: async (_channel, handler) => {
				if (refuse) throw new Error("the connection is busy");
				if (handler) handlers.delete(handler);
				else handlers.clear();
			},
		};
		const relay = new Relay({
			allowUnauthorizedChannels: true,
			transport,
		});
		const sse = await subscribedClient(relay, "news");

		await relay.startTransport();
		await expect(relay.shutdown()).rejects.toThrow("busy");

		await expect(relay.startTransport()).rejects.toThrow(
			/could not be released/,
		);
		expect(relay.hasStartedTransport()).toBe(false);

		// And once the bus recovers, a start works and delivers ONCE.
		refuse = false;
		await relay.shutdown();
		await relay.startTransport();
		for (const handler of handlers) {
			handler({ type: "broadcast", channel: "news", payload: { n: 1 } });
		}
		expect(sse.sent).toHaveLength(1);
	});

	it("refuses to start again after a subscribe whose undo failed", async () => {
		// A client can register the callback and THEN fail, and the undo can
		// fail too. Relay forgot its handler on the error and had no teardown
		// to point at, so the next start subscribed beside a listener still
		// live: two handlers, every message twice, and only the newer nameable.
		const handlers = new Set<(message: unknown) => void>();
		let refuseUnsubscribe = true;
		let failSubscribe = true;
		const transport: RelayTransport = {
			publish: async () => {},
			subscribe: async (_channel, handler) => {
				handlers.add(handler);
				if (failSubscribe) throw new Error("no route to the bus");
			},
			unsubscribe: async (_channel, handler) => {
				if (refuseUnsubscribe) throw new Error("the connection is busy");
				if (handler) handlers.delete(handler);
				else handlers.clear();
			},
		};
		const relay = new Relay({
			allowUnauthorizedChannels: true,
			transport,
		});
		const sse = await subscribedClient(relay, "news");

		await expect(relay.startTransport()).rejects.toThrow("no route to the bus");

		failSubscribe = false;
		await expect(relay.startTransport()).rejects.toThrow(
			/could not be released/,
		);

		// Once the bus recovers, the obligation clears and a start delivers ONCE.
		refuseUnsubscribe = false;
		await relay.startTransport();
		for (const handler of handlers) {
			handler({ type: "broadcast", channel: "news", payload: { n: 1 } });
		}
		expect(sse.sent).toHaveLength(1);
	});
});

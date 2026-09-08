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
 * A bus that models what matters here: a handler is only live once `subscribe`
 * has resolved, and `unsubscribe` takes it back off.
 */
function fakeBus(options: { hold?: Promise<void>; failFirst?: Promise<void> }) {
	const handlers = new Map<string, (message: unknown) => void>();
	let calls = 0;
	const transport: RelayTransport = {
		publish: async () => {},
		subscribe: async (channel, handler) => {
			calls += 1;
			if (calls === 1 && options.failFirst) {
				await options.failFirst;
				throw new Error("the bus refused the subscription");
			}
			if (calls === 1 && options.hold) await options.hold;
			handlers.set(channel, handler);
		},
		unsubscribe: async (channel) => {
			handlers.delete(channel);
		},
	};
	return {
		transport,
		calls: () => calls,
		isLive: () => handlers.has("relay::broadcast"),
		emit: (channel: string, payload: unknown) => {
			handlers.get("relay::broadcast")?.({
				type: "broadcast",
				channel,
				payload,
			});
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

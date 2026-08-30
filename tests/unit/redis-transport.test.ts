/**
 * The Redis bus: what carries a broadcast from the instance that made it to
 * the SSE clients of every other one.
 */
import { describe, expect, it, vi } from "vitest";
import {
	RedisRelayTransport,
	Relay,
	type RelayPubSubClient,
	type RelaySseStream,
	transports,
} from "../../src/index.js";

/** Minimal SSE double that records every `send(event, data)`. */
function fakeSse(id: string): RelaySseStream & {
	sent: Array<{ event: string; data: unknown }>;
} {
	const sent: Array<{ event: string; data: unknown }> = [];
	return {
		id,
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

/** A pub/sub client that delivers to its own subscribers, in-process. */
function fakeRedis() {
	const handlers = new Map<
		string,
		Set<(message: string, channel: string) => void>
	>();
	const client: RelayPubSubClient & { quit: () => void; quits: number } = {
		quits: 0,
		publish(channel, message) {
			for (const handler of handlers.get(channel) ?? []) {
				handler(message, channel);
			}
			return handlers.get(channel)?.size ?? 0;
		},
		subscribe(channel, handler) {
			const set = handlers.get(channel) ?? new Set();
			set.add(handler);
			handlers.set(channel, set);
		},
		unsubscribe(channel) {
			handlers.delete(channel);
		},
		quit() {
			client.quits += 1;
		},
	};
	return { client, handlers };
}

describe("relay > redis transport", () => {
	it("carries a broadcast from one instance to another", async () => {
		const { client } = fakeRedis();
		const config = {
			allowUnauthorizedChannels: true,
			transport: transports.redis({ connection: client }),
		};
		const publisher = new Relay(config);
		const subscriber = new Relay(config);

		// Let both subscriptions settle — they are established asynchronously.
		await Promise.resolve();
		await Promise.resolve();

		const sse = fakeSse("s-a");
		const connected = subscriber.connect(undefined, sse, {
			auth: { isAuthenticated: true, user: { id: "u-a" } },
		});
		if (connected.outcome !== "ok") throw new Error("connect failed");
		await subscriber.subscribe(connected.uid, "news", {
			auth: { isAuthenticated: true, user: { id: "u-a" } },
		});
		sse.sent.length = 0;

		await publisher.broadcast("news", { headline: "it works" });
		await Promise.resolve();

		expect(sse.sent).toEqual([
			{ event: "news", data: { headline: "it works" } },
		]);
	});

	it("drops a payload that is not the envelope it publishes", async () => {
		const { client, handlers } = fakeRedis();
		const relay = new Relay({
			allowUnauthorizedChannels: true,
			transport: transports.redis({ connection: client }),
		});
		await Promise.resolve();

		// A stray producer on the bus channel, and a truncated message. Neither
		// may take down the subscription the next real broadcast needs.
		for (const handler of handlers.get("relay::broadcast") ?? []) {
			expect(() =>
				handler("not json at all", "relay::broadcast"),
			).not.toThrow();
			expect(() =>
				handler(JSON.stringify({ hello: "world" }), "relay::broadcast"),
			).not.toThrow();
		}

		expect(relay).toBeInstanceOf(Relay);
	});

	it("closes the connection on shutdown, and only if it opened one", async () => {
		const { client } = fakeRedis();
		const unused = new RedisRelayTransport(() => client);
		await unused.disconnect();
		expect(client.quits).toBe(0);

		const used = new RedisRelayTransport(() => client);
		await used.publish("relay::broadcast", { type: "broadcast" });
		await used.disconnect();
		expect(client.quits).toBe(1);
	});

	it("resolves the client once, however many broadcasts follow", async () => {
		const { client } = fakeRedis();
		const resolve = vi.fn(() => client);
		const transport = new RedisRelayTransport(resolve);

		await transport.subscribe("relay::broadcast", () => {});
		await transport.publish("relay::broadcast", { type: "broadcast" });
		await transport.publish("relay::broadcast", { type: "broadcast" });

		expect(resolve).toHaveBeenCalledTimes(1);
	});

	it("says what is missing when a connection name has nothing to resolve", async () => {
		// Either quasar is absent, or it is present and nothing registered its
		// provider yet. Both name the thing to fix, and neither publishes into
		// a connection that does not exist.
		const transport = transports.redis({ connection: "main" })();
		await expect(
			transport.publish("relay::broadcast", { type: "broadcast" }),
		).rejects.toThrow(/quasar|redis/i);
	});
});

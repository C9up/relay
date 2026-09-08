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
	/** Every `unsubscribe` call, so a test can see WHAT was removed. */
	const unsubscribed: Array<
		[string, ((message: string, channel: string) => void) | undefined]
	> = [];
	const client: RelayPubSubClient & {
		quit: () => void;
		quits: number;
		unsubscribed: typeof unsubscribed;
	} = {
		quits: 0,
		unsubscribed,
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
		unsubscribe(channel, handler) {
			unsubscribed.push([channel, handler]);
			// Like a real shared client: named, only that listener goes.
			if (handler) {
				const set = handlers.get(channel);
				set?.delete(handler);
				if (set && set.size > 0) return;
			}
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
		// Constructing opens nothing now; the bus starts in `ready()`.
		await publisher.startTransport();
		await subscriber.startTransport();

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

	it("closes a connection it OWNS on shutdown, and only if it opened one", async () => {
		const { client } = fakeRedis();
		const unused = new RedisRelayTransport(() => client, true);
		await unused.disconnect();
		expect(client.quits).toBe(0);

		const used = new RedisRelayTransport(() => client, true);
		await used.publish("relay::broadcast", { type: "broadcast" });
		await used.disconnect();
		expect(client.quits).toBe(1);
	});

	it("never closes a connection it only borrowed", async () => {
		// `quit()` on a shared @c9up/quasar connection closes BOTH its sockets,
		// so relay shutting down took the cache, the sessions and the queues
		// down with it. Ownership is opt-in for exactly this reason.
		const { client } = fakeRedis();
		const borrowed = new RedisRelayTransport(() => client);

		await borrowed.publish("relay::broadcast", { type: "broadcast" });
		await borrowed.disconnect();

		expect(client.quits).toBe(0);
	});

	it("removes its own listener, not every listener on the channel", async () => {
		// Unsubscribing without naming a handler means "drop everything on this
		// channel" — on a shared client that silences the application's own
		// subscriptions the moment relay stops.
		const { client } = fakeRedis();
		const transport = new RedisRelayTransport(() => client);

		await transport.subscribe("relay::broadcast", () => {});
		await transport.unsubscribe("relay::broadcast");

		expect(client.unsubscribed).toHaveLength(1);
		const [channel, handler] = client.unsubscribed[0] ?? [];
		expect(channel).toBe("relay::broadcast");
		expect(typeof handler).toBe("function");
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

describe("relay > a failed resolution is not kept forever", () => {
	const fakeClient = () => ({
		publish: async () => undefined,
		subscribe: async () => undefined,
		unsubscribe: async () => undefined,
	});

	it("tries again after a blip, instead of failing for the life of the process", async () => {
		let attempts = 0;
		const transport = new RedisRelayTransport(() => {
			attempts += 1;
			if (attempts === 1) throw new Error("ECONNREFUSED");
			return fakeClient();
		});

		await expect(transport.publish("c", {})).rejects.toThrow(/ECONNREFUSED/);

		// Caching the rejected promise made one blip at start-up permanent:
		// every later call failed instantly, with the original error, long
		// after Redis came back.
		await expect(transport.publish("c", {})).resolves.toBeUndefined();
		expect(attempts).toBe(2);
	});

	it("still shares one in-flight resolution between concurrent callers", async () => {
		let attempts = 0;
		const transport = new RedisRelayTransport(async () => {
			attempts += 1;
			await new Promise((resolve) => setTimeout(resolve, 10));
			return fakeClient();
		});

		await Promise.all([
			transport.publish("a", {}),
			transport.publish("b", {}),
			transport.subscribe("c", () => {}),
		]);

		// Forgetting only the FAILURE — three concurrent calls must not open
		// three connections.
		expect(attempts).toBe(1);
	});

	it("keeps a successful resolution", async () => {
		let attempts = 0;
		const transport = new RedisRelayTransport(() => {
			attempts += 1;
			return fakeClient();
		});

		await transport.publish("a", {});
		await transport.publish("b", {});

		expect(attempts).toBe(1);
	});
});

describe("relay > unsubscribing what was never subscribed", () => {
	it("does not touch the client when relay has no listener on the channel", async () => {
		// `unsubscribe(channel)` with no handler means "drop everything here" on
		// a shared connection. Called when relay never subscribed — a failed
		// ready(), or a second shutdown — it would cut the cache's and the
		// sessions' listeners instead of relay's.
		const { client } = fakeRedis();
		const transport = new RedisRelayTransport(() => client);

		await transport.unsubscribe("relay::broadcast");

		expect(client.unsubscribed).toHaveLength(0);
	});

	it("is idempotent: the second call removes nothing", async () => {
		const { client } = fakeRedis();
		const transport = new RedisRelayTransport(() => client);
		await transport.subscribe("relay::broadcast", () => {});

		await transport.unsubscribe("relay::broadcast");
		await transport.unsubscribe("relay::broadcast");

		expect(client.unsubscribed).toHaveLength(1);
	});

	it("removes nothing MORE after a subscribe that failed", async () => {
		// The failed subscribe takes its own registration back off — it cannot
		// know whether the client kept one. What must not happen is a LATER
		// unsubscribe reaching the client again: on a shared connection that is
		// relay dropping listeners it never owned, and on a cold one it opens a
		// connection just to close it.
		const { client } = fakeRedis();
		const failing = {
			...client,
			subscribe(
				_c: string,
				_h: unknown,
				options?: { onError?: (e: unknown) => void },
			) {
				options?.onError?.(new Error("no route to the bus"));
			},
		};
		const transport = new RedisRelayTransport(() => failing);
		await expect(
			transport.subscribe("relay::broadcast", () => {}),
		).rejects.toThrow();
		const afterRollback = client.unsubscribed.length;

		await transport.unsubscribe("relay::broadcast");

		expect(client.unsubscribed).toHaveLength(afterRollback);
	});

	it("removes only the handler it is given", async () => {
		// The client STACKS subscriptions. Removing by channel alone takes down
		// a listener this transport does not own; keeping one wrapper per
		// channel loses track of the other and leaves it delivering.
		const { client } = fakeRedis();
		const transport = new RedisRelayTransport(() => client);
		const first: Array<unknown> = [];
		const second: Array<unknown> = [];
		const a = (m: unknown): void => {
			first.push(m);
		};
		const b = (m: unknown): void => {
			second.push(m);
		};
		await transport.subscribe("relay::broadcast", a);
		await transport.subscribe("relay::broadcast", b);

		await transport.unsubscribe("relay::broadcast", a);
		await transport.publish("relay::broadcast", { type: "broadcast" });

		expect(first).toHaveLength(0);
		expect(second).toHaveLength(1);
	});

	it("forgets a connection it has closed", async () => {
		// `quit()` leaves the client unusable, and the resolved promise was
		// kept: an application that stopped and started again in one process —
		// a hot reload, a test — came back up on the socket it had just shut
		// and never reached the bus again.
		const { client } = fakeRedis();
		let resolutions = 0;
		const source = (): RelayPubSubClient => {
			resolutions += 1;
			// Closed state is PER CONNECTION, as a real client's is: reusing a
			// quit one is exactly what this test is about.
			let closed = false;
			const live = {
				...client,
				quit: () => {
					closed = true;
				},
			};
			return {
				publish: (c, m) => live.publish(c, m),
				subscribe: (c, h, o) => {
					if (closed) throw new Error("the connection is closed");
					return live.subscribe(c, h, o);
				},
				unsubscribe: (c, h) => live.unsubscribe(c, h),
				quit: () => live.quit(),
			};
		};
		const transport = new RedisRelayTransport(source, true);
		await transport.subscribe("relay::broadcast", () => {});
		await transport.disconnect();

		await expect(
			transport.subscribe("relay::broadcast", () => {}),
		).resolves.toBeUndefined();
		expect(resolutions).toBe(2);
	});

	it("forgets a subscription the client rejected outright", async () => {
		// The wrapper is recorded BEFORE the client is called, so relay can name
		// it if the call resolves — and the failure path takes it back off,
		// because the client may have registered it before failing. What is
		// pinned here is that a LATER unsubscribe adds nothing: the record is
		// gone, so relay does not ask a shared client to drop a listener it
		// never owned.
		const { client } = fakeRedis();
		const refusing: RelayPubSubClient = {
			...client,
			subscribe() {
				throw new Error("no route to the bus");
			},
		};
		const transport = new RedisRelayTransport(() => refusing);
		const handler = (): void => {};

		await expect(
			transport.subscribe("relay::broadcast", handler),
		).rejects.toThrow("no route to the bus");
		const afterRollback = client.unsubscribed.length;

		await transport.unsubscribe("relay::broadcast", handler);
		expect(client.unsubscribed).toHaveLength(afterRollback);
	});

	it("keeps a subscription the client refused to remove", async () => {
		// Bookkeeping was cleared BEFORE the client was asked. A rejecting
		// unsubscribe then left a live subscription nothing could name again —
		// not to retry it, not to remove it at shutdown.
		const { client } = fakeRedis();
		let refuse = true;
		const flaky: RelayPubSubClient = {
			...client,
			unsubscribe(channel, handler) {
				if (refuse) throw new Error("the connection is busy");
				return client.unsubscribe(channel, handler);
			},
		};
		const transport = new RedisRelayTransport(() => flaky);
		const handler = (): void => {};
		await transport.subscribe("relay::broadcast", handler);

		await expect(
			transport.unsubscribe("relay::broadcast", handler),
		).rejects.toThrow("busy");

		// The retry still knows what to remove.
		refuse = false;
		await transport.unsubscribe("relay::broadcast", handler);
		expect(client.unsubscribed).toHaveLength(1);
	});

	it("takes back a registration the client kept while failing", async () => {
		// A client that registers the callback and THEN fails — an ambiguous
		// network result, or a duck-typed adapter that does its bookkeeping
		// first. Forgetting the wrapper left it live and unnameable: the later
		// unsubscribe did nothing at all.
		const { client } = fakeRedis();
		const half: RelayPubSubClient = {
			...client,
			subscribe(channel, handler, options) {
				client.subscribe(channel, handler, options);
				throw new Error("no route to the bus");
			},
		};
		const transport = new RedisRelayTransport(() => half);
		const handler = (): void => {};

		await expect(
			transport.subscribe("relay::broadcast", handler),
		).rejects.toThrow("no route to the bus");

		// Whatever it registered has been taken back off.
		expect(client.unsubscribed).toHaveLength(1);
		await transport.publish("relay::broadcast", { type: "broadcast" });
	});

	it("keeps a registration it could not take back", async () => {
		// The compensation failed too, so the subscription may still be live —
		// and the only way a later shutdown can name it is if the record stays.
		const { client } = fakeRedis();
		let refuse = true;
		const half: RelayPubSubClient = {
			...client,
			subscribe(channel, handler, options) {
				client.subscribe(channel, handler, options);
				throw new Error("no route to the bus");
			},
			unsubscribe(channel, handler) {
				if (refuse) throw new Error("the connection is busy");
				return client.unsubscribe(channel, handler);
			},
		};
		const transport = new RedisRelayTransport(() => half);
		const handler = (): void => {};
		await expect(
			transport.subscribe("relay::broadcast", handler),
		).rejects.toThrow("no route to the bus");

		refuse = false;
		await transport.unsubscribe("relay::broadcast", handler);

		expect(client.unsubscribed).toHaveLength(1);
	});
});

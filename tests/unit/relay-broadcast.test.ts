/**
 * Tests for the Transmit-parity broadcast surface:
 *   - broadcastExcept(channel, payload, senderUid) skips the excluded uid(s)
 *   - multi-instance sync via an injected `transport` bus
 *   - shutdown() releases the bus
 *   - on() returns a detacher
 *   - getSubscribersFor(channel) lists the subscribed uids
 */

import { describe, expect, it, vi } from "vitest";
import {
	Relay,
	type RelaySseStream,
	type RelayTransport,
} from "../../src/Relay.js";

/** Minimal SSE double that records every `send(event, data)`. */
function fakeSse(id = `s_${Math.random()}`): RelaySseStream & {
	sent: Array<{ event: string; data: unknown }>;
} {
	let open = true;
	let closeCb: (() => void) | undefined;
	const sent: Array<{ event: string; data: unknown }> = [];
	return {
		id,
		sent,
		isOpen: () => open,
		async send(event, data) {
			sent.push({ event, data });
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

/** Subscribe a fresh authenticated client and return its stream + uid. */
async function connectAndSubscribe(
	r: Relay,
	userId: string,
	channel: string,
): Promise<{ sse: ReturnType<typeof fakeSse>; uid: string }> {
	const sse = fakeSse(`s-${userId}`);
	const outcome = r.connect(undefined, sse, {
		auth: { isAuthenticated: true, user: { id: userId } },
	});
	if (outcome.outcome !== "ok") throw new Error("connect failed");
	const sub = await r.subscribe(outcome.uid, channel, {
		auth: { isAuthenticated: true, user: { id: userId } },
	});
	if (!sub.ok) throw new Error("subscribe failed");
	// Drop the `connected` frame so assertions only see broadcasts.
	sse.sent.length = 0;
	return { sse, uid: outcome.uid };
}

describe("relay-broadcast > broadcastExcept", () => {
	it("delivers to every subscriber except the excluded uid", async () => {
		const r = new Relay({ allowUnauthorizedChannels: true });
		const a = await connectAndSubscribe(r, "u-a", "room/1");
		const b = await connectAndSubscribe(r, "u-b", "room/1");

		const reached = r.broadcastExcept("room/1", { hi: true }, a.uid);

		expect(reached).toBe(1);
		expect(a.sse.sent).toEqual([]); // excluded sender got nothing
		expect(b.sse.sent).toEqual([{ event: "room/1", data: { hi: true } }]);
	});

	it("accepts an array of excluded uids", async () => {
		const r = new Relay({ allowUnauthorizedChannels: true });
		const a = await connectAndSubscribe(r, "u-a", "room/1");
		const b = await connectAndSubscribe(r, "u-b", "room/1");
		const c = await connectAndSubscribe(r, "u-c", "room/1");

		const reached = r.broadcastExcept("room/1", { n: 1 }, [a.uid, b.uid]);

		expect(reached).toBe(1);
		expect(a.sse.sent).toEqual([]);
		expect(b.sse.sent).toEqual([]);
		expect(c.sse.sent).toEqual([{ event: "room/1", data: { n: 1 } }]);
	});

	it("does NOT mirror onto the bus (local-only, like Transmit)", async () => {
		const publish = vi.fn();
		const transport: RelayTransport = { publish, subscribe: vi.fn() };
		const r = new Relay({ allowUnauthorizedChannels: true, transport });
		const a = await connectAndSubscribe(r, "u-a", "room/1");

		r.broadcastExcept("room/1", { x: 1 }, a.uid);

		expect(publish).not.toHaveBeenCalled();
	});
});

describe("relay-broadcast > multi-instance transport sync", () => {
	it("publishes every broadcast onto the bus envelope", async () => {
		const publish = vi.fn();
		const transport: RelayTransport = { publish, subscribe: vi.fn() };
		const r = new Relay({ transport, transportChannel: "custom::bus" });

		r.broadcast("news", { headline: "hi" });

		expect(publish).toHaveBeenCalledWith("custom::bus", {
			type: "broadcast",
			channel: "news",
			payload: { headline: "hi" },
		});
	});

	it("re-delivers a bus message to THIS instance's local clients (no re-publish loop)", async () => {
		// Capture the bus handler so the test can inject a remote message.
		let busHandler: ((message: unknown) => void) | undefined;
		const publish = vi.fn();
		const transport: RelayTransport = {
			publish,
			subscribe: (_channel, handler) => {
				busHandler = handler;
			},
		};
		const r = new Relay({ allowUnauthorizedChannels: true, transport });
		const a = await connectAndSubscribe(r, "u-a", "room/1");

		// A broadcast originating on ANOTHER instance arrives over the bus.
		busHandler?.({
			type: "broadcast",
			channel: "room/1",
			payload: { remote: 1 },
		});

		expect(a.sse.sent).toEqual([{ event: "room/1", data: { remote: 1 } }]);
		// Re-delivery is local-only — it must NOT re-publish (would loop forever).
		expect(publish).not.toHaveBeenCalled();
	});

	it("drops malformed bus messages instead of trusting them", async () => {
		let busHandler: ((message: unknown) => void) | undefined;
		const transport: RelayTransport = {
			publish: vi.fn(),
			subscribe: (_channel, handler) => {
				busHandler = handler;
			},
		};
		const r = new Relay({ allowUnauthorizedChannels: true, transport });
		const a = await connectAndSubscribe(r, "u-a", "room/1");

		busHandler?.(null);
		busHandler?.({ type: "nope" });
		busHandler?.({ type: "broadcast", channel: 42, payload: {} });

		expect(a.sse.sent).toEqual([]);
	});

	it("shutdown() unsubscribes from and disconnects the bus", async () => {
		const unsubscribe = vi.fn();
		const disconnect = vi.fn();
		const transport: RelayTransport = {
			publish: vi.fn(),
			subscribe: vi.fn(),
			unsubscribe,
			disconnect,
		};
		const r = new Relay({ transport, transportChannel: "custom::bus" });

		await r.shutdown();

		expect(unsubscribe).toHaveBeenCalledWith("custom::bus");
		expect(disconnect).toHaveBeenCalled();
	});

	it("shutdown() is a no-op without a transport", async () => {
		const r = new Relay();
		await expect(r.shutdown()).resolves.toBeUndefined();
	});
});

describe("relay-broadcast > on() detacher + getSubscribersFor", () => {
	it("on() returns a detacher that stops further events", () => {
		const r = new Relay();
		const seen: string[] = [];
		const off = r.on("broadcast", (evt) => seen.push(evt.channel));

		r.broadcast("a", {});
		off();
		r.broadcast("b", {});

		expect(seen).toEqual(["a"]);
	});

	it("getSubscribersFor lists the uids subscribed to a channel", async () => {
		const r = new Relay({ allowUnauthorizedChannels: true });
		const a = await connectAndSubscribe(r, "u-a", "room/1");
		const b = await connectAndSubscribe(r, "u-b", "room/1");

		expect(r.getSubscribersFor("room/1").sort()).toEqual([a.uid, b.uid].sort());
		expect(r.getSubscribersFor("empty")).toEqual([]);
	});
});

describe("relay > a transport that cannot subscribe says so", () => {
	it("reports instead of failing silently", async () => {
		const written: string[] = [];
		const original = process.stderr.write.bind(process.stderr);
		process.stderr.write = (chunk: string | Uint8Array): boolean => {
			written.push(String(chunk));
			return true;
		};
		try {
			const transport: RelayTransport = {
				publish: async () => {},
				subscribe: async () => {
					throw new Error("no route to the bus");
				},
			};
			new Relay({ transport });
			await new Promise((resolve) => setTimeout(resolve, 10));

			// A failed subscribe is how an instance stops hearing the others: it
			// keeps serving its own clients and misses everything published
			// elsewhere. Unawaited, that split-brain was invisible.
			expect(written.join("")).toContain("will not receive messages");
		} finally {
			process.stderr.write = original;
		}
	});
});

describe("relay > a delivery that rejects", () => {
	it("drops the client and keeps broadcasting to the others", async () => {
		const written: string[] = [];
		const original = process.stderr.write.bind(process.stderr);
		process.stderr.write = (chunk: string | Uint8Array): boolean => {
			written.push(String(chunk));
			return true;
		};
		const rejections: unknown[] = [];
		const onUnhandled = (reason: unknown): void => {
			rejections.push(reason);
		};
		process.on("unhandledRejection", onUnhandled);
		try {
			const r = new Relay({ allowUnauthorizedChannels: true });
			const good = await connectAndSubscribe(r, "u-good", "room/1");
			const bad = await connectAndSubscribe(r, "u-bad", "room/1");
			// This client's socket write fails from here on.
			bad.sse.send = async () => {
				throw new Error("socket gone");
			};

			r.broadcast("room/1", { n: 1 });
			await new Promise((resolve) => setTimeout(resolve, 10));

			// The healthy client still got it, and the failing write did not
			// surface as an unhandled rejection — which on a default Node ends
			// the process, so one dead socket ended everybody's stream.
			expect(good.sse.sent).toEqual([
				{ event: "room/1", data: { n: 1 } },
			]);
			expect(rejections).toEqual([]);
			expect(written.join("")).toContain("delivery to");
			// …and the client that cannot receive is no longer subscribed.
			expect(r.getSubscribersFor("room/1")).toEqual([good.uid]);
		} finally {
			process.stderr.write = original;
			process.off("unhandledRejection", onUnhandled);
		}
	});
})

describe("relay > rejections that used to escape", () => {
	/** Capture unhandled rejections and stderr for the duration of `fn`. */
	async function watch(fn: () => Promise<void>) {
		const written: string[] = [];
		const rejections: unknown[] = [];
		const originalWrite = process.stderr.write.bind(process.stderr);
		const onUnhandled = (reason: unknown): void => {
			rejections.push(reason);
		};
		process.stderr.write = (chunk: string | Uint8Array): boolean => {
			written.push(String(chunk));
			return true;
		};
		process.on("unhandledRejection", onUnhandled);
		try {
			await fn();
			await new Promise((resolve) => setTimeout(resolve, 15));
		} finally {
			process.stderr.write = originalWrite;
			process.off("unhandledRejection", onUnhandled);
		}
		return { written: written.join(""), rejections };
	}

	it("drops a client whose `connected` frame fails", async () => {
		const r = new Relay({ allowUnauthorizedChannels: true });
		const sse = fakeSse("s-bad-connect");
		sse.send = async () => {
			throw new Error("socket gone before the first frame");
		};

		const { rejections, written } = await watch(async () => {
			const outcome = r.connect(undefined, sse, {
				auth: { isAuthenticated: true, user: { id: "u" } },
			});
			expect(outcome.outcome).toBe("ok");
		});

		// Without the uid from that frame the client can neither subscribe nor
		// unsubscribe — it is connected to nothing it can use.
		expect(rejections).toEqual([]);
		expect(written).toContain("connected frame");
		expect(r.getSubscribersFor("anything")).toEqual([]);
	});

	it("reports a transport whose subscribe throws SYNCHRONOUSLY", async () => {
		// `Promise.resolve(x())` calls `x()` first, so a synchronous throw never
		// reached the `.catch` — it came straight out of `new Relay(...)`.
		const transport: RelayTransport = {
			publish: async () => {},
			subscribe: () => {
				throw new Error("no connection on this client");
			},
		};

		const { rejections, written } = await watch(async () => {
			expect(() => new Relay({ transport })).not.toThrow();
		});

		expect(rejections).toEqual([]);
		expect(written).toContain("will not receive messages");
	});
})

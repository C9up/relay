/**
 * Lifecycle events carry the request context, as Transmit's do.
 *
 * `connect`, `disconnect`, `subscribe` and `unsubscribe` used to carry the uid
 * alone, so a listener could not tell WHO connected or subscribed — presence and
 * audit logging had to keep a uid → user map fed from somewhere else.
 */

import { describe, expect, it } from "vitest";
import {
	Relay,
	type RelayContext,
	type RelayLifecycleEvents,
	type RelaySseStream,
} from "../../src/Relay.js";

function stream(): RelaySseStream {
	let open = true;
	let onClose: (() => void) | undefined;
	return {
		id: `s-${Math.random()}`,
		isOpen: () => open,
		send: async () => true,
		onClose: (cb) => {
			onClose = cb;
		},
		end: async () => {
			open = false;
			onClose?.();
		},
	};
}

describe("relay > lifecycle events carry the request context", () => {
	it("hands each listener the context its request was made with", async () => {
		const relay = new Relay({ allowUnauthorizedChannels: true });
		const seen: Array<{ event: string; context: RelayContext }> = [];
		const record =
			<E extends "connect" | "disconnect" | "subscribe" | "unsubscribe">(
				event: E,
			) =>
			(evt: RelayLifecycleEvents[E]) => {
				seen.push({ event, context: evt.context });
			};
		relay.on("connect", record("connect"));
		relay.on("subscribe", record("subscribe"));
		relay.on("unsubscribe", record("unsubscribe"));
		relay.on("disconnect", record("disconnect"));

		const auth = { isAuthenticated: true, user: { id: "u1" } };
		const connectCtx: RelayContext = { auth };
		const subscribeCtx: RelayContext = { auth };
		const unsubscribeCtx: RelayContext = { auth };
		const sse = stream();
		const connected = relay.connect(sse, connectCtx);
		if (connected.outcome !== "ok") throw new Error("capped");
		await relay.subscribe(connected.uid, "news", subscribeCtx);
		relay.unsubscribe(connected.uid, "news", unsubscribeCtx);
		await sse.end();

		expect(seen.map((s) => s.event)).toEqual([
			"connect",
			"subscribe",
			"unsubscribe",
			"disconnect",
		]);
		// The very objects, not copies: a listener reads whatever a middleware
		// put on the context, as an authorizer does.
		expect(seen[0]?.context).toBe(connectCtx);
		expect(seen[1]?.context).toBe(subscribeCtx);
		expect(seen[2]?.context).toBe(unsubscribeCtx);
		// A disconnect has no request of its own: it is the connection's.
		expect(seen[3]?.context).toBe(connectCtx);
	});

	it("still carries the uid and channel beside it", async () => {
		const relay = new Relay({ allowUnauthorizedChannels: true });
		const subscribed: Array<RelayLifecycleEvents["subscribe"]> = [];
		relay.on("subscribe", (evt) => {
			subscribed.push(evt);
		});
		const ctx: RelayContext = {};
		const connected = relay.connect(stream(), ctx);
		if (connected.outcome !== "ok") throw new Error("capped");

		await relay.subscribe(connected.uid, "news", ctx);

		expect(subscribed).toEqual([
			{ uid: connected.uid, channel: "news", context: ctx },
		]);
	});
});

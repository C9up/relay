import { describe, expect, it, vi } from "vitest";
import { Hub, type HubContext } from "../../src/Hub.js";
import { SignalRAdapter } from "../../src/SignalRAdapter.js";

const RS = "\x1e";

class TestHub extends Hub {
	public lastEcho?: { from: string; data: unknown };

	async onEcho(ctx: HubContext, data: unknown) {
		this.lastEcho = { from: ctx.clientId, data };
	}

	async onBoom() {
		throw new Error("boom");
	}
}

function makeAdapter(): {
	hub: TestHub;
	adapter: SignalRAdapter;
	clientId: string;
} {
	const hub = new TestHub();
	const adapter = new SignalRAdapter(hub);
	const clientId = "client-1";
	hub.registerClient({
		id: clientId,
		groups: new Set(),
		auth: { isAuthenticated: false },
		send: () => {},
	});
	return { hub, adapter, clientId };
}

describe("relay > SignalRAdapter", () => {
	it("negotiates the transport it can actually serve", () => {
		// Server-Sent Events by default: that is what relay provides, and
		// announcing WebSockets when nothing can upgrade sends every client
		// down a road that dead-ends.
		const adapter = new SignalRAdapter(new TestHub());
		const res = adapter.negotiate("abc-123");
		expect(res.connectionId).toBe("abc-123");
		expect(res.connectionToken).toBeDefined();
		expect(res.connectionToken).not.toBe(res.connectionId);
		expect(res.negotiateVersion).toBe(1);
		expect(res.availableTransports[0]?.transport).toBe("ServerSentEvents");
		expect(res.availableTransports[0]?.transferFormats).toContain("Text");
	});

	it("announces whatever transports the caller can serve", () => {
		const adapter = new SignalRAdapter(new TestHub());
		const res = adapter.negotiate("abc-123", ["WebSockets", "LongPolling"]);
		expect(res.availableTransports.map((t) => t.transport)).toEqual([
			"WebSockets",
			"LongPolling",
		]);
	});

	describe("containsClose", () => {
		it("detects a real Close (type 7) frame", () => {
			const frames = [JSON.stringify({ type: 7 }) + RS];
			expect(SignalRAdapter.containsClose(frames)).toBe(true);
		});

		it("detects a Close packed alongside other messages in one frame", () => {
			const frame =
				JSON.stringify({ type: 6 }) + RS + JSON.stringify({ type: 7 }) + RS;
			expect(SignalRAdapter.containsClose([frame])).toBe(true);
		});

		it("does NOT false-positive on an argument string containing '\"type\":7'", () => {
			// An invocation whose argument literally contains the substring
			// must not be mistaken for a Close frame.
			const frame =
				JSON.stringify({
					type: 1,
					target: "log",
					arguments: ['{"type":7} is just text in a payload'],
				}) + RS;
			expect(SignalRAdapter.containsClose([frame])).toBe(false);
		});

		it("does NOT false-positive on an error message containing the substring", () => {
			const frame =
				JSON.stringify({
					type: 3,
					invocationId: "1",
					error: 'unexpected "type":7 token in upstream',
				}) + RS;
			expect(SignalRAdapter.containsClose([frame])).toBe(false);
		});

		it("returns false for non-Close frames + ignores non-JSON fragments", () => {
			expect(
				SignalRAdapter.containsClose([
					JSON.stringify({ type: 6 }) + RS,
					`not-json${RS}`,
					"",
				]),
			).toBe(false);
		});
	});

	it("completes the JSON handshake with an empty object", async () => {
		const { adapter, clientId } = makeAdapter();
		const handshake = JSON.stringify({ protocol: "json", version: 1 }) + RS;
		const out = await adapter.handleFrame(clientId, handshake);
		expect(out).toEqual([`{}${RS}`]);
	});

	it("rejects an invalid handshake", async () => {
		const { adapter, clientId } = makeAdapter();
		const bad = JSON.stringify({ protocol: "messagepack" }) + RS;
		const out = await adapter.handleFrame(clientId, bad);
		// The error response, then a Close: the protocol ends the connection on a
		// failed handshake, and the Close is how the transport learns to.
		expect(out).toHaveLength(2);
		expect(out[0]).toContain("Invalid handshake");
		expect(SignalRAdapter.containsClose(out)).toBe(true);
	});

	it("rejects handshake with unsupported version", async () => {
		const { adapter, clientId } = makeAdapter();
		const bad = JSON.stringify({ protocol: "json", version: 2 }) + RS;
		const out = await adapter.handleFrame(clientId, bad);
		expect(out).toHaveLength(2);
		expect(out[0]).toContain("Invalid handshake");
		expect(SignalRAdapter.containsClose(out)).toBe(true);
	});

	it("dispatches an Invocation (type 1) to the matching hub method", async () => {
		const { hub, adapter, clientId } = makeAdapter();
		await adapter.handleFrame(
			clientId,
			JSON.stringify({ protocol: "json", version: 1 }) + RS,
		);

		const inv =
			JSON.stringify({
				type: 1,
				invocationId: "i1",
				target: "echo",
				arguments: [{ message: "hello" }],
			}) + RS;
		const out = await adapter.handleFrame(clientId, inv);

		expect(hub.lastEcho).toEqual({
			from: clientId,
			data: { message: "hello" },
		});
		expect(out).toHaveLength(1);
		const completion = JSON.parse(out[0]?.replace(RS, ""));
		expect(completion).toEqual({ type: 3, invocationId: "i1" });
	});

	it("a throwing handler with an invocationId answers with an error Completion, not success", async () => {
		const { adapter, clientId } = makeAdapter();
		await adapter.handleFrame(
			clientId,
			JSON.stringify({ protocol: "json", version: 1 }) + RS,
		);
		// onBoom throws; before the fix the client got a SUCCESS Completion
		// {type:3,invocationId} and the real failure only as an uncorrelated
		// 'error' event (audit 2026-06-13).
		const inv =
			JSON.stringify({
				type: 1,
				invocationId: "boom-1",
				target: "boom",
				arguments: [],
			}) + RS;
		const out = await adapter.handleFrame(clientId, inv);
		expect(out).toHaveLength(1);
		const completion = JSON.parse(out[0]?.replace(RS, ""));
		expect(completion.type).toBe(3);
		expect(completion.invocationId).toBe("boom-1");
		expect(completion.error).toBeDefined();
		expect(completion).not.toHaveProperty("result");
	});

	it("a throwing handler WITHOUT an invocationId emits no Completion (fire-and-forget)", async () => {
		const { adapter, clientId } = makeAdapter();
		await adapter.handleFrame(
			clientId,
			JSON.stringify({ protocol: "json", version: 1 }) + RS,
		);
		const inv = JSON.stringify({ type: 1, target: "boom", arguments: [] }) + RS;
		const out = await adapter.handleFrame(clientId, inv);
		expect(out).toHaveLength(0);
	});

	it("responds to a Ping (type 6) with a Ping", async () => {
		const { adapter, clientId } = makeAdapter();
		await adapter.handleFrame(
			clientId,
			JSON.stringify({ protocol: "json", version: 1 }) + RS,
		);
		const out = await adapter.handleFrame(
			clientId,
			JSON.stringify({ type: 6 }) + RS,
		);
		expect(out).toHaveLength(1);
		expect(JSON.parse(out[0]?.replace(RS, ""))).toEqual({ type: 6 });
	});

	it("handles batched messages in one frame (multiple records)", async () => {
		const { hub, adapter, clientId } = makeAdapter();
		const handshake = JSON.stringify({ protocol: "json", version: 1 }) + RS;
		const ping = JSON.stringify({ type: 6 }) + RS;
		const inv =
			JSON.stringify({ type: 1, target: "echo", arguments: ["x"] }) + RS;

		const out = await adapter.handleFrame(clientId, handshake + ping + inv);
		// Handshake response + ping response (invocation has no invocationId so no completion)
		expect(out.length).toBe(2);
		expect(hub.lastEcho?.data).toBe("x");
	});

	it("returns Close(type 7) on malformed message after handshake", async () => {
		const { adapter, clientId } = makeAdapter();
		await adapter.handleFrame(
			clientId,
			JSON.stringify({ protocol: "json", version: 1 }) + RS,
		);
		const out = await adapter.handleFrame(clientId, `{not-json}${RS}`);
		expect(out).toHaveLength(1);
		const parsed = JSON.parse(out[0]?.replace(RS, ""));
		expect(parsed.type).toBe(7);
		expect(parsed.error).toContain("Malformed");
	});

	it("encodes server-initiated invocations with the record separator", () => {
		const adapter = new SignalRAdapter(new TestHub());
		const frame = adapter.encodeInvocation("taskUpdated", [{ id: 1 }]);
		expect(frame.endsWith(RS)).toBe(true);
		const parsed = JSON.parse(frame.slice(0, -1));
		expect(parsed).toEqual({
			type: 1,
			target: "taskUpdated",
			arguments: [{ id: 1 }],
		});
	});

	it("forget() drops ALL connection tokens issued for a clientId, not just the first (audit 2026-05-22)", () => {
		const adapter = new SignalRAdapter(new TestHub());
		const r1 = adapter.negotiate("alice");
		const r2 = adapter.negotiate("alice");
		const r3 = adapter.negotiate("alice");
		// All three tokens are independently resolvable before forget.
		expect(adapter.resolveToken(r1.connectionToken)).toBe("alice");
		expect(adapter.resolveToken(r2.connectionToken)).toBe("alice");
		expect(adapter.resolveToken(r3.connectionToken)).toBe("alice");

		adapter.forget("alice");

		// All three must be invalidated. Previously only the first map entry
		// was removed and the older tokens stayed resolvable.
		expect(adapter.resolveToken(r1.connectionToken)).toBeUndefined();
		expect(adapter.resolveToken(r2.connectionToken)).toBeUndefined();
		expect(adapter.resolveToken(r3.connectionToken)).toBeUndefined();
	});

	it("forgets handshake state on close (type 7) so a new connection re-handshakes", async () => {
		const { adapter, clientId } = makeAdapter();
		await adapter.handleFrame(
			clientId,
			JSON.stringify({ protocol: "json", version: 1 }) + RS,
		);
		await adapter.handleFrame(clientId, JSON.stringify({ type: 7 }) + RS);
		// After close, next inbound frame should be treated as handshake again
		const out = await adapter.handleFrame(
			clientId,
			JSON.stringify({ type: 1, target: "echo", arguments: ["nope"] }) + RS,
		);
		// Not a valid handshake → rejected
		expect(out[0]).toContain("Invalid handshake");
	});
});

class MultiArgHub extends Hub {
	public seen: unknown[] = [];

	async onSum(_ctx: HubContext, ...args: unknown[]) {
		this.seen = args;
	}

	async onOne(_ctx: HubContext, first: unknown) {
		this.seen = [first];
	}
}

function multiArgAdapter(): {
	hub: MultiArgHub;
	adapter: SignalRAdapter;
	clientId: string;
} {
	const hub = new MultiArgHub();
	const adapter = new SignalRAdapter(hub);
	const clientId = "client-multi";
	hub.registerClient({
		id: clientId,
		groups: new Set(),
		auth: { isAuthenticated: false },
		send: () => {},
	});
	return { hub, adapter, clientId };
}

async function doHandshake(
	adapter: SignalRAdapter,
	clientId: string,
): Promise<void> {
	await adapter.handleFrame(
		clientId,
		`${JSON.stringify({ protocol: "json", version: 1 })}${RS}`,
	);
}

describe("relay > SignalRAdapter invocation arguments", () => {
	it("passes every argument, not just the first", async () => {
		const { hub, adapter, clientId } = multiArgAdapter();
		await doHandshake(adapter, clientId);

		await adapter.handleFrame(
			clientId,
			`${JSON.stringify({
				type: 1,
				target: "sum",
				arguments: [1, 2, 3],
			})}${RS}`,
		);

		// `connection.invoke('Sum', 1, 2, 3)` used to reach the handler as `1`.
		// The other two were dropped without a word, which is data loss the
		// caller cannot see.
		expect(hub.seen).toEqual([1, 2, 3]);
	});

	it("leaves a one-parameter handler working unchanged", async () => {
		const { hub, adapter, clientId } = multiArgAdapter();
		await doHandshake(adapter, clientId);

		await adapter.handleFrame(
			clientId,
			`${JSON.stringify({ type: 1, target: "one", arguments: [{ a: 1 }, "extra"] })}${RS}`,
		);

		expect(hub.seen).toEqual([{ a: 1 }]);
	});

	it("handles an invocation with no arguments at all", async () => {
		const { hub, adapter, clientId } = multiArgAdapter();
		await doHandshake(adapter, clientId);

		await adapter.handleFrame(
			clientId,
			`${JSON.stringify({ type: 1, target: "sum", arguments: [] })}${RS}`,
		);

		expect(hub.seen).toEqual([]);
	});
});

describe("relay > SignalRAdapter unsupported message types", () => {
	it("answers a stream invocation with an error Completion", async () => {
		const { adapter, clientId } = multiArgAdapter();
		await doHandshake(adapter, clientId);

		const out = await adapter.handleFrame(
			clientId,
			`${JSON.stringify({
				type: 4,
				invocationId: "inv-9",
				target: "sum",
				arguments: [],
			})}${RS}`,
		);

		// Dropped, the client's observable waits for the lifetime of the
		// connection: the protocol promises a Completion, so there is no
		// timeout to fall back on.
		const messages = out
			.join("")
			.split(RS)
			.filter(Boolean)
			.map((m) => JSON.parse(m));
		const completion = messages.find((m) => m.type === 3);
		expect(completion).toMatchObject({
			invocationId: "inv-9",
			error: expect.stringContaining("Streaming is not supported"),
		});
	});

	it("ignores a stream item and a cancellation without answering", async () => {
		const { adapter, clientId } = multiArgAdapter();
		await doHandshake(adapter, clientId);

		const out = await adapter.handleFrame(
			clientId,
			`${JSON.stringify({ type: 2, invocationId: "inv-9", item: 1 })}${RS}` +
				`${JSON.stringify({ type: 5, invocationId: "inv-9" })}${RS}`,
		);

		// Neither carries a reply in the protocol: the item belongs to a stream
		// whose opening invocation was already refused, and the cancel targets a
		// stream that was never started.
		expect(out.join("")).toBe("");
	});
});

describe("relay > SignalRAdapter negotiate is an unauthenticated write", () => {
	/**
	 * A SignalR connection is negotiated first and connected second, so a token
	 * always exists for a moment before anything uses it. Nothing ever closed
	 * that window: `negotiate` wrote an entry and only a stream CLOSING removed
	 * one, so a caller that never opened a stream left the entry for the life
	 * of the process. A thousand POSTs, a thousand live tokens, forever.
	 */
	it("expires a token nothing ever connected with", () => {
		const adapter = new SignalRAdapter(new TestHub(), { tokenTtlMs: 50 });
		const stale = adapter.negotiate("c1").connectionToken;
		expect(adapter.resolveToken(stale)).toBe("c1");

		vi.useFakeTimers();
		try {
			vi.setSystemTime(Date.now() + 51);
			expect(adapter.resolveToken(stale)).toBeUndefined();
		} finally {
			vi.useRealTimers();
		}
	});

	it("keeps a token whose stream attached, however long it lives", () => {
		const adapter = new SignalRAdapter(new TestHub(), { tokenTtlMs: 50 });
		const token = adapter.negotiate("c1").connectionToken;
		adapter.claimToken(token);

		vi.useFakeTimers();
		try {
			vi.setSystemTime(Date.now() + 60_000);
			// Sweeping runs on negotiate; a live connection must survive it.
			adapter.negotiate("c2");
			expect(adapter.resolveToken(token)).toBe("c1");
		} finally {
			vi.useRealTimers();
		}
	});

	it("caps how many unclaimed tokens may wait at once", () => {
		const adapter = new SignalRAdapter(new TestHub(), { maxPendingTokens: 4 });
		const tokens = Array.from(
			{ length: 20 },
			(_, i) => adapter.negotiate(`c${i}`).connectionToken,
		);

		const live = tokens.filter((t) => adapter.resolveToken(t) !== undefined);

		// The cap is applied before the new token is written, so the newest and
		// the four before it stand; the oldest are dropped.
		expect(live.length).toBeLessThanOrEqual(5);
		expect(live).toContain(tokens[19]);
	});

	it("forgets every token a client accumulated, claimed or not", () => {
		const adapter = new SignalRAdapter(new TestHub());
		const first = adapter.negotiate("c1").connectionToken;
		const second = adapter.negotiate("c1").connectionToken;
		adapter.claimToken(second);

		adapter.forget("c1");

		expect(adapter.resolveToken(first)).toBeUndefined();
		expect(adapter.resolveToken(second)).toBeUndefined();
	});
});

describe("relay > SignalRAdapter answers every invocation that asked", () => {
	async function handshook(): Promise<SignalRAdapter> {
		const { adapter, clientId } = makeAdapter();
		await adapter.handleFrame(clientId, `{"protocol":"json","version":1}${RS}`);
		return adapter;
	}

	// The protocol promises a Completion for an invocationId, and the caller
	// has no timeout to fall back on. Dropped, the client's invoke() waited for
	// the life of the connection — the same hang the stream branch answers.
	it("completes an invocation whose target is not a method name", async () => {
		const adapter = await handshook();

		const out = await adapter.handleFrame(
			"client-1",
			`{"type":1,"invocationId":"7","target":42,"arguments":[]}${RS}`,
		);

		expect(out.map((f) => JSON.parse(f.slice(0, -1)))).toEqual([
			{ type: 3, invocationId: "7", error: "Invalid invocation" },
		]);
	});

	it("says nothing for a bad invocation that asked for nothing", async () => {
		const adapter = await handshook();

		expect(
			await adapter.handleFrame("client-1", `{"type":1,"target":42}${RS}`),
		).toEqual([]);
	});

	it("closes on a message whose type is not a number", async () => {
		const adapter = await handshook();

		const out = await adapter.handleFrame("client-1", `{"type":"1"}${RS}`);

		// It used to fall through every case and answer with silence.
		expect(out.map((f) => JSON.parse(f.slice(0, -1)))).toEqual([
			{ type: 7, error: "Malformed message" },
		]);
	});

	it("completes a stream invocation that is malformed as well as one that is not", async () => {
		const adapter = await handshook();

		const out = await adapter.handleFrame(
			"client-1",
			`{"type":4,"invocationId":"9"}${RS}`,
		);

		expect(out.map((f) => JSON.parse(f.slice(0, -1)))).toEqual([
			{ type: 3, invocationId: "9", error: "Invalid stream invocation" },
		]);
	});
});

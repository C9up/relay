/**
 * In-memory `Relay` substitute for tests — captures every
 * `broadcast(channel, data)` call and exposes Adonis/Laravel-style
 * `assertSent` / `assertNotSent` helpers in the same shape as
 * Rover's `FakeMail`, Bay's `FakeQueue`, and Spectrum's
 * `FakeLogger`.
 *
 * The fake implements only the `broadcast` slice of the real
 * `Relay` surface — that's what the spec asks for and what tests
 * actually need to assert on. If a future story needs subscription
 * APIs, extend then.
 *
 * Not re-exported from the main barrel; reach via
 * `@c9up/relay/testing`.
 */

export interface CapturedBroadcast {
	channel: string;
	data: unknown;
	/** Always 0 in fake mode — no real clients connected. Kept on
	 *  the entry so tests asserting on "0 clients reached" can do
	 *  so without a separate API. */
	clients: number;
}

export interface FakeRelayPredicate {
	/** Custom data predicate. */
	dataMatches?: (data: unknown) => boolean;
}

export type FakeRelayPredicateArg =
	| FakeRelayPredicate
	| ((event: CapturedBroadcast) => boolean);

export class FakeRelay {
	#captured: CapturedBroadcast[] = [];

	/** Mirrors the real `Relay.broadcast` signature — captures the
	 *  call and returns 0 (no real clients in fake mode).
	 *
	 *  `data` is captured as the real relay sends it: through JSON, which
	 *  also makes it a copy a caller mutating the original cannot alter.
	 *  A payload JSON cannot carry (a cycle, a BigInt) throws here, as it
	 *  throws at the real `broadcast()`; a function field is dropped, as it
	 *  is on the wire. `structuredClone` accepted the first and threw on the
	 *  second — the reverse of the relay it stands in for. */
	broadcast(channel: string, data: unknown): number {
		// `undefined` becomes null on the real relay, because that is what JSON
		// can carry. A fake that captured it verbatim would let an assertion
		// pass on a payload the real broadcast never sends.
		return this.#capture(channel, data === undefined ? null : data);
	}

	/** Mirrors the real `Relay.broadcastExcept` — captures the call
	 *  (the excluded uid(s) are irrelevant to assertions, since the fake
	 *  holds no real clients) and returns 0. An absent payload is captured
	 *  as null, which is what the real one puts on the wire and the bus. */
	broadcastExcept(
		channel: string,
		data: unknown,
		_senderUid: string | string[],
	): number {
		return this.#capture(channel, data === undefined ? null : data);
	}

	#capture(channel: string, data: unknown): number {
		this.#captured.push({
			channel,
			data: data === undefined ? undefined : onTheWire(data),
			clients: 0,
		});
		return 0;
	}

	/** Defensive snapshot of every captured broadcast. */
	getSent(): CapturedBroadcast[] {
		return this.#captured.map((c) => ({
			...c,
			data: c.data === undefined ? undefined : deepClone(c.data),
		}));
	}

	reset(): void {
		this.#captured = [];
	}

	assertSent(channel: string, predicate?: FakeRelayPredicateArg): void {
		const match = makeMatcher(channel, predicate);
		if (this.#captured.some(match)) return;
		throw new Error(
			`relay.assertSent('${channel}'${describePredicate(predicate)}) failed — no captured broadcast matches.\n${describeCaptured(this.#captured)}`,
		);
	}

	assertNotSent(channel: string, predicate?: FakeRelayPredicateArg): void {
		const match = makeMatcher(channel, predicate);
		if (!this.#captured.some(match)) return;
		throw new Error(
			`relay.assertNotSent('${channel}'${describePredicate(predicate)}) failed — at least one captured broadcast matches.\n${describeCaptured(this.#captured)}`,
		);
	}
}

function makeMatcher(
	channel: string,
	predicate: FakeRelayPredicateArg | undefined,
): (e: CapturedBroadcast) => boolean {
	if (typeof predicate === "function") {
		return (e) => e.channel === channel && predicate(e);
	}
	if (predicate === undefined) {
		return (e) => e.channel === channel;
	}
	return (e) => {
		if (e.channel !== channel) return false;
		if (predicate.dataMatches && !predicate.dataMatches(e.data)) return false;
		return true;
	};
}

function describePredicate(
	predicate: FakeRelayPredicateArg | undefined,
): string {
	if (predicate === undefined) return "";
	if (typeof predicate === "function") return ", <function predicate>";
	if (Object.keys(predicate).length === 0) {
		return ", <empty predicate (channel-only)>";
	}
	return `, ${safeStringify(predicate)}`;
}

function describeCaptured(captured: CapturedBroadcast[]): string {
	if (captured.length === 0) return "Captured: (none)";
	const lines = captured.map(
		(c, i) => `  [${i}] channel="${c.channel}" data=${safeStringify(c.data)}`,
	);
	return `Captured (${captured.length}):\n${lines.join("\n")}`;
}

/**
 * `JSON.stringify` for an assertion message: a function renders as
 * `<function>`, a BigInt as `<digits>n`, a cycle as `<circular>`, and a value
 * that still cannot be written as `<unstringifiable>` — so a failure message
 * is never eaten by a JSON throw.
 *
 * Only a cycle is cut. `path` holds the objects from the root to the value
 * being written, so a value reached through two keys is written under both, as
 * `JSON.stringify` writes it; remembering every object seen dropped the second
 * one. The same rule as `@c9up/ream`'s `src/utils/safeStringify.ts`, kept as a
 * local copy because this message format differs from a response body's.
 */
function safeStringify(value: unknown): string {
	const path: object[] = [];
	try {
		const out: string | undefined = JSON.stringify(
			value,
			function replace(this: unknown, _key: string, v: unknown) {
				if (typeof v === "function") return "<function>";
				if (typeof v === "bigint") return `${v}n`;
				if (typeof v !== "object" || v === null) return v;
				while (path.length > 0 && path[path.length - 1] !== this) path.pop();
				if (path.includes(v)) return "<circular>";
				path.push(v);
				return v;
			},
		);
		return out ?? "undefined";
	} catch {
		return "<unstringifiable>";
	}
}

function deepClone<T>(value: T): T {
	return structuredClone(value);
}

/** What the real relay's `encodePayload` puts in a frame, read back. */
function onTheWire(value: unknown): unknown {
	const encoded: string | undefined = JSON.stringify(value);
	return JSON.parse(encoded ?? "null");
}

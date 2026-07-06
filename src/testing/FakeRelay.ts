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
	 *  `data` is deep-cloned via `structuredClone` so a caller mutating
	 *  the original object after broadcasting cannot retroactively
	 *  alter the captured snapshot. */
	broadcast(channel: string, data: unknown): number {
		this.#captured.push({
			channel,
			data: data === undefined ? undefined : deepClone(data),
			clients: 0,
		});
		return 0;
	}

	/** Mirrors the real `Relay.broadcastExcept` — captures the call
	 *  (the excluded uid(s) are irrelevant to assertions, since the fake
	 *  holds no real clients) and returns 0. */
	broadcastExcept(
		channel: string,
		data: unknown,
		_senderUid: string | string[],
	): number {
		return this.broadcast(channel, data);
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

/** `JSON.stringify` with circular-ref + function-field handling.
 *  Functions render as `<function>`, circular refs as `<circular>`,
 *  unstringifiable values as `<unstringifiable>` — so an assertion
 *  failure message never gets eaten by a JSON throw. */
function safeStringify(value: unknown): string {
	const seen = new WeakSet<object>();
	try {
		return JSON.stringify(value, (_key, v: unknown) => {
			if (typeof v === "function") return "<function>";
			if (typeof v === "object" && v !== null) {
				if (seen.has(v)) return "<circular>";
				seen.add(v);
			}
			return v;
		});
	} catch {
		return "<unstringifiable>";
	}
}

function deepClone<T>(value: T): T {
	return structuredClone(value);
}

/**
 * SignalR JSON Protocol Adapter — wraps a Hub with the SignalR wire format.
 *
 * Implements the subset of the SignalR JSON Protocol that real clients use:
 *   - Negotiate (HTTP POST /<endpoint>/negotiate)
 *   - Handshake (first message from client: { protocol: 'json', version: 1 })
 *   - Invocation (type 1): client → server method invocation
 *   - StreamItem (2) / CancelInvocation (5): accepted and ignored, which is
 *     what the protocol asks for once streaming has been refused — see the
 *     dispatch below. Server→client streaming itself is not offered: a hub
 *     method that returns a stream is answered with an explicit error, never
 *     with silence.
 *   - Ping (type 6): bidirectional keep-alive
 *   - Close (type 7): graceful disconnect
 *
 * The framing rule: every SignalR JSON message is terminated by 0x1E (record separator).
 * This adapter is transport-agnostic — it parses an inbound text frame and emits
 * outbound text frames; the caller is responsible for the WebSocket socket itself.
 *
 * @implements MISS-21 (Epic 23 — Story 23.2)
 */

import { randomUUID } from "node:crypto";
import type { Hub } from "./Hub.js";

/** SignalR record separator — every JSON message ends with this byte. */
const RS = "\x1e";
const DEFAULT_MAX_FRAME_SIZE = 65_536;
/**
 * How long a negotiated token stays valid before its stream attaches, and how
 * many may be waiting at once.
 *
 * A SignalR connection is negotiated first and connected second, so there is
 * always a window in which a token exists and nothing has used it. SignalR's
 * own server closes that window itself — a negotiated connection whose
 * transport never attaches is disposed after a timeout. Here it stayed open
 * forever: `negotiate` is an unauthenticated POST that writes an entry, and the
 * only thing that ever removed one was a stream closing. A thousand requests
 * left a thousand live tokens, and they were still live an hour later.
 */
const DEFAULT_TOKEN_TTL_MS = 30_000;
const DEFAULT_MAX_PENDING_TOKENS = 1_024;

/** A token handed out by `negotiate`, before or after its stream attaches. */
interface IssuedToken {
	clientId: string;
	issuedAt: number;
	/** True once a stream has been opened with it — no longer on the clock. */
	claimed: boolean;
}

export type SignalRMessageType = 1 | 2 | 3 | 4 | 5 | 6 | 7;

interface InvocationMessage {
	type: 1;
	invocationId?: string;
	target: string;
	arguments: unknown[];
	[key: string]: unknown;
}

interface CompletionMessage {
	type: 3;
	invocationId: string;
	result?: unknown;
	error?: string;
}

/**
 * A client asking the hub to STREAM results back (`connection.stream(...)`).
 *
 * Unlike a plain invocation, the invocationId is required: it is the handle the
 * client's observable is waiting on.
 */
interface StreamInvocationMessage {
	type: 4;
	invocationId: string;
	target: string;
	arguments?: unknown[];
	[key: string]: unknown;
}

interface PingMessage {
	type: 6;
}
interface CloseMessage {
	type: 7;
	error?: string;
}
interface HandshakeRequest {
	protocol: "json";
	version: number;
}

function isStreamInvocation(
	msg: Record<string, unknown>,
): msg is StreamInvocationMessage {
	return (
		msg.type === 4 &&
		typeof msg.invocationId === "string" &&
		typeof msg.target === "string"
	);
}

function isInvocationMessage(
	msg: Record<string, unknown>,
): msg is InvocationMessage {
	return msg.type === 1 && typeof msg.target === "string";
}

/** The first frame a client must send, and the only shape accepted for it. */
function isHandshakeRequest(value: unknown): value is HandshakeRequest {
	if (typeof value !== "object" || value === null) return false;
	return (
		Reflect.get(value, "protocol") === "json" &&
		Reflect.get(value, "version") === 1
	);
}

/** Any SignalR message, identified only by carrying a numeric `type`. */
function isTypedMessage(
	value: unknown,
): value is Record<string, unknown> & { type: number } {
	if (typeof value !== "object" || value === null) return false;
	return typeof Reflect.get(value, "type") === "number";
}

export interface NegotiateResponse {
	connectionId: string;
	connectionToken: string;
	negotiateVersion: number;
	availableTransports: Array<{ transport: string; transferFormats: string[] }>;
}

export class SignalRAdapter {
	#hub: Hub;
	#handshakes = new Set<string>(); // clientIds that have completed handshake
	#tokenToId: Map<string, IssuedToken> = new Map();
	#maxFrameSize: number;
	#tokenTtlMs: number;
	#maxPendingTokens: number;

	constructor(
		hub: Hub,
		options?: {
			maxFrameSize?: number;
			/** Milliseconds a negotiated token stays valid unclaimed. */
			tokenTtlMs?: number;
			/** How many unclaimed tokens may wait at once. */
			maxPendingTokens?: number;
		},
	) {
		this.#hub = hub;
		this.#maxFrameSize = options?.maxFrameSize ?? DEFAULT_MAX_FRAME_SIZE;
		this.#tokenTtlMs = options?.tokenTtlMs ?? DEFAULT_TOKEN_TTL_MS;
		this.#maxPendingTokens =
			options?.maxPendingTokens ?? DEFAULT_MAX_PENDING_TOKENS;
	}

	/**
	 * Build a SignalR `/negotiate` response for an HTTP POST.
	 *
	 * `transports` names what the caller can actually serve. The default is
	 * Server-Sent Events, which is the transport `@c9up/relay` provides and a
	 * first-class SignalR transport — announcing WebSockets when nothing can
	 * upgrade would send every client down a road that dead-ends.
	 */
	negotiate(
		connectionId: string,
		transports: readonly string[] = ["ServerSentEvents"],
	): NegotiateResponse {
		this.#expireTokens();
		const connectionToken = randomUUID();
		this.#tokenToId.set(connectionToken, {
			clientId: connectionId,
			issuedAt: Date.now(),
			claimed: false,
		});
		return {
			connectionId,
			connectionToken,
			negotiateVersion: 1,
			availableTransports: transports.map((transport) => ({
				transport,
				transferFormats: ["Text"],
			})),
		};
	}

	/** Resolve a connectionId from a connectionToken. Returns undefined if invalid. */
	resolveToken(connectionToken: string): string | undefined {
		const issued = this.#tokenToId.get(connectionToken);
		if (!issued) return undefined;
		if (!issued.claimed && this.#isExpired(issued)) {
			this.#tokenToId.delete(connectionToken);
			return undefined;
		}
		return issued.clientId;
	}

	/**
	 * Mark a token as in use — its stream has attached.
	 *
	 * From here the token lives as long as the connection does, and `forget`
	 * is what ends it. Only the unclaimed ones are on a clock, because only
	 * they can be created by someone who never intends to connect.
	 */
	claimToken(connectionToken: string): void {
		const issued = this.#tokenToId.get(connectionToken);
		if (issued) issued.claimed = true;
	}

	/**
	 * Drop unclaimed tokens that have run out of time, and, if too many are
	 * still waiting, the oldest of those. The sweep runs on `negotiate`, which
	 * is the only thing that grows the table.
	 */
	#expireTokens(): void {
		const pending: string[] = [];
		for (const [token, issued] of this.#tokenToId) {
			if (issued.claimed) continue;
			if (this.#isExpired(issued)) {
				this.#tokenToId.delete(token);
				continue;
			}
			pending.push(token);
		}
		// Insertion order is issue order, so the front of the list is the oldest.
		const excess = pending.length - this.#maxPendingTokens;
		for (let i = 0; i < excess; i++) {
			const token = pending[i];
			if (token !== undefined) this.#tokenToId.delete(token);
		}
	}

	#isExpired(issued: IssuedToken): boolean {
		return Date.now() - issued.issuedAt >= this.#tokenTtlMs;
	}

	/**
	 * Process an inbound frame from a client.
	 * Returns the outbound frames to send back (may be empty).
	 * Use `SignalRAdapter.containsClose(frames)` to check if the connection should be closed.
	 *
	 * The first frame must be a handshake `{"protocol":"json","version":1}<RS>`.
	 * Subsequent frames are SignalR messages (Invocation, Ping, Close).
	 */
	async handleFrame(clientId: string, frame: string): Promise<string[]> {
		if (frame.length > this.#maxFrameSize) {
			return [
				this.#encode({
					type: 7,
					error: "Frame too large",
				} satisfies CloseMessage),
			];
		}

		const messages = this.#splitFrames(frame);
		const out: string[] = [];

		for (const raw of messages) {
			if (!this.#handshakes.has(clientId)) {
				// Expecting handshake
				if (!isHandshakeRequest(this.#tryParse(raw))) {
					out.push(
						this.#encode({
							error:
								'Invalid handshake — expected {"protocol":"json","version":1}',
						}),
					);
					// The protocol ends the connection on a failed handshake. The
					// error response is the last thing a real client reads — it stops
					// there and closes — so the Close that follows is for the
					// transport, which had no other way to know the socket should go.
					// Without it the connection stayed open, with every later frame
					// re-read as another handshake attempt.
					out.push(
						this.#encode({
							type: 7,
							error: "Handshake failed",
						} satisfies CloseMessage),
					);
					return out;
				}
				this.#handshakes.add(clientId);
				out.push(this.#encode({})); // empty object = handshake OK
				continue;
			}

			// A message whose `type` is not a number is malformed by the same
			// rule as one that is not JSON: nothing below can act on it, and it
			// used to fall through every case without a word.
			const msg = this.#tryParse(raw);
			if (!isTypedMessage(msg)) {
				out.push(
					this.#encode({
						type: 7,
						error: "Malformed message",
					} satisfies CloseMessage),
				);
				continue;
			}

			switch (msg.type) {
				case 1: {
					// Invocation
					if (!isInvocationMessage(msg)) {
						// Type 1 without a usable target. Dropping it left an
						// invoke() waiting on a Completion the protocol promises and
						// nothing was going to send — the same hang the type-4
						// branch below was written to stop.
						const id = msg.invocationId;
						if (typeof id === "string") {
							out.push(
								this.#encodeCompletion(id, undefined, "Invalid invocation"),
							);
						}
						break;
					}
					const inv = msg;
					let ok = false;
					try {
						ok = await this.#hub.dispatch(
							clientId,
							inv.target,
							...(inv.arguments ?? []),
						);
					} catch {
						// dispatch() reports failures via its boolean return; guard
						// against an unexpected throw so one bad invocation can't
						// abort the whole frame batch.
						ok = false;
					}
					// Only a request-style invocation (with an invocationId) expects
					// a Completion. A failed handler MUST answer with an error
					// Completion, not a success one — else the client's invoke()
					// promise resolves as if the call succeeded.
					if (inv.invocationId) {
						out.push(
							ok
								? this.#encodeCompletion(inv.invocationId)
								: this.#encodeCompletion(
										inv.invocationId,
										undefined,
										"Handler error",
									),
						);
					}
					break;
				}
				case 4: {
					// StreamInvocation. Streaming is not implemented, and dropping
					// this left the client's observable pending for the lifetime of
					// the connection — the caller has no timeout to fall back on
					// because the protocol promises a Completion. Say so instead.
					const invocationId = msg.invocationId;
					// Nothing to answer to without one — and a stream invocation
					// that arrives without an invocationId is malformed anyway.
					if (typeof invocationId !== "string") break;
					out.push(
						this.#encodeCompletion(
							invocationId,
							undefined,
							isStreamInvocation(msg)
								? "Streaming is not supported by this hub"
								: "Invalid stream invocation",
						),
					);
					break;
				}
				// StreamItem (2) and CancelInvocation (5) carry no reply of their
				// own in the protocol: an item belongs to a stream whose opening
				// invocation was already answered with the error above, and a
				// cancel targets a stream that was never started. Ignoring them is
				// the correct response, not a gap.
				case 2:
				case 5:
					break;
				case 6: // Ping
					out.push(this.#encode({ type: 6 } satisfies PingMessage));
					break;
				case 7: // Close
					this.#handshakes.delete(clientId);
					break;
			}
		}

		return out;
	}

	/**
	 * True when any frame in the response is an actual Close (type 7)
	 * message. Each frame may pack several RS-separated JSON messages;
	 * we parse each and check its `type` field, instead of a raw
	 * `includes('"type":7')` substring scan — the latter false-positives
	 * whenever a JSON payload, error string, or invocation argument
	 * merely contains that text, closing a healthy connection.
	 */
	static containsClose(frames: string[]): boolean {
		for (const frame of frames) {
			for (const segment of frame.split(RS)) {
				if (segment.length === 0) continue;
				let parsed: unknown;
				try {
					parsed = JSON.parse(segment);
				} catch {
					// Non-JSON fragment — can't be a Close frame.
					continue;
				}
				if (
					typeof parsed === "object" &&
					parsed !== null &&
					Reflect.get(parsed, "type") === 7
				) {
					return true;
				}
			}
		}
		return false;
	}

	/** Build a server-initiated invocation frame to push to a client. */
	encodeInvocation(target: string, args: unknown[]): string {
		return this.#encode({
			type: 1,
			target,
			arguments: args,
		} satisfies InvocationMessage);
	}

	/** Forget a client's handshake state when it disconnects. */
	forget(clientId: string): void {
		this.#handshakes.delete(clientId);
		// Audit 2026-05-22: drop ALL tokens for this clientId, not just the
		// first match. A client that renegotiates several times (network
		// blip, reconnect after sleep, tab refresh) calls negotiate() each
		// time and accumulates entries in #tokenToId. With the previous
		// `break`, every disconnect cleared exactly one entry and the older
		// tokens stayed resolvable indefinitely — a stale-token surface that
		// a hijacker could replay after the client believes it's gone.
		for (const [token, issued] of this.#tokenToId) {
			if (issued.clientId === clientId) {
				this.#tokenToId.delete(token);
			}
		}
	}

	/** Split a transport frame on the SignalR record separator (0x1E). */
	#splitFrames(frame: string): string[] {
		return frame.split(RS).filter((s) => s.length > 0);
	}

	/** Parse a frame, or answer `undefined` when it is not JSON at all. */
	#tryParse(raw: string): unknown {
		try {
			return JSON.parse(raw);
		} catch {
			return undefined;
		}
	}

	#encode(obj: unknown): string {
		return JSON.stringify(obj) + RS;
	}

	#encodeCompletion(
		invocationId: string,
		result?: unknown,
		error?: string,
	): string {
		const msg: CompletionMessage = error
			? { type: 3, invocationId, error }
			: { type: 3, invocationId, result };
		return this.#encode(msg);
	}
}

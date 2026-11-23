/**
 * The errors of relay, one class per code. A caller tests `error.code` or
 * `error instanceof errors.E_RELAY_…`; the HTTP exception handler reads
 * `error.status`. The refusals relay answers over the wire
 * (`E_RELAY_NOT_OWNER`, `E_RELAY_MAX_CHANNELS`…) use the same codes.
 */

import { createError } from "@c9up/flare";

/** Two hub handlers answer the same invocation (names match case-insensitively). */
export const E_RELAY_DUPLICATE_HANDLER = createError<[string, string]>(
	"Hub handlers '%s' and '%s' answer the same invocation — method names are matched case-insensitively. Rename one.",
	"E_RELAY_DUPLICATE_HANDLER",
);

/** A hub instance was mounted twice. */
export const E_RELAY_HUB_ALREADY_MOUNTED = createError(
	"This hub instance is already mounted: give each mount its own instance, so a send crosses instances under one path.",
	"E_RELAY_HUB_ALREADY_MOUNTED",
);

/** The object mounted with `relay.hub()` does not extend `Hub`. */
export const E_RELAY_INVALID_HUB = createError<[string]>(
	'The object mounted at "%s" is not a Hub — it must extend Hub from @c9up/relay.',
	"E_RELAY_INVALID_HUB",
);

/** The adapter given to `relay.hub()` is not a `SignalRAdapter`. */
export const E_RELAY_INVALID_ADAPTER = createError<[string]>(
	'The adapter given for the hub at "%s" is not a SignalRAdapter.',
	"E_RELAY_INVALID_ADAPTER",
);

/** A hub is already mounted at that path. */
export const E_RELAY_HUB_PATH_TAKEN = createError<[string]>(
	'A hub is already mounted at "%s" — unmount it or pick another path.',
	"E_RELAY_HUB_PATH_TAKEN",
);

/** An invariant of relay broke. A bug. */
export const E_RELAY_INTERNAL = createError<[string]>(
	"[relay] %s",
	"E_RELAY_INTERNAL",
);

/** `FakeRelay.assertSent()` or `assertNotSent()` does not hold. */
export const E_RELAY_ASSERTION_FAILED = createError<[string, string, string]>(
	"relay.%s(%s) failed — %s",
	"E_RELAY_ASSERTION_FAILED",
);

/** The pub/sub client threw something that is not an `Error` while subscribing. */
export const E_RELAY_SUBSCRIBE_FAILED = createError<[string]>(
	"%s",
	"E_RELAY_SUBSCRIBE_FAILED",
);

/** The SignalR pending-connection table, or the owner's share of it, is full. */
export const E_RELAY_MAX_PENDING = createError(
	"Too many pending SignalR connections — the negotiate was refused.",
	"E_RELAY_MAX_PENDING",
	503,
);

/** The previous bus subscription could not be released before a new start. */
export const E_RELAY_TRANSPORT_UNRELEASED = createError<[string]>(
	"Relay.startTransport(): the previous subscription could not be released, so starting again would leave two handlers on the bus. Cause: %s",
	"E_RELAY_TRANSPORT_UNRELEASED",
);

/** A newer start or a shutdown overtook `startTransport()`. */
export const E_RELAY_TRANSPORT_SUPERSEDED = createError<[string]>(
	"Relay.startTransport() was superseded %s",
	"E_RELAY_TRANSPORT_SUPERSEDED",
);

/** `whenTransportReady()` was called before `startTransport()`. */
export const E_RELAY_TRANSPORT_NOT_STARTED = createError(
	"Relay.whenTransportReady() was called before startTransport(). The bus opens in the provider's ready() phase — nothing has subscribed yet.",
	"E_RELAY_TRANSPORT_NOT_STARTED",
);

/** `pingInterval` is neither milliseconds nor a duration. */
export const E_RELAY_INVALID_PING_INTERVAL = createError<[string]>(
	"[relay] pingInterval must be milliseconds or a duration like '30s', got '%s'",
	"E_RELAY_INVALID_PING_INTERVAL",
);

/** A channel pattern given to `authorize()` could never match. */
export const E_RELAY_INVALID_CHANNEL_PATTERN = createError<[string, string]>(
	"[relay] invalid channel pattern '%s': %s",
	"E_RELAY_INVALID_CHANNEL_PATTERN",
);

/** A Redis transport names a connection and `@c9up/quasar` is not installed. */
export const E_RELAY_QUASAR_MISSING = createError<[string]>(
	"%s",
	"E_RELAY_QUASAR_MISSING",
);

/** The quasar connection lacks a command the transport issues. */
export const E_RELAY_REDIS_INCOMPLETE = createError<[string]>(
	"%s",
	"E_RELAY_REDIS_INCOMPLETE",
);

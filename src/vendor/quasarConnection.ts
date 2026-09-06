// Generated from scripts/vendor/quasarConnection.ts — do not edit.
//
// This package is published and built from its own repository, so the file
// has to exist here rather than be imported. `pnpm vendor:sync` rewrites it,
// and `pnpm vendor:check` fails if this copy has drifted from the original.

/**
 * Resolving a Redis connection by name, from `@c9up/quasar`.
 *
 * No package here depends on quasar: it is an optional peer, and this module
 * never imports it statically — the specifier is built at runtime so the
 * TypeScript build stays free of it too. A host that never names a connection
 * neither installs it nor pays for it.
 *
 * What varies between callers is passed in rather than copied: which commands
 * they will issue, and what they are doing with them. Both end up in the
 * message, because "the connection is missing lpop" is only useful next to
 * "which the queue's atomic pop issues".
 */

/** The slice of quasar's manager this needs: a connection, by name. */
interface ConnectionSource {
	connection(name?: string): unknown;
}

/**
 * Read a member without assuming how the object answers.
 *
 * Two shapes have to work, and they disagree about `in`. Quasar's accessor is
 * a Proxy over an empty null-prototype object with only a `get` trap, so
 * `"connection" in manager` is FALSE while reading it returns the function —
 * gating on `in` rejected the real manager. And a module namespace under a
 * test double RAISES for an export it does not have, rather than answering
 * undefined, so a bare read fails on the mock. Reading through a catch is what
 * satisfies both.
 */
function readMember(value: unknown, name: string): unknown {
	if (typeof value !== "object" || value === null) return undefined;
	try {
		return Reflect.get(value, name);
	} catch {
		return undefined;
	}
}

function isConnectionSource(value: unknown): value is ConnectionSource {
	return typeof readMember(value, "connection") === "function";
}

export interface QuasarConnectionRequest {
	/** The package asking. It prefixes every message this can raise. */
	pkg: string;
	/**
	 * How this package builds an error.
	 *
	 * Not optional, and not `new Error` by default: a package with its own
	 * error type carries codes (`E_<PKG>_<REASON>`) that a caller catches on,
	 * and a shared helper throwing a bare Error would silently drop them. The
	 * two reasons are distinguished so the codes can be too.
	 */
	raise: (
		reason: "quasar-missing" | "incomplete-connection",
		message: string,
		cause?: unknown,
	) => Error;
	/** Connection name, or `undefined` for quasar's default. */
	name?: string;
	/** The commands the caller will actually issue. */
	required: readonly string[];
	/** What the caller does with them, for the message that names a gap. */
	what: string;
	/**
	 * A second way out, when this package has one.
	 *
	 * Several of them accept a client object directly, so "pass a client
	 * instead of a connection name" is actionable — and it was in each
	 * package's own message before this was shared. Dropping it to unify the
	 * wording would have made the error strictly less useful.
	 */
	alternative?: string;
}

/**
 * The named connection, checked for the commands the caller needs BEFORE it is
 * handed over — a connection missing one would otherwise fail on the first
 * command, far from the line that asked for it.
 */
export async function quasarConnection<T>(
	request: QuasarConnectionRequest,
): Promise<T> {
	const { pkg, name, required, what, raise, alternative } = request;
	const specifier = "@c9up/quasar/services/main";
	let loaded: unknown;
	try {
		loaded = await import(/* @vite-ignore */ specifier);
	} catch (cause) {
		throw raise(
			"quasar-missing",
			`[${pkg}] ${what} asks for the quasar connection "${name ?? "default"}", but @c9up/quasar is not installed.\n` +
				`  pnpm add @c9up/quasar${alternative ? `, ${alternative}` : ""}`,
			cause,
		);
	}

	const manager = isConnectionSource(loaded)
		? loaded
		: Reflect.get(Object(loaded), "default");
	if (!isConnectionSource(manager)) {
		throw raise(
			"quasar-missing",
			`[${pkg}] @c9up/quasar/services/main did not expose a connection() manager`,
		);
	}

	const connection = manager.connection(name);
	const missing = required.filter(
		(command) => typeof readMember(connection, command) !== "function",
	);
	if (missing.length > 0) {
		throw raise(
			"incomplete-connection",
			`[${pkg}] the quasar connection${name ? ` '${name}'` : ""} is missing ${missing.join(", ")}, which ${what} issues`,
		);
	}
	// Load-bearing: every member of `required` has just been checked to be a
	// function on this object. The caller names the type it needs because only
	// the caller knows which commands it asked for.
	return connection as T;
}

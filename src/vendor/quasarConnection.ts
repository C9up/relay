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
 * undefined, so a bare read fails on the mock.
 *
 * So the read is caught, but what it raised is kept: on quasar's accessor a
 * throw is not an absence, it is the accessor saying its provider has not
 * booted, and dropping that turned it into a wrong diagnosis.
 */
type MemberRead = { value: unknown } | { threw: unknown };

function readMember(value: unknown, name: string): MemberRead {
	if (typeof value !== "object" || value === null) return { value: undefined };
	try {
		return { value: Reflect.get(value, name) };
	} catch (threw) {
		return { threw };
	}
}

function memberValue(read: MemberRead): unknown {
	return "value" in read ? read.value : undefined;
}

function isConnectionSource(value: unknown): value is ConnectionSource {
	return typeof memberValue(readMember(value, "connection")) === "function";
}

/**
 * The commands among `commands` that `value` does not carry as functions.
 *
 * Reads go through the same catch as above, so a command whose read throws
 * counts as missing.
 */
export function missingCommands(
	value: unknown,
	commands: readonly string[],
): string[] {
	return commands.filter(
		(command) => typeof memberValue(readMember(value, command)) !== "function",
	);
}

/**
 * Whether `value` carries every one of `commands` as a function.
 *
 * The building block of the guard a caller passes as `isConnection`, over the
 * same list it passes as `required` — which is what names the gap when the
 * guard says no.
 */
export function hasCommands(
	value: unknown,
	commands: readonly string[],
): value is object {
	if (typeof value !== "object" || value === null) return false;
	return missingCommands(value, commands).length === 0;
}

export interface QuasarConnectionRequest<T> {
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
	/** The commands the caller will actually issue, named in the message. */
	required: readonly string[];
	/**
	 * The caller's check that the connection is the client it needs.
	 *
	 * Written by the caller over the commands it issues (`hasCommands` with
	 * `required` does it), because only the caller knows its client type. The
	 * connection is handed over only once this has said yes.
	 */
	isConnection: (value: unknown) => value is T;
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
	request: QuasarConnectionRequest<T>,
): Promise<T> {
	const { pkg, name, required, what, raise, alternative, isConnection } =
		request;
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

	// The namespace itself may be the manager (a test double); a throw there is
	// the mock refusing an export it lacks, not an answer.
	const direct = memberValue(readMember(loaded, "connection"));
	const manager =
		typeof direct === "function"
			? loaded
			: memberValue(readMember(Object(loaded), "default"));
	const read = readMember(manager, "connection");
	if ("threw" in read) {
		throw raise(
			"quasar-missing",
			`[${pkg}] ${what} asks for the quasar connection "${name ?? "default"}", but @c9up/quasar's manager is not available yet: ` +
				"its provider has not booted, or is not registered in this application's providers.",
			read.threw,
		);
	}
	if (!isConnectionSource(manager)) {
		throw raise(
			"quasar-missing",
			`[${pkg}] @c9up/quasar/services/main did not expose a connection() manager`,
		);
	}

	const connection = manager.connection(name);
	const missing = missingCommands(connection, required);
	if (missing.length > 0) {
		throw raise(
			"incomplete-connection",
			`[${pkg}] the quasar connection${name ? ` '${name}'` : ""} is missing ${missing.join(", ")}, which ${what} issues`,
		);
	}
	if (!isConnection(connection)) {
		throw raise(
			"incomplete-connection",
			`[${pkg}] the quasar connection${name ? ` '${name}'` : ""} is not the client ${what} needs`,
		);
	}
	return connection;
}

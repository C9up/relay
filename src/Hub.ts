/**
 * Hub — bidirectional realtime communication over WebSocket.
 *
 * Extends Relay with a Hub pattern for bidirectional messaging.
 * Client → Server messages dispatched to Hub methods.
 * Server → Client via groups and direct sends.
 *
 * Usage:
 *   class TaskHub extends Hub {
 *     @inject()
 *     constructor(private taskService: TaskService) { super() }
 *
 *     async onTaskUpdate(ctx: HubContext, data: { taskId: string, status: string }) {
 *       const task = await this.taskService.transition(data.taskId, data.status)
 *       ctx.group(`residence:${task.residenceId}`).send('task:updated', task)
 *     }
 *
 *     async onJoin(ctx: HubContext, residenceId: string) {
 *       ctx.joinGroup(`residence:${residenceId}`)
 *     }
 *   }
 *
 * @implements MISS-20 (Epic 23)
 */

import {
	E_RELAY_DUPLICATE_HANDLER,
	E_RELAY_HUB_ALREADY_MOUNTED,
} from "./errors.js";

/**
 * What {@link Hub.dispatch} reports: the handler's result, or a failure.
 *
 * A failure carrying `denial` is the hub's guards refusing the caller. The
 * client is no longer registered on the hub by then, and the transport must
 * close its connection: the guards are what it takes to be connected.
 */
export type DispatchResult =
	| { ok: true; result: unknown }
	| { ok: false; denial?: HubDenial };

/**
 * The auth state a hub reads: the request's, as the transport hands it over.
 */
export interface HubAuth {
	isAuthenticated: boolean;
	/** The guard that authenticated the request. */
	strategy?: string;
	user?: Record<string, unknown>;
	roles?: string[];
	/**
	 * The permissions the user's payload carries. Informational when
	 * {@link hasAllPermissions} is there: a `permissions` guard asks that.
	 */
	permissions?: string[];
	/**
	 * The auth layer's own answer to "does this user hold every one of these
	 * permissions" — the request's `ctx.auth.hasAllPermissions`, when the auth
	 * layer resolves permissions itself. Asked again on every check, so a
	 * permission taken away is refused on the next one.
	 */
	hasAllPermissions?(permissions: readonly string[]): Promise<boolean>;
}

/**
 * A refusal by a hub's guards: the HTTP status a connection attempt is
 * answered with, and the code and message sent either way.
 *
 * `401` is a request that is not authenticated, or not through a guard the
 * hub accepts; `403` one that is, without the role or the permissions.
 */
export interface HubDenial {
	status: 401 | 403;
	code: "E_RELAY_UNAUTHORIZED" | "E_RELAY_FORBIDDEN";
	message: string;
}

export interface HubContext {
	/** Client ID. */
	readonly clientId: string;
	/**
	 * Auth state: the invoking request's in a handler dispatched with
	 * {@link Hub.dispatchAs}, the connection's otherwise (and in `onConnect`).
	 */
	readonly auth: HubAuth;
	/** Join a group. */
	joinGroup(group: string): void;
	/** Leave a group. */
	leaveGroup(group: string): void;
	/** Send to this client. */
	send(event: string, data: unknown): void;
	/** Send to all clients in a group. */
	group(name: string): { send(event: string, data: unknown): void };
	/** Send to all connected clients. */
	broadcast(event: string, data: unknown): void;
	/**
	 * End this connection: it receives nothing further, and its stream or
	 * socket is closed. See {@link Hub.disconnect}.
	 */
	disconnect(): void;
}

export interface HubGuardOptions {
	guard?: string;
	guards?: string[];
	roles?: string[];
	permissions?: string[];
}

/**
 * A group send or a broadcast, as it crosses to the other instances.
 *
 * Group memberships live on the instance the client is connected to, so a
 * send made on one instance has to be heard by every other one, which then
 * delivers it to ITS members. `target` names who: a group, or every client of
 * the hub.
 */
export interface HubFanOut {
	target: { kind: "group"; group: string } | { kind: "all" };
	event: string;
	data: unknown;
}

/**
 * A connection as a transport hands it to {@link Hub.registerClient}: its id,
 * the groups it joined, the auth it connected with, how to push an event to
 * it, and how to end it.
 */
export interface ConnectedHubClient {
	id: string;
	groups: Set<string>;
	auth: HubContext["auth"];
	send: (event: string, data: unknown) => void;
	/**
	 * Close the connection's stream or socket. {@link Hub.disconnect} calls it
	 * after taking the client off the hub; without it, a disconnect only does
	 * the latter and the transport is left holding an open, silent connection.
	 */
	close?: () => void;
}

/**
 * Base Hub class — extend to create typed hubs.
 *
 * Methods starting with "on" are auto-registered as message handlers. The
 * event name is the method name after "on", matched case-insensitively as
 * SignalR resolves hub methods: `onTaskUpdate` handles `taskUpdate`,
 * `TaskUpdate` and `TASKUPDATE`. (`onConnect` / `onDisconnect` are lifecycle
 * hooks and are not dispatchable.)
 */
export abstract class Hub {
	#clients: Map<string, ConnectedHubClient> = new Map();
	#groupIndex: Map<string, Set<string>> = new Map();
	/**
	 * Where a group send or a broadcast goes after the local delivery: the
	 * relay's bus, once the hub is mounted on a relay that has one. Without it
	 * a hub behind a load balancer reached only the clients of the instance
	 * that made the send, which looks right until the second replica.
	 */
	#fanOut: ((message: HubFanOut) => void) | undefined;
	#guards: { guards: string[]; roles: string[]; permissions: string[] } = {
		guards: [],
		roles: [],
		permissions: [],
	};
	/**
	 * Allowlist of dispatchable handler methods (populated at construction),
	 * keyed by the lowercased event name.
	 */
	#handlerMethods: Map<
		string,
		(ctx: HubContext, ...args: unknown[]) => Promise<void> | void
	> = new Map();

	/**
	 * Build the allowlist of dispatchable handlers.
	 *
	 * The whole prototype chain BELOW `Hub` is walked, not just the class's own
	 * prototype. A hub that extends another hub is an ordinary way to share
	 * handlers, and reading one level meant the inherited ones were answered
	 * with `E_RELAY_UNKNOWN_EVENT` — a handler that exists, is spelled right,
	 * and never runs.
	 *
	 * The walk STOPS at `Hub.prototype`, which is the point of the allowlist:
	 * nothing on this class or on `Object.prototype` is ever reachable from the
	 * wire, so `dispatch` cannot be talked into calling `useGuards`,
	 * `removeClient` or `constructor`. Subclasses win over their bases, as
	 * method resolution already says they do.
	 */
	constructor() {
		const reserved = new Set(["connect", "disconnect"]);
		/** Lowercased event → the method name that claimed it. */
		const claimedBy = new Map<string, string>();
		let proto: object | null = Object.getPrototypeOf(this);
		while (proto !== null && proto !== Hub.prototype) {
			for (const name of Object.getOwnPropertyNames(proto)) {
				if (!name.startsWith("on") || name.length <= 2) continue;
				// Lowercased because SignalR resolves a hub method name without
				// regard to case, and its clients invoke PascalCase names
				// (`invoke('Sum')`) that an exact lookup never found.
				const eventName = name.slice(2).toLowerCase();
				if (reserved.has(eventName)) continue;
				const claimant = claimedBy.get(eventName);
				if (claimant !== undefined) {
					// The same name lower in the chain is an override: the
					// subclass's wins, as method resolution already says.
					if (claimant === name) continue;
					// Two spellings of one event would leave the invocation to
					// whichever happened to be walked first.
					throw new E_RELAY_DUPLICATE_HANDLER([claimant, name]);
				}
				// Read the descriptor rather than the property: a getter would
				// otherwise be invoked here, during construction, just to find out
				// whether it answers a function.
				const handler = Object.getOwnPropertyDescriptor(proto, name)?.value;
				if (isHandler(handler)) {
					claimedBy.set(eventName, name);
					this.#handlerMethods.set(eventName, handler);
				}
			}
			proto = Object.getPrototypeOf(proto);
		}
	}

	/**
	 * What it takes to CONNECT to this hub. Normalizes singular guard/guards.
	 *
	 * A transport asks {@link authorize} before it issues a connection token
	 * and before it opens a stream or a socket: a client the guards refuse is
	 * never registered, never reaches `onConnect` and receives nothing. They
	 * are asked again on every invocation, so a connection that stops meeting
	 * them is ended at its next call — as `[Authorize]` on a hub class does in
	 * ASP.NET Core SignalR, the protocol's reference server.
	 *
	 * `roles` is satisfied by ANY of the names, `permissions` by ALL of them —
	 * the same split every other entry point in the framework uses.
	 */
	useGuards(options: HubGuardOptions): void {
		this.#guards = {
			guards: [
				...(options.guards ?? []),
				...(options.guard ? [options.guard] : []),
			],
			roles: options.roles ?? [],
			permissions: options.permissions ?? [],
		};
	}

	/** Called when a client connects. Override to customize. */
	async onConnect(_ctx: HubContext): Promise<void> {}

	/** Called when a client disconnects. Override to customize. */
	async onDisconnect(_clientId: string): Promise<void> {}

	/**
	 * Build a HubContext for a client, carrying `auth` — the invoking
	 * request's when there is one, the connection's otherwise.
	 */
	#buildContext(
		client: ConnectedHubClient,
		auth: HubContext["auth"] = client.auth,
	): HubContext {
		const ctx: HubContext = {
			clientId: client.id,
			auth,
			joinGroup: (group: string) => {
				client.groups.add(group);
				if (!this.#groupIndex.has(group))
					this.#groupIndex.set(group, new Set());
				this.#groupIndex.get(group)?.add(client.id);
			},
			leaveGroup: (group: string) => {
				client.groups.delete(group);
				const members = this.#groupIndex.get(group);
				if (members) {
					members.delete(client.id);
					if (members.size === 0) this.#groupIndex.delete(group);
				}
			},
			send: (event: string, data: unknown) => {
				client.send(event, data);
			},
			group: (name: string) => ({
				send: (event: string, data: unknown) => {
					this.#sendToGroup(name, event, data);
				},
			}),
			broadcast: (event: string, data: unknown) => {
				this.#broadcastAll(event, data);
			},
			disconnect: () => {
				this.#end(client);
			},
		};

		return ctx;
	}

	/**
	 * Register a client connection.
	 *
	 * A registration under an id that already has one REPLACES it, and takes
	 * the previous client's group memberships out of the index on the way. It
	 * used to overwrite the map entry and leave the index alone, which had two
	 * consequences, both silent: the new connection received everything sent to
	 * the groups the old one had joined — groups it never asked for and was
	 * never authorized for — and `removeClient` could not clean them up
	 * afterwards, because it walks the client's own group set and the client
	 * holding that set had been dropped.
	 *
	 * This does NOT ask the guards: it is the low-level half a transport calls
	 * once {@link authorize} has let the connection in.
	 */
	registerClient(client: ConnectedHubClient): HubContext {
		this.#forgetGroups(this.#clients.get(client.id));
		this.#clients.set(client.id, client);
		return this.#buildContext(client);
	}

	/**
	 * End a connection held by this instance: the client is taken off the hub
	 * at once — no broadcast or group send reaches it from here on — and its
	 * stream or socket is closed. Returns whether there was such a client.
	 *
	 * The guards are asked when a client connects and when it invokes; nothing
	 * asks them while it only listens. This is how an application ends a
	 * connection whose user logged out or lost a role in between.
	 */
	disconnect(clientId: string): boolean {
		const client = this.#clients.get(clientId);
		if (client === undefined) return false;
		this.#end(client);
		return true;
	}

	/**
	 * {@link disconnect} every connection this instance holds for the user with
	 * that id — what a logout or a revoked role calls. Returns how many were
	 * ended. Connections live on the instance that opened them, so each
	 * instance ends its own.
	 */
	disconnectUser(userId: string | number | bigint): number {
		const wanted = userIdKey(userId);
		if (wanted === undefined) return 0;
		let ended = 0;
		// A copy: ending a client removes it from the map being walked.
		for (const client of [...this.#clients.values()]) {
			if (
				client.auth.isAuthenticated &&
				userIdKey(client.auth.user?.id) === wanted
			) {
				this.#end(client);
				ended++;
			}
		}
		return ended;
	}

	/**
	 * Take a client off the hub, then close its transport. In that order: a
	 * socket reports its close later, and the client must not be delivered to
	 * in between.
	 */
	#end(client: ConnectedHubClient): void {
		// Only the registration this client object holds: a newer connection
		// under the same id is not this one's to end.
		if (this.#clients.get(client.id) === client) this.removeClient(client.id);
		try {
			client.close?.();
		} catch {
			// The client is off the hub either way; its transport's own failure
			// to close is not the caller's to handle.
		}
	}

	/**
	 * The auth recorded when `clientId` connected, or `undefined` when there is
	 * no such client. A transport uses it to check that a later request on the
	 * same connection comes from the identity the connection was opened with.
	 */
	authFor(clientId: string): HubContext["auth"] | undefined {
		return this.#clients.get(clientId)?.auth;
	}

	/** Remove a client. */
	removeClient(clientId: string): void {
		const client = this.#clients.get(clientId);
		if (client) {
			this.#forgetGroups(client);
			this.#clients.delete(clientId);
			this.onDisconnect(clientId).catch(() => {});
		}
	}

	/** Take a client out of every group index it is a member of. */
	#forgetGroups(client: ConnectedHubClient | undefined): void {
		if (!client) return;
		for (const group of client.groups) {
			const members = this.#groupIndex.get(group);
			if (!members) continue;
			members.delete(client.id);
			if (members.size === 0) this.#groupIndex.delete(group);
		}
	}

	/**
	 * Dispatch an incoming message to the appropriate handler method.
	 *
	 * Returns `{ ok: true, result }` when the handler ran to completion — its
	 * return value, which a SignalR `invoke()` resolves with — and
	 * `{ ok: false }` on any failure (unknown client/event, auth rejection, or
	 * a throwing handler). A bare `error` event is still emitted to the client
	 * for each failure (transport agnostic), and `ok` lets a correlated
	 * transport answer with an error Completion instead of a misleading
	 * success one.
	 */
	async dispatch(
		clientId: string,
		event: string,
		...args: unknown[]
	): Promise<DispatchResult> {
		return this.#dispatch(clientId, undefined, event, args);
	}

	/**
	 * {@link dispatch}, authorized against `auth` — the request that carries
	 * the invocation — rather than the auth recorded when the client connected.
	 *
	 * A transport whose invocations arrive on requests of their own (SignalR
	 * over SSE posts each one) uses this: the guards then see a revoked role on
	 * the next request, not at the next reconnect, and the handler's context
	 * carries the same auth the guards checked.
	 */
	async dispatchAs(
		clientId: string,
		auth: HubContext["auth"],
		event: string,
		args: readonly unknown[],
	): Promise<DispatchResult> {
		return this.#dispatch(clientId, auth, event, args);
	}

	async #dispatch(
		clientId: string,
		requestAuth: HubContext["auth"] | undefined,
		event: string,
		args: readonly unknown[],
	): Promise<DispatchResult> {
		const client = this.#clients.get(clientId);
		if (!client) return { ok: false };
		const auth = requestAuth ?? client.auth;

		const denial = await this.authorize(auth);
		if (denial) {
			deliver(client, "error", {
				code: denial.code,
				message: denial.message,
			});
			// The guards are what it takes to be connected, and this caller no
			// longer meets them: it stops receiving now. Closing the stream or
			// the socket is left to the transport, which still has the refusal
			// to write down it.
			if (this.#clients.get(clientId) === client) this.removeClient(clientId);
			return { ok: false, denial };
		}

		// Find handler from allowlist (never dispatches to lifecycle hooks or inherited methods)
		const handler = this.#handlerMethods.get(event.toLowerCase());
		if (!handler) {
			client.send("error", {
				code: "E_RELAY_UNKNOWN_EVENT",
				message: `No handler for: ${event}`,
			});
			return { ok: false };
		}

		const ctx = this.#buildContext(client, auth);
		try {
			// EVERY argument, not just the first. A SignalR client calling
			// `connection.invoke('Method', a, b, c)` sends three; passing only
			// `a` dropped the rest without a word. A handler declaring one
			// parameter is unaffected — JavaScript ignores the extras.
			const result: unknown = await handler.call(this, ctx, ...args);
			return { ok: true, result };
		} catch {
			client.send("error", {
				code: "E_RELAY_HANDLER_ERROR",
				message: "Handler error",
			});
			return { ok: false };
		}
	}

	/**
	 * Whether `auth` meets the hub's guards: `null` when it does — always, on a
	 * hub with none — or the refusal to answer with.
	 *
	 * A transport calls it where a connection is admitted (negotiate, the
	 * stream, the socket's handshake) and {@link dispatch} on every invocation.
	 *
	 * Roles are ANY-of and permissions are ALL-of, and the asymmetry is
	 * deliberate: a role names who someone is, and holding one of several
	 * accepted roles is enough; a permission names one thing they may do, and a
	 * hub asking for two of them needs both. It is the rule the rest of the
	 * framework applies at every other entry point — HTTP middleware, the RPC
	 * router, the GraphQL resolver guard — so a hub agrees with the route next
	 * to it. Written down here because reading `some` beside `every` otherwise
	 * looks like one of them is a typo.
	 *
	 * An error raised by the auth layer while it answers the permission
	 * question propagates: a resolver that cannot answer is never read as a
	 * grant.
	 */
	async authorize(auth: HubAuth): Promise<HubDenial | null> {
		// `useGuards` already folded the singular `guard` into `guards`: the
		// normalisation has exactly one home.
		const { guards, roles, permissions } = this.#guards;
		if (guards.length === 0 && roles.length === 0 && permissions.length === 0) {
			return null;
		}

		if (!auth.isAuthenticated) {
			return {
				status: 401,
				code: "E_RELAY_UNAUTHORIZED",
				message: "Not authenticated",
			};
		}

		// Still 401: the request is somebody, but no guard this hub accepts
		// vouches for it — what a route's `.guard()` answers too. The message
		// does not list the accepted guards: a refusal is not where a client
		// learns how the application is wired.
		if (
			guards.length > 0 &&
			(auth.strategy === undefined || !guards.includes(auth.strategy))
		) {
			return {
				status: 401,
				code: "E_RELAY_UNAUTHORIZED",
				message: "Not authenticated through a guard this hub accepts",
			};
		}

		if (roles.length > 0) {
			// At the top level, or under the user, where an auth layer may keep
			// them — the two places the route gate reads.
			const held = auth.roles ?? stringList(auth.user?.roles);
			if (!roles.some((role) => held.includes(role))) {
				return {
					status: 403,
					code: "E_RELAY_FORBIDDEN",
					message: "Insufficient role",
				};
			}
		}

		if (
			permissions.length > 0 &&
			!(await holdsPermissions(auth, permissions))
		) {
			return {
				status: 403,
				code: "E_RELAY_FORBIDDEN",
				message: "Insufficient permissions",
			};
		}

		return null;
	}

	/**
	 * @internal Hand every group send and broadcast to `publish` as well, so
	 * the other instances deliver it to their clients. The relay a hub is
	 * mounted on calls this; one hub carries one publisher.
	 */
	attachFanOut(publish: (message: HubFanOut) => void): void {
		if (this.#fanOut !== undefined && this.#fanOut !== publish) {
			throw new E_RELAY_HUB_ALREADY_MOUNTED();
		}
		this.#fanOut = publish;
	}

	/**
	 * @internal A send another instance made, delivered to this instance's
	 * clients only: it is never handed back to the bus.
	 */
	receiveFanOut(message: HubFanOut): void {
		if (message.target.kind === "group") {
			this.#deliverToGroup(message.target.group, message.event, message.data);
		} else {
			this.#deliverToAll(message.event, message.data);
		}
	}

	/** Send to all clients in a group, on this instance and the others. */
	#sendToGroup(group: string, event: string, data: unknown): void {
		this.#deliverToGroup(group, event, data);
		this.#fanOut?.({ target: { kind: "group", group }, event, data });
	}

	/** Broadcast to all connected clients, on this instance and the others. */
	#broadcastAll(event: string, data: unknown): void {
		this.#deliverToAll(event, data);
		this.#fanOut?.({ target: { kind: "all" }, event, data });
	}

	/** The members of `group` connected to this instance. */
	#deliverToGroup(group: string, event: string, data: unknown): void {
		const members = this.#groupIndex.get(group);
		if (!members) return;
		const dead: string[] = [];
		for (const id of members) {
			const client = this.#clients.get(id);
			if (client) {
				deliver(client, event, data);
			} else {
				dead.push(id);
			}
		}
		for (const id of dead) members.delete(id);
	}

	/** Every client connected to this instance. */
	#deliverToAll(event: string, data: unknown): void {
		for (const client of this.#clients.values()) {
			deliver(client, event, data);
		}
	}

	/**
	 * The clients and groups held on THIS instance. Sends cross instances
	 * through the relay's bus; memberships do not, so the others are not
	 * counted.
	 */
	stats(): { clients: number; groups: number } {
		return { clients: this.#clients.size, groups: this.#groupIndex.size };
	}
}

/**
 * Send to one client without letting it end the fan-out.
 *
 * `send` is supplied by whatever transport registered the client, so a throw
 * from one socket used to abort the loop: every member of the group listed
 * after it silently received nothing, and which ones depended on Set order.
 */
function deliver(
	client: ConnectedHubClient,
	event: string,
	data: unknown,
): void {
	try {
		client.send(event, data);
	} catch {
		// One client's transport is not the group's problem.
	}
}

/**
 * Whether the identity behind `auth` holds every permission in `required` —
 * the rule the route, RPC and GraphQL `.permission()` gates apply, read off
 * what the request's auth exposes.
 *
 * When the auth state can answer itself (`hasAllPermissions`: the auth layer
 * resolves permissions, as a rights store does), its answer decides. Anything
 * but `true` denies. The payload's list is only the fallback of an auth state
 * that cannot: a permission left in a token's claims after it was revoked
 * must not pass where the resolver says no.
 *
 * When the request was authenticated with an access token
 * (`user.currentAccessToken`), each permission must ALSO be allowed by it: an
 * intersection, never a union. A token limited to `profile` does not carry
 * every permission of its user, and one whose `allows` cannot be called
 * denies.
 */
async function holdsPermissions(
	auth: HubAuth,
	required: readonly string[],
): Promise<boolean> {
	if (typeof auth.hasAllPermissions === "function") {
		if ((await auth.hasAllPermissions(required)) !== true) return false;
	} else {
		const granted = auth.permissions ?? stringList(auth.user?.permissions);
		if (!required.every((permission) => granted.includes(permission))) {
			return false;
		}
	}
	const token: unknown = auth.user?.currentAccessToken;
	if (token === undefined || token === null) return true;
	if (typeof token !== "object") return false;
	const allows: unknown = Reflect.get(token, "allows");
	return required.every(
		(permission) =>
			typeof allows === "function" &&
			Reflect.apply(allows, token, [permission]) === true,
	);
}

/** A list of names as an auth payload carries it; anything else is none. */
function stringList(value: unknown): string[] {
	if (!Array.isArray(value)) return [];
	return value.filter((item): item is string => typeof item === "string");
}

/**
 * A user id as one comparable string. A string is taken as given; a number or
 * a bigint — an auto-increment primary key — as its decimal text, so `1` and
 * the same user on the next request compare equal. Anything else cannot be
 * compared, and matches nothing.
 */
export function userIdKey(id: unknown): string | undefined {
	if (typeof id === "string" && id !== "") return id;
	if (typeof id === "number" && Number.isSafeInteger(id)) return String(id);
	if (typeof id === "bigint") return String(id);
	return undefined;
}

/** What the allowlist accepts: a method taking a context and whatever follows. */
function isHandler(
	value: unknown,
): value is (ctx: HubContext, ...args: unknown[]) => Promise<void> | void {
	return typeof value === "function";
}

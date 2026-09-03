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

export interface HubContext {
	/** Client ID. */
	readonly clientId: string;
	/** Auth state from the upgrade request. */
	readonly auth: {
		isAuthenticated: boolean;
		strategy?: string;
		user?: Record<string, unknown>;
		roles?: string[];
		permissions?: string[];
	};
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
}

export interface HubGuardOptions {
	guard?: string;
	guards?: string[];
	roles?: string[];
	permissions?: string[];
}

interface ConnectedHubClient {
	id: string;
	groups: Set<string>;
	auth: HubContext["auth"];
	send: (event: string, data: unknown) => void;
}

/**
 * Base Hub class — extend to create typed hubs.
 *
 * Methods starting with "on" are auto-registered as message handlers. The
 * event name is the method name after "on", with its first letter lowercased:
 * `onTaskUpdate` handles `taskUpdate`. (`onConnect` / `onDisconnect` are
 * lifecycle hooks and are not dispatchable.)
 */
export abstract class Hub {
	#clients: Map<string, ConnectedHubClient> = new Map();
	#groupIndex: Map<string, Set<string>> = new Map();
	#guards: HubGuardOptions = {};
	/** Allowlist of dispatchable handler methods (populated at construction). */
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
		const reserved = new Set(["onConnect", "onDisconnect"]);
		let proto: object | null = Object.getPrototypeOf(this);
		while (proto !== null && proto !== Hub.prototype) {
			for (const name of Object.getOwnPropertyNames(proto)) {
				if (!name.startsWith("on") || name.length <= 2 || reserved.has(name)) {
					continue;
				}
				const eventName = name.charAt(2).toLowerCase() + name.slice(3);
				// A base class does not override the subclass that extends it.
				if (this.#handlerMethods.has(eventName)) continue;
				// Read the descriptor rather than the property: a getter would
				// otherwise be invoked here, during construction, just to find out
				// whether it answers a function.
				const handler = Object.getOwnPropertyDescriptor(proto, name)?.value;
				if (isHandler(handler)) this.#handlerMethods.set(eventName, handler);
			}
			proto = Object.getPrototypeOf(proto);
		}
	}

	/** Set guard options for the entire hub. Normalizes singular guard/guards. */
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

	/** Build a HubContext for a client. */
	#buildContext(client: ConnectedHubClient): HubContext {
		const ctx: HubContext = {
			clientId: client.id,
			auth: client.auth,
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
	 */
	registerClient(client: ConnectedHubClient): HubContext {
		this.#forgetGroups(this.#clients.get(client.id));
		this.#clients.set(client.id, client);
		return this.#buildContext(client);
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
	 * Returns `true` when the handler ran to completion, `false` on any failure
	 * (unknown client/event, auth rejection, or a throwing handler). A bare
	 * `error` event is still emitted to the client for each failure (transport
	 * agnostic), and the boolean lets a correlated transport — SignalR, whose
	 * invocations carry an `invocationId` — answer with an error Completion
	 * instead of a misleading success one.
	 */
	async dispatch(
		clientId: string,
		event: string,
		...args: unknown[]
	): Promise<boolean> {
		const client = this.#clients.get(clientId);
		if (!client) return false;

		const authError = this.#checkDispatchAuth(client);
		if (authError) {
			client.send("error", authError);
			return false;
		}

		// Find handler from allowlist (never dispatches to lifecycle hooks or inherited methods)
		const handler = this.#handlerMethods.get(event);
		if (!handler) {
			client.send("error", {
				code: "E_RELAY_UNKNOWN_EVENT",
				message: `No handler for: ${event}`,
			});
			return false;
		}

		const ctx = this.#buildContext(client);
		try {
			// EVERY argument, not just the first. A SignalR client calling
			// `connection.invoke('Method', a, b, c)` sends three; passing only
			// `a` dropped the rest without a word. A handler declaring one
			// parameter is unaffected — JavaScript ignores the extras.
			await handler.call(this, ctx, ...args);
			return true;
		} catch {
			client.send("error", {
				code: "E_RELAY_HANDLER_ERROR",
				message: "Handler error",
			});
			return false;
		}
	}

	/**
	 * Enforce the hub's guard / strategy / role / permission requirements against
	 * the client's auth state. Returns the error envelope to send, or null when
	 * the client is authorised.
	 */
	#checkDispatchAuth(
		client: ConnectedHubClient,
	): { code: string; message: string } | null {
		const requiredStrategies: string[] = [
			...(this.#guards.guard ? [this.#guards.guard] : []),
			...(this.#guards.guards ?? []),
		];
		const needsAuth =
			requiredStrategies.length > 0 ||
			(this.#guards.roles?.length ?? 0) > 0 ||
			(this.#guards.permissions?.length ?? 0) > 0;

		if (needsAuth && !client.auth.isAuthenticated) {
			return { code: "E_RELAY_UNAUTHORIZED", message: "Not authenticated" };
		}

		if (requiredStrategies.length > 0) {
			const actualStrategy = client.auth.strategy;
			if (!actualStrategy || !requiredStrategies.includes(actualStrategy)) {
				return {
					code: "E_RELAY_UNAUTHORIZED",
					message: `Authentication strategy mismatch (expected: ${requiredStrategies.join(", ")})`,
				};
			}
		}

		if (this.#guards.roles && this.#guards.roles.length > 0) {
			const userRoles = client.auth.roles ?? [];
			if (!this.#guards.roles.some((r) => userRoles.includes(r))) {
				return { code: "E_RELAY_FORBIDDEN", message: "Insufficient role" };
			}
		}

		if (this.#guards.permissions && this.#guards.permissions.length > 0) {
			const userPerms = client.auth.permissions ?? [];
			if (!this.#guards.permissions.every((p) => userPerms.includes(p))) {
				return {
					code: "E_RELAY_FORBIDDEN",
					message: "Insufficient permissions",
				};
			}
		}

		return null;
	}

	/** Send to all clients in a group. */
	#sendToGroup(group: string, event: string, data: unknown): void {
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

	/** Broadcast to all connected clients. */
	#broadcastAll(event: string, data: unknown): void {
		for (const client of this.#clients.values()) {
			deliver(client, event, data);
		}
	}

	/** Get stats. */
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

/** What the allowlist accepts: a method taking a context and whatever follows. */
function isHandler(
	value: unknown,
): value is (ctx: HubContext, ...args: unknown[]) => Promise<void> | void {
	return typeof value === "function";
}

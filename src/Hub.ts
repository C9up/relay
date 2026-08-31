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

	constructor() {
		// Build allowlist from own prototype methods starting with "on" (excluding lifecycle hooks)
		const reserved = new Set(["onConnect", "onDisconnect"]);
		const proto = Object.getPrototypeOf(this);
		for (const name of Object.getOwnPropertyNames(proto)) {
			if (
				name.startsWith("on") &&
				name.length > 2 &&
				!reserved.has(name) &&
				typeof proto[name] === "function"
			) {
				// Convert onTaskUpdate → taskUpdate (event name)
				const eventName = name.charAt(2).toLowerCase() + name.slice(3);
				this.#handlerMethods.set(eventName, proto[name]);
			}
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

	/** Register a client connection. */
	registerClient(client: ConnectedHubClient): HubContext {
		this.#clients.set(client.id, client);
		return this.#buildContext(client);
	}

	/** Remove a client. */
	removeClient(clientId: string): void {
		const client = this.#clients.get(clientId);
		if (client) {
			for (const group of client.groups) {
				const members = this.#groupIndex.get(group);
				if (members) {
					members.delete(clientId);
					if (members.size === 0) this.#groupIndex.delete(group);
				}
			}
			this.#clients.delete(clientId);
			this.onDisconnect(clientId).catch(() => {});
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
				code: "E_UNKNOWN_EVENT",
				message: `No handler for: ${event}`,
			});
			return false;
		}

		// Reuse existing ctx if available, otherwise create
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
				code: "E_HANDLER_ERROR",
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
			return { code: "UNAUTHORIZED", message: "Not authenticated" };
		}

		if (requiredStrategies.length > 0) {
			const actualStrategy = client.auth.strategy;
			if (!actualStrategy || !requiredStrategies.includes(actualStrategy)) {
				return {
					code: "UNAUTHORIZED",
					message: `Authentication strategy mismatch (expected: ${requiredStrategies.join(", ")})`,
				};
			}
		}

		if (this.#guards.roles && this.#guards.roles.length > 0) {
			const userRoles = client.auth.roles ?? [];
			if (!this.#guards.roles.some((r) => userRoles.includes(r))) {
				return { code: "FORBIDDEN", message: "Insufficient role" };
			}
		}

		if (this.#guards.permissions && this.#guards.permissions.length > 0) {
			const userPerms = client.auth.permissions ?? [];
			if (!this.#guards.permissions.every((p) => userPerms.includes(p))) {
				return { code: "FORBIDDEN", message: "Insufficient permissions" };
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
				client.send(event, data);
			} else {
				dead.push(id);
			}
		}
		for (const id of dead) members.delete(id);
	}

	/** Broadcast to all connected clients. */
	#broadcastAll(event: string, data: unknown): void {
		for (const client of this.#clients.values()) {
			client.send(event, data);
		}
	}

	/** Get stats. */
	stats(): { clients: number; groups: number } {
		return { clients: this.#clients.size, groups: this.#groupIndex.size };
	}
}

/**
 * A guarded hub on a booted ream application, on the native server, mounted
 * the documented way — the provider, `relay.hub()`, no route customizer —
 * and reached by a stock `@microsoft/signalr` client over both transports.
 *
 * `useGuards()` is what it takes to connect: a client the guards refuse gets
 * plain HTTP 401 or 403 at negotiate, at the stream and at the socket's
 * handshake, is never registered, never reaches `onConnect` and hears nothing
 * of what the hub pushes. A connection that stops meeting them is ended at
 * its next invocation, and the application can end one itself.
 */

import { request } from "node:http";
import { Ignitor, type MiddlewareFunction } from "@c9up/ream";
import { createHyperServerFactory } from "@c9up/ream/bootstrap";
import {
	HttpTransportType,
	type HubConnection,
	HubConnectionBuilder,
	LogLevel,
} from "@microsoft/signalr";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { Hub, type HubContext } from "../../src/Hub.js";
import RelayProvider from "../../src/RelayProvider.js";
import relay from "../../src/services/main.js";

const RS = "\x1e";
const HANDSHAKE = `${JSON.stringify({ protocol: "json", version: 1 })}${RS}`;

/** Who ran `onConnect`, per hub path. */
const connected = new Map<string, string[]>();
function noteConnect(path: string, ctx: HubContext): void {
	const who = ctx.auth.isAuthenticated
		? String(ctx.auth.user?.id)
		: "anonymous";
	connected.set(path, [...(connected.get(path) ?? []), who]);
}

/** The guide's guarded hub. */
class AdminHub extends Hub {
	constructor() {
		super();
		this.useGuards({ roles: ["admin", "owner"] });
	}
	override async onConnect(ctx: HubContext): Promise<void> {
		noteConnect("/hubs/admin", ctx);
		ctx.joinGroup("admins");
	}
	async onAnnounce(ctx: HubContext, text: string): Promise<string> {
		ctx.broadcast("announcement", text);
		ctx.group("admins").send("admins-only", text);
		return "announced";
	}
}

class ModerationHub extends Hub {
	readonly purged: string[] = [];
	constructor() {
		super();
		this.useGuards({ permissions: ["chat.moderate"] });
	}
	async onPurge(ctx: HubContext, room: string): Promise<string> {
		this.purged.push(`${String(ctx.auth.user?.id)} purged ${room}`);
		return "purged";
	}
}

/** No guards: anyone connects. */
class LobbyHub extends Hub {
	override async onConnect(ctx: HubContext): Promise<void> {
		noteConnect("/hubs/lobby", ctx);
	}
	async onShout(ctx: HubContext, text: string): Promise<void> {
		ctx.broadcast("shout", text);
	}
}

/** Refuses a connection the only way a hub without guards can: by throwing. */
class MembersOnlyHub extends Hub {
	override async onConnect(ctx: HubContext): Promise<void> {
		if (!ctx.auth.isAuthenticated) throw new Error("members only");
	}
	async onShout(ctx: HubContext, text: string): Promise<void> {
		ctx.broadcast("shout", text);
	}
}

interface Account {
	id: string;
	roles: string[];
	/** What the user's payload says — a token's claims, a user row. */
	payloadPermissions: string[];
	/** The abilities of the access token the bearer is, when it is one. */
	tokenAbilities?: string[];
}

/** Bearer value → account. Mutable: a test revokes a role by editing it. */
const accounts = new Map<string, Account>();
/** What the rights store grants, by user id. The resolver reads it live. */
const grants = new Map<string, string[]>();

function seed(): void {
	accounts.clear();
	grants.clear();
	const add = (bearer: string, account: Partial<Account>): void => {
		accounts.set(bearer, {
			id: bearer,
			roles: [],
			payloadPermissions: [],
			...account,
		});
	};
	add("alice", { roles: ["admin"] });
	add("bob", { roles: ["owner"] });
	add("frank", {});
	// A permission the token's claims still carry and the store never granted.
	add("stale-claims", { id: "u1", payloadPermissions: ["chat.moderate"] });
	// A moderator, and the narrow token handed to an integration for them.
	add("moderator", { id: "u2" });
	add("moderator-profile-token", { id: "u2", tokenAbilities: ["profile"] });
	add("carol", { id: "u3" });
	grants.set("u2", ["chat.moderate"]);
	grants.set("u3", ["chat.moderate"]);
}

/**
 * Stands in for an auth layer's global middleware: it fills `ctx.auth` and
 * refuses nothing, as a non-enforcing one does. The state has the shape a
 * per-request authenticator exposes — roles as the request found them,
 * `hasAllPermissions` asking the rights store each time it is called.
 */
const signIn: MiddlewareFunction = async (ctx, next) => {
	const bearer = (ctx.request.header("authorization") ?? "").replace(
		/^Bearer /,
		"",
	);
	const account = accounts.get(bearer);
	if (account !== undefined) {
		const abilities = account.tokenAbilities;
		Reflect.set(ctx, "auth", {
			isAuthenticated: true,
			authenticatedViaGuard: "web",
			user: {
				id: account.id,
				permissions: [...account.payloadPermissions],
				...(abilities === undefined
					? {}
					: {
							currentAccessToken: {
								expiresAt: null,
								allows: (ability: string) => abilities.includes(ability),
							},
						}),
			},
			roles: [...account.roles],
			permissions: [...account.payloadPermissions],
			hasAllPermissions: async (required: readonly string[]) =>
				required.every((permission) =>
					(grants.get(account.id) ?? []).includes(permission),
				),
		});
	}
	await next();
};

const TRANSPORTS = [
	["WebSockets", HttpTransportType.WebSockets],
	["ServerSentEvents", HttpTransportType.ServerSentEvents],
] as const;

const pause = (ms = 200): Promise<void> =>
	new Promise((resolve) => setTimeout(resolve, ms));

describe("relay > a guarded hub refuses the connection itself", () => {
	let ignitor: Ignitor;
	let base = "";
	const adminHub = new AdminHub();
	const moderationHub = new ModerationHub();
	const lobbyHub = new LobbyHub();
	const open: HubConnection[] = [];

	beforeAll(async () => {
		seed();
		ignitor = await new Ignitor({
			port: 0,
			gracefulShutdown: false,
			serverFactory: createHyperServerFactory(),
		})
			.provider((app) => new RelayProvider(app))
			.use(signIn)
			.routes(() => {
				// No `registerRoutes()` customizer anywhere: what guards a hub is
				// the hub, whatever else is mounted and in whichever order.
				relay.hub("/hubs/admin", adminHub);
				relay.hub("/hubs/moderation", moderationHub);
				relay.hub("/hubs/lobby", lobbyHub);
				relay.hub("/hubs/members", new MembersOnlyHub());
			})
			.httpServer()
			.start();
		base = `http://127.0.0.1:${await ignitor.port()}`;
	});

	afterAll(async () => {
		await Promise.all(open.map((connection) => connection.stop()));
		await ignitor.stop();
	});

	function build(
		path: string,
		transport: HttpTransportType,
		bearer?: string,
	): HubConnection {
		const connection = new HubConnectionBuilder()
			.withUrl(`${base}${path}`, {
				transport,
				headers:
					bearer === undefined ? {} : { authorization: `Bearer ${bearer}` },
			})
			.configureLogging(LogLevel.None)
			.build();
		open.push(connection);
		return connection;
	}

	async function connect(
		path: string,
		transport: HttpTransportType,
		bearer?: string,
	): Promise<HubConnection> {
		const connection = build(path, transport, bearer);
		await connection.start();
		return connection;
	}

	/** Everything a connection is pushed on the events a hub here sends. */
	function listen(connection: HubConnection): Array<[string, unknown]> {
		const heard: Array<[string, unknown]> = [];
		for (const event of ["announcement", "admins-only", "shout"]) {
			connection.on(event, (data: unknown) => heard.push([event, data]));
		}
		return heard;
	}

	async function negotiate(
		path: string,
		bearer?: string,
	): Promise<{ status: number; body: unknown }> {
		const res = await fetch(`${base}${path}/negotiate?negotiateVersion=1`, {
			method: "POST",
			headers:
				bearer === undefined ? {} : { authorization: `Bearer ${bearer}` },
		});
		return { status: res.status, body: await res.json() };
	}

	async function tokenFor(path: string, bearer?: string): Promise<string> {
		const { body } = await negotiate(path, bearer);
		const token: unknown = Reflect.get(Object(body), "connectionToken");
		if (typeof token !== "string") throw new Error("no connection token");
		return token;
	}

	/** The status a WebSocket handshake is answered with, by hand. */
	function handshakeStatus(
		path: string,
		token: string,
		bearer?: string,
	): Promise<number> {
		return new Promise((resolve, reject) => {
			const req = request(`${base}${path}?id=${token}`, {
				headers: {
					connection: "Upgrade",
					upgrade: "websocket",
					"sec-websocket-version": "13",
					"sec-websocket-key": "dGhlIHNhbXBsZSBub25jZQ==",
					...(bearer === undefined
						? {}
						: { authorization: `Bearer ${bearer}` }),
				},
			});
			req.on("response", (res) => {
				res.resume();
				resolve(res.statusCode ?? 0);
			});
			req.on("upgrade", (_res, socket) => {
				socket.destroy();
				resolve(101);
			});
			req.on("error", reject);
			req.end();
		});
	}

	it("answers an anonymous negotiate with 401, and a missing role with 403", async () => {
		expect(await negotiate("/hubs/admin")).toEqual({
			status: 401,
			body: {
				error: { code: "E_RELAY_UNAUTHORIZED", message: "Not authenticated" },
			},
		});
		expect(await negotiate("/hubs/admin", "frank")).toEqual({
			status: 403,
			body: {
				error: { code: "E_RELAY_FORBIDDEN", message: "Insufficient role" },
			},
		});
		expect((await negotiate("/hubs/admin", "alice")).status).toBe(200);
	});

	it("refuses the stream and the socket too, to whoever holds a token", async () => {
		// A token negotiated by an admin, then presented without her
		// credentials: the guards answer before the token is even looked at.
		const token = await tokenFor("/hubs/admin", "alice");

		const stream = await fetch(`${base}/hubs/admin?id=${token}`, {
			headers: { accept: "text/event-stream" },
		});
		expect(stream.status).toBe(401);
		expect(stream.headers.get("content-type")).not.toContain("event-stream");
		expect(await stream.json()).toMatchObject({
			error: { code: "E_RELAY_UNAUTHORIZED" },
		});
		const asFrank = await fetch(`${base}/hubs/admin?id=${token}`, {
			headers: { accept: "text/event-stream", authorization: "Bearer frank" },
		});
		expect(asFrank.status).toBe(403);

		expect(await handshakeStatus("/hubs/admin", token)).toBe(401);
		expect(await handshakeStatus("/hubs/admin", token, "frank")).toBe(403);
		expect(await handshakeStatus("/hubs/admin", token, "alice")).toBe(101);
	});

	it("refuses a socket once the role is gone between negotiate and the handshake", async () => {
		accounts.set("temp", {
			id: "temp",
			roles: ["admin"],
			payloadPermissions: [],
		});
		const token = await tokenFor("/hubs/admin", "temp");
		accounts.set("temp", { id: "temp", roles: [], payloadPermissions: [] });

		expect(await handshakeStatus("/hubs/admin", token, "temp")).toBe(403);
	});

	for (const [name, transport] of TRANSPORTS) {
		it(`serves the admin and nobody the guards refuse (${name})`, async () => {
			connected.set("/hubs/admin", []);
			const before = adminHub.stats().clients;

			await expect(build("/hubs/admin", transport).start()).rejects.toThrow(
				/Status code '401'/,
			);
			await expect(
				build("/hubs/admin", transport, "frank").start(),
			).rejects.toThrow(/Status code '403'/);

			const alice = await connect("/hubs/admin", transport, "alice");
			const bob = await connect("/hubs/admin", transport, "bob");
			const heard = listen(bob);
			expect(await alice.invoke("announce", "payroll 2026-11")).toBe(
				"announced",
			);
			await pause();

			// Only the two the guards accept were ever registered, ran
			// `onConnect` or joined the group the hub puts its members in.
			expect(connected.get("/hubs/admin")).toEqual(["alice", "bob"]);
			expect(adminHub.stats().clients).toBe(before + 2);
			expect(heard).toEqual(
				expect.arrayContaining([
					["announcement", "payroll 2026-11"],
					["admins-only", "payroll 2026-11"],
				]),
			);
			await alice.stop();
			await bob.stop();
		});

		it(`leaves a hub with no guards open to an anonymous client (${name})`, async () => {
			connected.set("/hubs/lobby", []);
			const guest = await connect("/hubs/lobby", transport);
			const other = await connect("/hubs/lobby", transport, "frank");
			const heard = listen(guest);

			await other.invoke("shout", "hello");
			await pause();

			expect(connected.get("/hubs/lobby")).toEqual(["anonymous", "frank"]);
			expect(heard).toEqual([["shout", "hello"]]);
			await guest.stop();
			await other.stop();
		});

		it(`ends a connection the application disconnects (${name})`, async () => {
			const alice = await connect("/hubs/admin", transport, "alice");
			const bob = await connect("/hubs/admin", transport, "bob");
			const heard = listen(alice);
			const closed = new Promise<void>((resolve) => {
				alice.onclose(() => resolve());
			});

			// What a logout handler calls.
			expect(adminHub.disconnectUser("alice")).toBe(1);
			await bob.invoke("announce", "after the logout");
			await closed;
			await pause();

			expect(heard).toEqual([]);
			await bob.stop();
		});

		it(`asks the auth layer for a permission, not the payload (${name})`, async () => {
			// The claims say `chat.moderate`; the rights store never granted it.
			await expect(
				build("/hubs/moderation", transport, "stale-claims").start(),
			).rejects.toThrow(/Status code '403'/);
			// A token limited to `profile`, for a user who does hold it.
			await expect(
				build("/hubs/moderation", transport, "moderator-profile-token").start(),
			).rejects.toThrow(/Status code '403'/);

			// Granted by the store, absent from the payload.
			const moderator = await connect(
				"/hubs/moderation",
				transport,
				"moderator",
			);
			moderationHub.purged.length = 0;
			expect(await moderator.invoke("purge", "lobby")).toBe("purged");
			expect(moderationHub.purged).toEqual(["u2 purged lobby"]);
			await moderator.stop();
		});

		it(`ends a connection whose permission is revoked, at its next call (${name})`, async () => {
			grants.set("u3", ["chat.moderate"]);
			const carol = await connect("/hubs/moderation", transport, "carol");
			expect(await carol.invoke("purge", "lobby")).toBe("purged");
			const closed = new Promise<void>((resolve) => {
				carol.onclose(() => resolve());
			});
			moderationHub.purged.length = 0;

			grants.set("u3", []);

			// Over a socket the handshake's auth answers for every call, and it
			// asks the store again each time.
			await expect(carol.invoke("purge", "lobby")).rejects.toThrow();
			await closed;
			expect(moderationHub.purged).toEqual([]);
			grants.set("u3", ["chat.moderate"]);
		});
	}

	it("ends a stream whose role is revoked, at its next call", async () => {
		accounts.set("dave", {
			id: "dave",
			roles: ["admin"],
			payloadPermissions: [],
		});
		const dave = await connect(
			"/hubs/admin",
			HttpTransportType.ServerSentEvents,
			"dave",
		);
		const alice = await connect(
			"/hubs/admin",
			HttpTransportType.WebSockets,
			"alice",
		);
		const heard = listen(dave);
		const closed = new Promise<void>((resolve) => {
			dave.onclose(() => resolve());
		});

		accounts.set("dave", { id: "dave", roles: [], payloadPermissions: [] });

		// Each call arrives on its own request, authenticated afresh.
		await expect(dave.invoke("announce", "still here?")).rejects.toThrow();
		await closed;
		await alice.invoke("announce", "admins only now");
		await pause();
		expect(heard).toEqual([]);
		await alice.stop();
	});

	it("ends a stream whose hub threw from onConnect", async () => {
		// By hand: the stock client stops by itself when its handshake POST
		// fails, which would hide a stream the server left registered.
		const token = await tokenFor("/hubs/members");
		const stream = await fetch(`${base}/hubs/members?id=${token}`, {
			headers: { accept: "text/event-stream" },
		});
		expect(stream.status).toBe(200);
		const reader = stream.body?.getReader();
		if (reader === undefined) throw new Error("no stream");
		let received = "";
		const pump = (async () => {
			const decoder = new TextDecoder();
			for (;;) {
				const { done, value } = await reader.read();
				if (done) return;
				received += decoder.decode(value, { stream: true });
			}
		})();

		const handshake = await fetch(`${base}/hubs/members?id=${token}`, {
			method: "POST",
			body: HANDSHAKE,
		});
		expect(handshake.status).toBe(500);

		const member = await connect(
			"/hubs/members",
			HttpTransportType.WebSockets,
			"frank",
		);
		await member.invoke("shout", "after the throw");
		await pause();
		expect(received).not.toContain("after the throw");
		await reader.cancel();
		await pump.catch(() => {});
		await member.stop();
	});
});

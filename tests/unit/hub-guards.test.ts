/**
 * A hub's guards are what it takes to be connected to it: `authorize()` is
 * the one question, asked where a connection is admitted and again on every
 * invocation, and `disconnect()` is how an application ends a connection in
 * between.
 */

import { describe, expect, it } from "vitest";
import {
	type ConnectedHubClient,
	Hub,
	type HubAuth,
	type HubContext,
} from "../../src/Hub.js";

class ModerationHub extends Hub {
	readonly ran: string[] = [];
	readonly left: string[] = [];

	override async onDisconnect(clientId: string): Promise<void> {
		this.left.push(clientId);
	}

	async onPurge(ctx: HubContext, room: string): Promise<string> {
		this.ran.push(`${String(ctx.auth.user?.id)} purged ${room}`);
		return "purged";
	}

	async onLeave(ctx: HubContext): Promise<void> {
		ctx.disconnect();
	}
}

function client(
	id: string,
	auth: HubAuth,
): ConnectedHubClient & {
	sent: Array<{ event: string; data: unknown }>;
	closed: number;
} {
	const sent: Array<{ event: string; data: unknown }> = [];
	const self = {
		id,
		groups: new Set<string>(),
		auth,
		sent,
		closed: 0,
		send: (event: string, data: unknown) => {
			sent.push({ event, data });
		},
		close: () => {
			self.closed++;
		},
	};
	return self;
}

/** Narrows an object built loosely, as a JavaScript caller would build it. */
function isHubAuth(value: object): value is HubAuth {
	return "isAuthenticated" in value;
}

const FORBIDDEN = {
	status: 403,
	code: "E_RELAY_FORBIDDEN",
	message: "Insufficient permissions",
};

describe("Hub.authorize > who may connect", () => {
	it("lets anyone into a hub with no guards", async () => {
		const hub = new ModerationHub();

		expect(await hub.authorize({ isAuthenticated: false })).toBeNull();
	});

	it("answers 401 to a request that is not authenticated", async () => {
		const hub = new ModerationHub();
		hub.useGuards({ roles: ["admin"] });

		expect(await hub.authorize({ isAuthenticated: false })).toEqual({
			status: 401,
			code: "E_RELAY_UNAUTHORIZED",
			message: "Not authenticated",
		});
	});

	it("answers 401 to a guard the hub does not accept, without naming the accepted ones", async () => {
		const hub = new ModerationHub();
		hub.useGuards({ guards: ["session"] });

		const denial = await hub.authorize({
			isAuthenticated: true,
			strategy: "api",
		});

		expect(denial).toEqual({
			status: 401,
			code: "E_RELAY_UNAUTHORIZED",
			message: "Not authenticated through a guard this hub accepts",
		});
		expect(denial?.message).not.toContain("session");
	});

	it("answers 403 to an authenticated user without the role", async () => {
		const hub = new ModerationHub();
		hub.useGuards({ roles: ["admin", "owner"] });

		expect(
			await hub.authorize({ isAuthenticated: true, roles: ["member"] }),
		).toEqual({
			status: 403,
			code: "E_RELAY_FORBIDDEN",
			message: "Insufficient role",
		});
		expect(
			await hub.authorize({ isAuthenticated: true, roles: ["owner"] }),
		).toBeNull();
	});

	it("reads the roles under the user when the auth state keeps them there", async () => {
		const hub = new ModerationHub();
		hub.useGuards({ roles: ["admin"] });

		expect(
			await hub.authorize({
				isAuthenticated: true,
				user: { id: "u1", roles: ["admin"] },
			}),
		).toBeNull();
		// A value that is not a list of names is no role at all.
		expect(
			await hub.authorize({
				isAuthenticated: true,
				user: { id: "u1", roles: "admin" },
			}),
		).toMatchObject({ status: 403 });
	});
});

describe("Hub.authorize > the permissions guard asks the auth layer", () => {
	function guarded(): ModerationHub {
		const hub = new ModerationHub();
		hub.useGuards({ permissions: ["chat.moderate", "chat.read"] });
		return hub;
	}

	it("refuses a permission only the payload carries", async () => {
		const asked: string[][] = [];
		const denial = await guarded().authorize({
			isAuthenticated: true,
			user: { id: "u1" },
			// What a token signed before the permission was revoked still says.
			permissions: ["chat.moderate", "chat.read"],
			hasAllPermissions: async (required) => {
				asked.push([...required]);
				return false;
			},
		});

		expect(denial).toEqual(FORBIDDEN);
		expect(asked).toEqual([["chat.moderate", "chat.read"]]);
	});

	it("accepts a permission only the resolver grants", async () => {
		expect(
			await guarded().authorize({
				isAuthenticated: true,
				user: { id: "u2" },
				permissions: [],
				hasAllPermissions: async () => true,
			}),
		).toBeNull();
	});

	it("denies on any answer but true", async () => {
		const answers: unknown[] = [undefined, 1, "true", null];
		for (const answer of answers) {
			// An auth layer written in JavaScript is not held to the type.
			const auth = {
				isAuthenticated: true,
				user: { id: "u1" },
				permissions: ["chat.moderate", "chat.read"],
				hasAllPermissions: () => Promise.resolve(answer),
			};
			if (!isHubAuth(auth)) throw new Error("not an auth state");
			expect(await guarded().authorize(auth)).toEqual(FORBIDDEN);
		}
	});

	it("lets an error of the resolver through rather than grant", async () => {
		await expect(
			guarded().authorize({
				isAuthenticated: true,
				user: { id: "u1" },
				hasAllPermissions: async () => {
					throw new Error("rights store unreachable");
				},
			}),
		).rejects.toThrow("rights store unreachable");
	});

	it("falls back to the payload only when the auth state cannot answer", async () => {
		const hub = guarded();

		expect(
			await hub.authorize({
				isAuthenticated: true,
				permissions: ["chat.moderate", "chat.read"],
			}),
		).toBeNull();
		expect(
			await hub.authorize({
				isAuthenticated: true,
				user: { id: "u1", permissions: ["chat.moderate", "chat.read"] },
			}),
		).toBeNull();
		// All of them, not one.
		expect(
			await hub.authorize({
				isAuthenticated: true,
				permissions: ["chat.moderate"],
			}),
		).toEqual(FORBIDDEN);
		expect(await hub.authorize({ isAuthenticated: true })).toEqual(FORBIDDEN);
	});

	it("requires the access token to allow each permission as well", async () => {
		const hub = guarded();
		const holder = (allows: unknown): HubAuth => ({
			isAuthenticated: true,
			user: {
				id: "u2",
				currentAccessToken: allows === undefined ? {} : { allows },
			},
			hasAllPermissions: async () => true,
		});

		// A token handed to an integration so that it can NOT moderate.
		expect(
			await hub.authorize(holder((ability: string) => ability === "profile")),
		).toEqual(FORBIDDEN);
		// One of the two is not enough.
		expect(
			await hub.authorize(holder((ability: string) => ability === "chat.read")),
		).toEqual(FORBIDDEN);
		expect(await hub.authorize(holder(() => true))).toBeNull();
		// A truthy answer is not `true`.
		expect(await hub.authorize(holder(() => "yes"))).toEqual(FORBIDDEN);
		// A token that cannot be asked allows nothing.
		expect(await hub.authorize(holder(undefined))).toEqual(FORBIDDEN);
		expect(
			await hub.authorize({
				isAuthenticated: true,
				user: { id: "u2", currentAccessToken: "opaque" },
				hasAllPermissions: async () => true,
			}),
		).toEqual(FORBIDDEN);
	});

	it("does not let a token's abilities add to what the user holds", async () => {
		expect(
			await guarded().authorize({
				isAuthenticated: true,
				user: { id: "u1", currentAccessToken: { allows: () => true } },
				hasAllPermissions: async () => false,
			}),
		).toEqual(FORBIDDEN);
	});

	it("calls `allows` on the token itself", async () => {
		const token = {
			abilities: ["chat.moderate", "chat.read"],
			allows(ability: string): boolean {
				return this.abilities.includes(ability);
			},
		};

		expect(
			await guarded().authorize({
				isAuthenticated: true,
				user: { id: "u2", currentAccessToken: token },
				hasAllPermissions: async () => true,
			}),
		).toBeNull();
	});
});

describe("Hub > a refused invocation ends the connection", () => {
	it("takes the caller off the hub and reports the denial", async () => {
		const hub = new ModerationHub();
		hub.useGuards({ roles: ["admin"] });
		const alice = client("a", { isAuthenticated: true, roles: ["admin"] });
		const bob = client("b", { isAuthenticated: true, roles: ["admin"] });
		hub.registerClient(alice).joinGroup("admins");
		const bobCtx = hub.registerClient(bob);

		// Alice's role is gone by the time she invokes.
		const result = await hub.dispatchAs(
			"a",
			{ isAuthenticated: true, roles: [] },
			"purge",
			["lobby"],
		);

		expect(result).toEqual({
			ok: false,
			denial: {
				status: 403,
				code: "E_RELAY_FORBIDDEN",
				message: "Insufficient role",
			},
		});
		expect(hub.ran).toEqual([]);
		expect(hub.left).toEqual(["a"]);
		expect(hub.stats()).toEqual({ clients: 1, groups: 0 });

		alice.sent.length = 0;
		bobCtx.broadcast("announcement", "payroll");
		bobCtx.group("admins").send("admins-only", "payroll");
		expect(alice.sent).toEqual([]);
		expect(bob.sent).toEqual([{ event: "announcement", data: "payroll" }]);
	});

	it("still reports the denial when the caller's transport cannot take the error", async () => {
		const hub = new ModerationHub();
		hub.useGuards({ roles: ["admin"] });
		hub.registerClient({
			id: "a",
			groups: new Set(),
			auth: { isAuthenticated: false },
			send: () => {
				throw new Error("socket gone");
			},
		});

		expect(await hub.dispatch("a", "purge", "lobby")).toMatchObject({
			ok: false,
			denial: { status: 401 },
		});
		expect(hub.stats().clients).toBe(0);
	});
});

describe("Hub.disconnect > ending a connection from the server", () => {
	it("stops deliveries at once and closes the transport", () => {
		const hub = new ModerationHub();
		const alice = client("a", { isAuthenticated: true, user: { id: "u1" } });
		const bob = client("b", { isAuthenticated: true, user: { id: "u2" } });
		hub.registerClient(alice).joinGroup("room");
		const bobCtx = hub.registerClient(bob);

		expect(hub.disconnect("a")).toBe(true);

		expect(alice.closed).toBe(1);
		expect(hub.left).toEqual(["a"]);
		bobCtx.broadcast("news", 1);
		bobCtx.group("room").send("news", 2);
		expect(alice.sent).toEqual([]);
		expect(hub.stats()).toEqual({ clients: 1, groups: 0 });
		// Nothing left to end.
		expect(hub.disconnect("a")).toBe(false);
		expect(alice.closed).toBe(1);
	});

	it("ends every connection of one user, whatever the type of its id", () => {
		const hub = new ModerationHub();
		const tabs = [
			client("a", { isAuthenticated: true, user: { id: 7 } }),
			client("b", { isAuthenticated: true, user: { id: "7" } }),
			client("c", { isAuthenticated: true, user: { id: 7n } }),
		];
		const other = client("d", { isAuthenticated: true, user: { id: 70 } });
		// An anonymous connection is nobody's, even if a payload names an id.
		const guest = client("e", { isAuthenticated: false, user: { id: 7 } });
		for (const each of [...tabs, other, guest]) hub.registerClient(each);

		expect(hub.disconnectUser(7)).toBe(3);

		expect(tabs.map((tab) => tab.closed)).toEqual([1, 1, 1]);
		expect(other.closed + guest.closed).toBe(0);
		expect(hub.stats().clients).toBe(2);
		expect(hub.disconnectUser("nobody")).toBe(0);
		// An id that cannot be compared matches no one.
		expect(hub.disconnectUser(1.5)).toBe(0);
		expect(hub.disconnectUser("")).toBe(0);
	});

	it("lets a handler end its own connection", async () => {
		const hub = new ModerationHub();
		const alice = client("a", { isAuthenticated: true });
		hub.registerClient(alice);

		expect(await hub.dispatch("a", "leave")).toMatchObject({ ok: true });

		expect(alice.closed).toBe(1);
		expect(hub.stats().clients).toBe(0);
	});

	it("leaves a newer connection under the same id alone", () => {
		const hub = new ModerationHub();
		const old = client("a", { isAuthenticated: true });
		const stale = hub.registerClient(old);
		const fresh = client("a", { isAuthenticated: true });
		hub.registerClient(fresh);

		stale.disconnect();

		expect(old.closed).toBe(1);
		expect(fresh.closed).toBe(0);
		expect(hub.stats().clients).toBe(1);
		expect(hub.left).toEqual([]);
	});

	it("takes the client off the hub even when its transport fails to close", () => {
		const hub = new ModerationHub();
		hub.registerClient({
			id: "a",
			groups: new Set(),
			auth: { isAuthenticated: true },
			send: () => {},
			close: () => {
				throw new Error("already closed");
			},
		});
		// A transport that gave no way to close: the hub's half still happens.
		hub.registerClient({
			id: "b",
			groups: new Set(),
			auth: { isAuthenticated: true },
			send: () => {},
		});

		expect(hub.disconnect("a")).toBe(true);
		expect(hub.disconnect("b")).toBe(true);
		expect(hub.stats().clients).toBe(0);
	});
});

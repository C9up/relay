/**
 * The connection caps are partitioned by owner.
 *
 * `maxClients`, a hub's `maxConnections` and the pending-token table were
 * global and nothing else: one client opening `maxClients` streams — HTTP/2
 * multiplexes them on one socket — locked every other user out, and a flood of
 * `/negotiate` from one address evicted every other client's pending token
 * between its negotiate and its GET. A global cap protects the process; a cap
 * per owner — the user, or the client address of an anonymous request — is what
 * stops one client from taking all of it.
 */

import { describe, expect, it } from "vitest";
import { Hub } from "../../src/Hub.js";
import {
	Relay,
	type RelayContext,
	type RelaySseStream,
} from "../../src/Relay.js";
import { SignalRAdapter } from "../../src/SignalRAdapter.js";
import {
	type HubHttpContext,
	type HubSseStream,
	registerHubRoutes,
} from "../../src/SignalRTransport.js";

function stream(): RelaySseStream & HubSseStream {
	let open = true;
	const closers: Array<() => void> = [];
	return {
		id: `s-${Math.random()}`,
		isOpen: () => open,
		send: async () => open,
		onClose: (cb) => {
			closers.push(cb);
		},
		end: async () => {
			if (!open) return;
			open = false;
			for (const cb of closers) cb();
		},
	};
}

const fromIp = (ip: string): RelayContext => ({ request: { ip: () => ip } });
const asUser = (id: string, ip = "10.0.0.9"): RelayContext => ({
	auth: { isAuthenticated: true, user: { id } },
	request: { ip: () => ip },
});

describe("relay > connections are capped per owner", () => {
	it("stops one address at its own cap while another still connects", () => {
		const relay = new Relay({ maxClientsPerOwner: 3 });
		const outcomes = Array.from(
			{ length: 4 },
			() => relay.connect(stream(), fromIp("203.0.113.7")).outcome,
		);

		expect(outcomes).toEqual(["ok", "ok", "ok", "owner-capped"]);
		expect(relay.connect(stream(), fromIp("198.51.100.1")).outcome).toBe("ok");
		expect(relay.connect(stream(), asUser("u1", "203.0.113.7")).outcome).toBe(
			"ok",
		);
	});

	it("counts an authenticated user's tabs together, whatever address they come from", () => {
		const relay = new Relay({ maxClientsPerOwner: 2 });

		relay.connect(stream(), asUser("u1", "203.0.113.1"));
		relay.connect(stream(), asUser("u1", "203.0.113.2"));

		expect(relay.connect(stream(), asUser("u1", "203.0.113.3")).outcome).toBe(
			"owner-capped",
		);
		expect(relay.connect(stream(), asUser("u2")).outcome).toBe("ok");
	});

	it("gives the slot back when one of the owner's streams closes", async () => {
		const relay = new Relay({ maxClientsPerOwner: 1 });
		const first = stream();
		relay.connect(first, fromIp("203.0.113.7"));
		expect(relay.connect(stream(), fromIp("203.0.113.7")).outcome).toBe(
			"owner-capped",
		);

		await first.end();

		expect(relay.connect(stream(), fromIp("203.0.113.7")).outcome).toBe("ok");
	});

	it("defaults to twenty per owner, under the global cap", () => {
		const relay = new Relay({ maxClients: 25 });
		const mine = Array.from(
			{ length: 21 },
			() => relay.connect(stream(), fromIp("203.0.113.7")).outcome,
		);
		expect(mine.filter((o) => o === "ok")).toHaveLength(20);
		expect(mine[20]).toBe("owner-capped");

		// The global cap still holds across owners.
		const others = Array.from(
			{ length: 6 },
			(_, i) => relay.connect(stream(), fromIp(`198.51.100.${i}`)).outcome,
		);
		expect(others).toEqual(["ok", "ok", "ok", "ok", "ok", "capped"]);
	});

	it("can be turned off", () => {
		const relay = new Relay({ maxClientsPerOwner: false });
		const outcomes = Array.from(
			{ length: 30 },
			() => relay.connect(stream(), fromIp("203.0.113.7")).outcome,
		);
		expect(outcomes.every((o) => o === "ok")).toBe(true);
	});
});

class ChatHub extends Hub {}

type Handler = (ctx: HubHttpContext) => Promise<void> | void;

interface Exchange {
	ctx: HubHttpContext;
	status: () => number;
	body: () => unknown;
}

function exchange(
	url: string,
	ip: string,
	auth?: HubHttpContext["auth"],
): Exchange {
	let status = 200;
	let body: unknown;
	const sse = stream();
	return {
		ctx: {
			request: {
				url: (qs?: boolean) => (qs === true ? url : (url.split("?")[0] ?? url)),
				raw: () => "",
				ip: () => ip,
			},
			response: {
				status(code: number) {
					status = code;
					return {
						json(value: unknown) {
							body = value;
						},
					};
				},
				json(value: unknown) {
					body = value;
				},
				sse: async () => sse,
			},
			auth,
		},
		status: () => status,
		body: () => body,
	};
}

function mountedHub(options: ConstructorParameters<typeof SignalRAdapter>[1]) {
	const routes = new Map<string, Handler>();
	const router = {
		get: (path: string, handler: Handler) => {
			routes.set(`GET ${path}`, handler);
			return {};
		},
		post: (path: string, handler: Handler) => {
			routes.set(`POST ${path}`, handler);
			return {};
		},
	};
	const hub = new ChatHub();
	const adapter = new SignalRAdapter(hub, options);
	registerHubRoutes(router, { path: "/hub", hub, adapter });
	const call = async (key: string, ex: Exchange): Promise<Exchange> => {
		const handler = routes.get(key);
		if (handler === undefined) throw new Error(`no route ${key}`);
		await handler(ex.ctx);
		return ex;
	};
	const negotiate = async (ip: string, auth?: HubHttpContext["auth"]) =>
		call("POST /hub/negotiate", exchange("/hub/negotiate", ip, auth));
	const tokenOf = (ex: Exchange): string =>
		String(Reflect.get(Object(ex.body()), "connectionToken"));
	const open = async (ip: string, auth?: HubHttpContext["auth"]) => {
		const token = tokenOf(await negotiate(ip, auth));
		return call("GET /hub", exchange(`/hub?id=${token}`, ip, auth));
	};
	return { adapter, negotiate, open, tokenOf };
}

describe("hub > streams are capped per owner", () => {
	it("refuses one address past its own cap while another still opens", async () => {
		const { open } = mountedHub({ maxConnectionsPerOwner: 2 });

		const statuses = [
			(await open("203.0.113.7")).status(),
			(await open("203.0.113.7")).status(),
		];
		const third = await open("203.0.113.7");

		expect(statuses).toEqual([200, 200]);
		expect(third.status()).toBe(503);
		expect(third.body()).toEqual({
			error: {
				code: "E_RELAY_MAX_CONNECTIONS_PER_OWNER",
				message: "Too many open connections from this client.",
			},
		});
		expect((await open("198.51.100.1")).status()).toBe(200);
	});

	it("keys an authenticated stream on the user", async () => {
		const { open } = mountedHub({ maxConnectionsPerOwner: 1 });
		const auth = { isAuthenticated: true, user: { id: "u1" } };

		expect((await open("203.0.113.1", auth)).status()).toBe(200);
		expect((await open("203.0.113.2", auth)).status()).toBe(503);
	});
});

describe("hub > a numeric user id is a user, not an anonymous client", () => {
	it("keys the stream cap on the user across addresses", async () => {
		const { open } = mountedHub({ maxConnectionsPerOwner: 1 });
		const auth = { isAuthenticated: true, user: { id: 1 } };

		expect((await open("203.0.113.1", auth)).status()).toBe(200);
		expect((await open("203.0.113.2", auth)).status()).toBe(503);
	});

	it("does not share the cap between two numeric users on one address", async () => {
		const { open } = mountedHub({ maxConnectionsPerOwner: 1 });

		const first = await open("203.0.113.1", {
			isAuthenticated: true,
			user: { id: 1 },
		});
		const second = await open("203.0.113.1", {
			isAuthenticated: true,
			user: { id: 2 },
		});

		expect([first.status(), second.status()]).toEqual([200, 200]);
	});
});

describe("hub > a negotiate flood cannot evict another client's token", () => {
	it("keeps a pending token valid through a flood from one address", async () => {
		const { adapter, negotiate, tokenOf } = mountedHub({});
		const legit = tokenOf(await negotiate("198.51.100.1"));

		const flood: number[] = [];
		for (let i = 0; i < 1_100; i++) {
			flood.push((await negotiate("203.0.113.7")).status());
		}

		expect(adapter.resolveToken(legit)).toBeDefined();
		// The flood itself stops at its own cap.
		expect(flood.filter((s) => s === 200)).toHaveLength(20);
		expect(flood.at(-1)).toBe(503);
	});

	it("refuses a new negotiate when the table is full instead of evicting", () => {
		const adapter = new SignalRAdapter(new ChatHub(), {
			maxPendingTokens: 3,
			maxPendingTokensPerOwner: false,
		});
		const first = adapter.negotiate("c0").connectionToken;
		adapter.negotiate("c1");
		adapter.negotiate("c2");

		expect(adapter.admits("anyone")).toBe(false);
		expect(() => adapter.negotiate("c3")).toThrow(/pending/);
		expect(adapter.resolveToken(first)).toBe("c0");
	});
});

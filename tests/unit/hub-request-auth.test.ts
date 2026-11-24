/**
 * A hub's guards decide who is connected, on the hub's own routes.
 *
 * Negotiate, the stream and every invocation each ask them. They used to be
 * asked on invocations only: a client the guards refused still negotiated,
 * opened its stream, ran `onConnect` and received every broadcast — only its
 * own calls were turned away, which is what made the hub look guarded.
 *
 * Driven here against a fake router and a fake SSE stream; the same contract
 * runs through the real transports in `tests/integration`.
 */

import { afterEach, describe, expect, it, vi } from "vitest";
import { Hub, type HubContext } from "../../src/Hub.js";
import { SignalRAdapter } from "../../src/SignalRAdapter.js";
import {
	type HubHttpContext,
	type HubSseStream,
	registerHubRoutes,
} from "../../src/SignalRTransport.js";

const RS = "\x1e";
const HANDSHAKE = `${JSON.stringify({ protocol: "json", version: 1 })}${RS}`;

class AdminHub extends Hub {
	readonly sawRoles: Array<string[] | undefined> = [];
	readonly connected: string[] = [];
	readonly left: string[] = [];
	refuseOnConnect = false;

	constructor(guarded = true) {
		super();
		if (guarded) this.useGuards({ roles: ["admin"] });
	}

	override async onConnect(ctx: HubContext): Promise<void> {
		if (this.refuseOnConnect) throw new Error("not welcome");
		this.connected.push(ctx.clientId);
		ctx.joinGroup("admins");
	}

	override async onDisconnect(clientId: string): Promise<void> {
		this.left.push(clientId);
	}

	async onPurgeUsers(ctx: HubContext): Promise<string> {
		this.sawRoles.push(ctx.auth.roles);
		return "purged";
	}

	async onAnnounce(ctx: HubContext, text: string): Promise<void> {
		ctx.broadcast("announcement", text);
		ctx.group("admins").send("admins-only", text);
	}
}

type Auth = NonNullable<HubHttpContext["auth"]>;
type Handler = (ctx: HubHttpContext) => Promise<void> | void;

interface Exchange {
	ctx: HubHttpContext;
	status: () => number;
	body: () => unknown;
}

function recordingStream(): HubSseStream & {
	frames: string[];
	opened: number;
} {
	const frames: string[] = [];
	const closers: Array<() => void> = [];
	let open = true;
	return {
		id: "s",
		frames,
		opened: 0,
		isOpen: () => open,
		send: async (_event, data) => {
			if (open) frames.push(String(data));
			return open;
		},
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

function exchange(
	url: string,
	raw: string,
	auth: Auth | undefined,
	sse: ReturnType<typeof recordingStream>,
): Exchange {
	let status = 200;
	let body: unknown;
	const ctx: HubHttpContext = {
		request: {
			url: (qs?: boolean) => (qs === true ? url : (url.split("?")[0] ?? url)),
			raw: () => raw,
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
			sse: async () => {
				sse.opened++;
				return sse;
			},
		},
		auth,
	};
	return { ctx, status: () => status, body: () => body };
}

const admin = (id = "u1"): Auth => ({
	isAuthenticated: true,
	authenticatedViaGuard: "web",
	user: { id },
	roles: ["admin"],
});
const member = (id = "u1"): Auth => ({
	isAuthenticated: true,
	authenticatedViaGuard: "web",
	user: { id },
	roles: [],
});

function invocation(target: string, args: unknown[] = [], id = "1"): string {
	return `${JSON.stringify({ type: 1, invocationId: id, target, arguments: args })}${RS}`;
}

function mount(hub: AdminHub = new AdminHub()) {
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
	const adapter = new SignalRAdapter(hub);
	registerHubRoutes(router, { path: "/hubs/admin", hub, adapter });
	const call = async (key: string, ex: Exchange): Promise<Exchange> => {
		const handler = routes.get(key);
		if (handler === undefined) throw new Error(`no route ${key}`);
		await handler(ex.ctx);
		return ex;
	};
	const negotiate = (auth: Auth | undefined) =>
		call(
			"POST /hubs/admin/negotiate",
			exchange("/hubs/admin/negotiate", "", auth, recordingStream()),
		);
	return { hub, adapter, call, negotiate };
}

function read(value: unknown, key: string): string {
	return String(Reflect.get(Object(value), key));
}

/** Negotiate, open the stream and complete the handshake, all as `openedAs`. */
async function openStream(
	openedAs: Auth | undefined,
	mounted = mount(),
	streamAuth: Auth | undefined = openedAs,
) {
	const { call, negotiate } = mounted;
	const sse = recordingStream();
	const negotiated = await negotiate(openedAs);
	const token = read(negotiated.body(), "connectionToken");
	const clientId = read(negotiated.body(), "connectionId");
	const url = `/hubs/admin?id=${token}`;
	const opened = await call(
		"GET /hubs/admin",
		exchange(url, "", streamAuth, sse),
	);
	const post = (raw: string, auth: Auth | undefined) =>
		call("POST /hubs/admin", exchange(url, raw, auth, sse));
	if (sse.opened === 1) {
		await post(HANDSHAKE, streamAuth);
		sse.frames.length = 0;
	}
	const invoke = (auth: Auth | undefined, id = "1") =>
		post(invocation("purgeUsers", [], id), auth);
	return { ...mounted, sse, opened, clientId, post, invoke };
}

/** Each frame the client received, parsed. */
function messages(frames: string[]): unknown[] {
	return frames
		.flatMap((frame) => frame.split(RS))
		.filter((piece) => piece !== "")
		.map((piece) => JSON.parse(piece));
}

afterEach(() => {
	vi.useRealTimers();
});

describe("hub > the guards are asked before a connection exists", () => {
	it("refuses an anonymous negotiate with 401 and issues no token", async () => {
		const { negotiate, adapter } = mount();

		const refused = await negotiate(undefined);

		expect(refused.status()).toBe(401);
		expect(refused.body()).toEqual({
			error: { code: "E_RELAY_UNAUTHORIZED", message: "Not authenticated" },
		});
		// Nothing was written to the token table: a flood of refused
		// negotiates takes no slot from the clients the hub does accept.
		expect(adapter.admits("anyone")).toBe(true);
	});

	it("refuses a signed-in user without the role with 403", async () => {
		const { negotiate } = mount();

		const refused = await negotiate(member());

		expect(refused.status()).toBe(403);
		expect(refused.body()).toEqual({
			error: { code: "E_RELAY_FORBIDDEN", message: "Insufficient role" },
		});
	});

	it("asks them again at the stream: a token does not carry the role", async () => {
		// The role is there at negotiate and gone when the stream opens.
		const { hub, sse, opened } = await openStream(admin(), mount(), member());

		expect(opened.status()).toBe(403);
		expect(opened.body()).toEqual({
			error: { code: "E_RELAY_FORBIDDEN", message: "Insufficient role" },
		});
		expect(sse.opened).toBe(0);
		expect(hub.stats().clients).toBe(0);
		expect(hub.connected).toEqual([]);
	});

	it("refuses a stream before it says anything about the token", async () => {
		const { call } = mount();
		const sse = recordingStream();

		const opened = await call(
			"GET /hubs/admin",
			exchange("/hubs/admin?id=not-a-token", "", undefined, sse),
		);

		expect(opened.status()).toBe(401);
		expect(sse.opened).toBe(0);
	});

	it("connects a client that meets them, and runs onConnect for it alone", async () => {
		const mounted = mount();
		await mounted.negotiate(undefined);
		await mounted.negotiate(member("u2"));

		const { hub, clientId, invoke, sse } = await openStream(admin(), mounted);
		await invoke(admin());

		expect(hub.connected).toEqual([clientId]);
		expect(hub.stats().clients).toBe(1);
		expect(messages(sse.frames)).toEqual([
			{ type: 3, invocationId: "1", result: "purged" },
		]);
		expect(hub.sawRoles).toEqual([["admin"]]);
	});

	it("leaves a hub with no guards open to an anonymous client", async () => {
		const { hub, sse, post } = await openStream(
			undefined,
			mount(new AdminHub(false)),
		);

		await post(invocation("announce", ["hello"]), undefined);

		expect(hub.connected).toHaveLength(1);
		expect(messages(sse.frames)).toEqual([
			{ type: 1, target: "announcement", arguments: ["hello"] },
			{ type: 1, target: "admins-only", arguments: ["hello"] },
			{ type: 3, invocationId: "1" },
		]);
	});
});

describe("hub > the guards are asked again on every invocation", () => {
	it("refuses the call and ends the connection once the role is revoked", async () => {
		const mounted = mount();
		const alice = await openStream(admin("alice"), mounted);
		const bob = await openStream(admin("bob"), mounted);

		const posted = await alice.invoke(member("alice"));

		expect(posted.status()).toBe(200);
		expect(alice.hub.sawRoles).toEqual([]);
		expect(messages(alice.sse.frames)).toEqual([
			{
				type: 1,
				target: "error",
				arguments: [
					{ code: "E_RELAY_FORBIDDEN", message: "Insufficient role" },
				],
			},
			{ type: 3, invocationId: "1", error: "Handler error" },
			{ type: 7, error: "Insufficient role" },
		]);
		expect(alice.sse.isOpen()).toBe(false);
		expect(alice.hub.left).toEqual([alice.clientId]);
		expect(alice.hub.stats().clients).toBe(1);

		// What the hub pushes from here on does not reach her.
		alice.sse.frames.length = 0;
		await bob.post(invocation("announce", ["payroll"]), admin("bob"));
		expect(alice.sse.frames).toEqual([]);
		expect(messages(bob.sse.frames)).toContainEqual({
			type: 1,
			target: "admins-only",
			arguments: ["payroll"],
		});

		// And her token is spent: getting the role back takes a new connection.
		const again = await alice.invoke(admin("alice"), "2");
		expect(again.status()).toBe(400);
		expect(again.body()).toMatchObject({
			error: { code: "E_RELAY_UNKNOWN_CONNECTION" },
		});
	});

	it("runs nothing that follows the refused call in the same request", async () => {
		const { hub, post } = await openStream(admin());

		await post(
			invocation("purgeUsers", [], "1") + invocation("purgeUsers", [], "2"),
			member(),
		);

		expect(hub.sawRoles).toEqual([]);
	});

	it("keeps serving a client whose role holds", async () => {
		const { sse, invoke } = await openStream(admin());

		await invoke(admin(), "1");
		await invoke(admin(), "2");

		expect(messages(sse.frames)).toEqual([
			{ type: 3, invocationId: "1", result: "purged" },
			{ type: 3, invocationId: "2", result: "purged" },
		]);
		expect(sse.isOpen()).toBe(true);
	});
});

describe("hub > a connection takes requests from the identity that opened it", () => {
	it("refuses an authenticated POST on a stream opened anonymously", async () => {
		// The token is the anonymous stream's only credential, and the answers
		// go down that stream: a victim's credentials on the POST — a
		// cross-site form — would have the guard pass for whoever holds it.
		const { hub, sse, invoke } = await openStream(
			undefined,
			mount(new AdminHub(false)),
		);

		const posted = await invoke(admin("victim"));

		expect(posted.status()).toBe(403);
		expect(posted.body()).toEqual({
			error: {
				code: "E_RELAY_NOT_OWNER",
				message: "This connection belongs to another user.",
			},
		});
		expect(hub.sawRoles).toEqual([]);
		expect(sse.frames).toEqual([]);
	});

	it("still refuses a POST from another user", async () => {
		const { hub, invoke } = await openStream(admin("u1"));

		const posted = await invoke(admin("u2"));

		expect(posted.status()).toBe(403);
		expect(hub.sawRoles).toEqual([]);
	});
});

describe("hub > a POST needs an open connection", () => {
	it("refuses frames posted before the stream opens, and records no handshake", async () => {
		const { negotiate, call, adapter, hub } = mount(new AdminHub(false));
		const negotiated = await negotiate(undefined);
		const token = read(negotiated.body(), "connectionToken");
		const clientId = read(negotiated.body(), "connectionId");

		const posted = await call(
			"POST /hubs/admin",
			exchange(
				`/hubs/admin?id=${token}`,
				HANDSHAKE,
				undefined,
				recordingStream(),
			),
		);

		expect(posted.status()).toBe(400);
		expect(posted.body()).toEqual({
			error: {
				code: "E_RELAY_NOT_CONNECTED",
				message: "This connection has no open stream — open it first.",
			},
		});
		expect(adapter.hasCompletedHandshake(clientId)).toBe(false);
		expect(hub.connected).toEqual([]);
	});
});

describe("hub > a hub that throws from onConnect has refused the connection", () => {
	it("ends the stream instead of leaving it registered", async () => {
		const mounted = mount(new AdminHub(false));
		mounted.hub.refuseOnConnect = true;
		const { call, negotiate, hub } = mounted;
		const sse = recordingStream();
		const negotiated = await negotiate(undefined);
		const url = `/hubs/admin?id=${read(negotiated.body(), "connectionToken")}`;
		await call("GET /hubs/admin", exchange(url, "", undefined, sse));
		expect(hub.stats().clients).toBe(1);

		await expect(
			call("POST /hubs/admin", exchange(url, HANDSHAKE, undefined, sse)),
		).rejects.toThrow("not welcome");

		expect(sse.isOpen()).toBe(false);
		expect(hub.stats().clients).toBe(0);
	});
});

describe("hub > ending a connection from the server", () => {
	it("closes the stream of a user the application disconnects", async () => {
		const mounted = mount();
		const alice = await openStream(admin("alice"), mounted);
		const bob = await openStream(admin("bob"), mounted);

		// What a logout calls.
		expect(mounted.hub.disconnectUser("alice")).toBe(1);

		// Off the hub at once; the stream is told, then closed.
		expect(mounted.hub.stats().clients).toBe(1);
		await bob.post(invocation("announce", ["after logout"]), admin("bob"));
		expect(messages(alice.sse.frames)).toEqual([{ type: 7 }]);
		expect(alice.sse.isOpen()).toBe(false);
		expect(bob.sse.isOpen()).toBe(true);
		const late = await alice.invoke(admin("alice"));
		expect(late.status()).toBe(400);
	});

	it("ends a connection when the access token that opened it expires", async () => {
		vi.useFakeTimers();
		const withToken = (expiresAt: Date | null): Auth => ({
			isAuthenticated: true,
			authenticatedViaGuard: "api",
			user: { id: "u1", currentAccessToken: { expiresAt } },
			roles: ["admin"],
		});
		const mounted = mount();
		const shortLived = await openStream(
			withToken(new Date(Date.now() + 60_000)),
			mounted,
		);
		const unlimited = await openStream(withToken(null), mounted);

		await vi.advanceTimersByTimeAsync(59_000);
		expect(shortLived.sse.isOpen()).toBe(true);

		await vi.advanceTimersByTimeAsync(1_000);
		expect(shortLived.sse.isOpen()).toBe(false);
		expect(mounted.hub.left).toEqual([shortLived.clientId]);
		expect(unlimited.sse.isOpen()).toBe(true);
	});

	it("waits out an expiry further away than one timer can hold", async () => {
		vi.useFakeTimers();
		const thirtyDays = 30 * 24 * 60 * 60 * 1000;
		const { sse } = await openStream({
			isAuthenticated: true,
			user: {
				id: "u1",
				currentAccessToken: { expiresAt: new Date(Date.now() + thirtyDays) },
			},
			roles: ["admin"],
		});

		// A timer takes 2^31 - 1 ms at most; a longer delay fired at once.
		await vi.advanceTimersByTimeAsync(2_147_483_647);
		expect(sse.isOpen()).toBe(true);

		await vi.advanceTimersByTimeAsync(thirtyDays - 2_147_483_647);
		expect(sse.isOpen()).toBe(false);
	});

	it("reads no expiry from a token that states none it can use", async () => {
		vi.useFakeTimers();
		const odd: unknown[] = ["2030-01-01", new Date(Number.NaN), 12];
		const streams = [];
		for (const [index, expiresAt] of odd.entries()) {
			streams.push(
				await openStream({
					isAuthenticated: true,
					user: { id: `u${index}`, currentAccessToken: { expiresAt } },
					roles: ["admin"],
				}),
			);
		}
		streams.push(
			await openStream({
				isAuthenticated: true,
				user: { id: "u9", currentAccessToken: "opaque" },
				roles: ["admin"],
			}),
		);

		await vi.advanceTimersByTimeAsync(10_000);

		expect(streams.map((stream) => stream.sse.isOpen())).toEqual([
			true,
			true,
			true,
			true,
		]);
	});
});

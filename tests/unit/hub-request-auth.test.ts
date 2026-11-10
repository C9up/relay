/**
 * A hub invocation is authorized against the request that carries it.
 *
 * Every invocation arrives on its own POST, authenticated afresh by the route's
 * middleware. The guards used to read the auth recorded when the stream opened,
 * so a user whose role was revoked kept calling guarded methods for the life of
 * the stream — and one who was granted a role could not use it until they
 * reconnected.
 */

import { describe, expect, it } from "vitest";
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

	constructor() {
		super();
		this.useGuards({ roles: ["admin"] });
	}

	async onPurgeUsers(ctx: HubContext): Promise<string> {
		this.sawRoles.push(ctx.auth.roles);
		return "purged";
	}
}

type Auth = NonNullable<HubHttpContext["auth"]>;
type Handler = (ctx: HubHttpContext) => Promise<void> | void;

interface Exchange {
	ctx: HubHttpContext;
	status: () => number;
	body: () => unknown;
}

function exchange(
	url: string,
	raw: string,
	auth: Auth | undefined,
	sse: HubSseStream,
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
			sse: async () => sse,
		},
		auth,
	};
	return { ctx, status: () => status, body: () => body };
}

function recordingStream(): HubSseStream & { frames: string[] } {
	const frames: string[] = [];
	return {
		id: "s",
		frames,
		isOpen: () => true,
		send: async (_event, data) => {
			frames.push(String(data));
			return true;
		},
		onClose: () => {},
		end: async () => {},
	};
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

/** Mount the hub, negotiate, open the stream with `openedAs`, complete the handshake. */
async function openStream(openedAs: Auth | undefined) {
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
	const hub = new AdminHub();
	registerHubRoutes(router, {
		path: "/hubs/admin",
		hub,
		adapter: new SignalRAdapter(hub),
	});
	const call = async (key: string, ex: Exchange): Promise<Exchange> => {
		const handler = routes.get(key);
		if (handler === undefined) throw new Error(`no route ${key}`);
		await handler(ex.ctx);
		return ex;
	};
	const sse = recordingStream();
	const negotiated = await call(
		"POST /hubs/admin/negotiate",
		exchange("/hubs/admin/negotiate", "", openedAs, sse),
	);
	const token = String(
		Reflect.get(Object(negotiated.body()), "connectionToken"),
	);
	const url = `/hubs/admin?id=${token}`;
	await call("GET /hubs/admin", exchange(url, "", openedAs, sse));
	await call("POST /hubs/admin", exchange(url, HANDSHAKE, openedAs, sse));
	sse.frames.length = 0;

	const invoke = (auth: Auth | undefined, id = "1") =>
		call(
			"POST /hubs/admin",
			exchange(
				url,
				`${JSON.stringify({ type: 1, invocationId: id, target: "purgeUsers", arguments: [] })}${RS}`,
				auth,
				sse,
			),
		);
	return { hub, sse, invoke };
}

/** Each frame the client received, parsed. */
function messages(frames: string[]): unknown[] {
	return frames
		.flatMap((frame) => frame.split(RS))
		.filter((piece) => piece !== "")
		.map((piece) => JSON.parse(piece));
}

describe("hub > guards read the invoking request's auth", () => {
	it("refuses an invocation once the role is revoked, though the stream opened as admin", async () => {
		const { hub, sse, invoke } = await openStream(admin());

		const posted = await invoke(member());

		expect(posted.status()).toBe(200);
		expect(hub.sawRoles).toEqual([]);
		expect(messages(sse.frames)).toEqual([
			{
				type: 1,
				target: "error",
				arguments: [
					{ code: "E_RELAY_FORBIDDEN", message: "Insufficient role" },
				],
			},
			{ type: 3, invocationId: "1", error: "Handler error" },
		]);
	});

	it("allows an invocation once the role is granted, though the stream opened without it", async () => {
		const { hub, sse, invoke } = await openStream(member());

		await invoke(admin());

		expect(messages(sse.frames)).toEqual([
			{ type: 3, invocationId: "1", result: "purged" },
		]);
		// The handler's context carries the same auth the guard checked.
		expect(hub.sawRoles).toEqual([["admin"]]);
	});

	it("follows the role from one request to the next on the same stream", async () => {
		const { sse, invoke } = await openStream(admin());

		await invoke(admin(), "1");
		await invoke(member(), "2");
		await invoke(admin(), "3");

		const completions = messages(sse.frames).filter(
			(message) => Reflect.get(Object(message), "type") === 3,
		);
		expect(completions).toEqual([
			{ type: 3, invocationId: "1", result: "purged" },
			{ type: 3, invocationId: "2", error: "Handler error" },
			{ type: 3, invocationId: "3", result: "purged" },
		]);
	});

	it("refuses an authenticated POST on a stream opened anonymously", async () => {
		// The token is the anonymous stream's only credential, and the answers
		// go down that stream: a victim's credentials on the POST — a
		// cross-site form — would have the guard pass for whoever holds it.
		const { hub, sse, invoke } = await openStream(undefined);

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

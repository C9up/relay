/**
 * A stock `@microsoft/signalr` client against a hub mounted on a booted ream
 * application, on the native server: negotiate announces WebSockets, the
 * client takes them, invokes hub methods and receives group sends; hub guards
 * decide who connects, from what the request's auth carries; and the same
 * path still serves a client that asks for Server-Sent Events.
 */

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
import { SignalRAdapter } from "../../src/SignalRAdapter.js";
import { registerHubRoutes } from "../../src/SignalRTransport.js";

class RoomHub extends Hub {
	async onJoin(ctx: HubContext, room: string): Promise<string> {
		ctx.joinGroup(`room:${room}`);
		return `joined ${room}`;
	}

	async onSay(ctx: HubContext, room: string, text: string): Promise<void> {
		ctx.group(`room:${room}`).send("said", { from: ctx.clientId, text });
	}
}

/** Members only: the guard reads the roles the handshake's auth carried. */
class MembersHub extends Hub {
	constructor() {
		super();
		this.useGuards({ roles: ["member"] });
	}

	async onWhoami(ctx: HubContext): Promise<unknown> {
		return ctx.auth.user?.id ?? null;
	}
}

/**
 * Stands in for an auth guard: `x-user: alice:member` signs alice in with the
 * member role.
 */
const signIn: MiddlewareFunction = async (ctx, next) => {
	const header = ctx.request.header("x-user");
	if (header !== undefined) {
		const [id, role] = header.split(":");
		Reflect.set(ctx, "auth", {
			isAuthenticated: true,
			authenticatedViaGuard: "web",
			user: { id },
			roles: role === undefined ? [] : [role],
		});
	}
	await next();
};

describe("relay > a stock SignalR client over ream's WebSockets", () => {
	let ignitor: Ignitor;
	let base = "";
	const open: HubConnection[] = [];

	beforeAll(async () => {
		const serverFactory = createHyperServerFactory();
		const webSockets = (): boolean => ignitor.getServer().supportsWebSockets();
		ignitor = await new Ignitor({
			port: 0,
			gracefulShutdown: false,
			serverFactory,
		})
			.routes((router) => {
				for (const [path, hub] of [
					["/hubs/rooms", new RoomHub()],
					["/hubs/members", new MembersHub()],
				] as const) {
					const routes = registerHubRoutes(
						router,
						{ path, hub, adapter: new SignalRAdapter(hub) },
						{ webSockets },
					);
					// Every hub route signs the caller in, the handshake included.
					for (const route of [
						routes.negotiate,
						routes.stream,
						routes.send,
						routes.socket,
					]) {
						route?.use(signIn);
					}
				}
			})
			.httpServer()
			.start();
		base = `http://127.0.0.1:${await ignitor.port()}`;
	});

	afterAll(async () => {
		await Promise.all(open.map((connection) => connection.stop()));
		await ignitor.stop();
	});

	async function connect(
		path: string,
		transport: HttpTransportType,
		user?: string,
	): Promise<HubConnection> {
		const connection = new HubConnectionBuilder()
			.withUrl(`${base}${path}`, {
				transport,
				headers: user === undefined ? {} : { "x-user": user },
			})
			.configureLogging(LogLevel.None)
			.build();
		await connection.start();
		open.push(connection);
		return connection;
	}

	it("announces WebSockets in negotiate", async () => {
		const res = await fetch(`${base}/hubs/rooms/negotiate?negotiateVersion=1`, {
			method: "POST",
		});
		const body: unknown = await res.json();
		const transports =
			typeof body === "object" && body !== null
				? Reflect.get(body, "availableTransports")
				: undefined;
		expect(transports).toEqual([
			{ transport: "WebSockets", transferFormats: ["Text"] },
			{ transport: "ServerSentEvents", transferFormats: ["Text"] },
		]);
	});

	it("connects over WebSockets, invokes, and receives a group send", async () => {
		const alice = await connect(
			"/hubs/rooms",
			HttpTransportType.WebSockets,
			"alice:member",
		);
		const bob = await connect(
			"/hubs/rooms",
			HttpTransportType.WebSockets,
			"bob:member",
		);
		expect(await alice.invoke("join", "lobby")).toBe("joined lobby");
		expect(await bob.invoke("join", "lobby")).toBe("joined lobby");

		const heard = new Promise<unknown>((resolve) => {
			bob.on("said", resolve);
		});
		await alice.invoke("say", "lobby", "bonjour");
		expect(await heard).toEqual({
			from: alice.connectionId,
			text: "bonjour",
		});
	});

	it("serves a Server-Sent Events client on the same path, beside the sockets", async () => {
		const sse = await connect(
			"/hubs/rooms",
			HttpTransportType.ServerSentEvents,
			"carol:member",
		);
		const ws = await connect(
			"/hubs/rooms",
			HttpTransportType.WebSockets,
			"dave:member",
		);
		await sse.invoke("join", "mixed");
		await ws.invoke("join", "mixed");
		const heard = new Promise<unknown>((resolve) => {
			sse.on("said", resolve);
		});
		await ws.invoke("say", "mixed", "across transports");
		expect(await heard).toEqual({
			from: ws.connectionId,
			text: "across transports",
		});
	});

	it("applies the hub's guards to the handshake's auth", async () => {
		const member = await connect(
			"/hubs/members",
			HttpTransportType.WebSockets,
			"erin:member",
		);
		expect(await member.invoke("whoami")).toBe("erin");

		// Without the role there is no connection to invoke on: the negotiate
		// is refused.
		await expect(
			connect("/hubs/members", HttpTransportType.WebSockets, "frank:visitor"),
		).rejects.toThrow(/Status code '403'/);
	});

	it("refuses a socket opened on the token by anyone but its owner", async () => {
		const res = await fetch(`${base}/hubs/rooms/negotiate?negotiateVersion=1`, {
			method: "POST",
			headers: { "x-user": "grace:member" },
		});
		const body: unknown = await res.json();
		const token =
			typeof body === "object" && body !== null
				? Reflect.get(body, "connectionToken")
				: undefined;
		if (typeof token !== "string") throw new Error("no connection token");
		// The token reaches logs and proxies with the URL. A socket opened on it
		// without grace's credentials — anonymous here, Node's WebSocket sends no
		// custom header — is refused at the handshake, before any socket opens.
		const stranger = new WebSocket(
			`${base.replace("http", "ws")}/hubs/rooms?id=${token}`,
		);
		const outcome = await new Promise<string>((resolve) => {
			stranger.addEventListener("open", () => resolve("opened"));
			stranger.addEventListener("error", () => resolve("refused"));
		});
		expect(outcome).toBe("refused");
	});
});

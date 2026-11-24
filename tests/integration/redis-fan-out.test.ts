/**
 * Two ream applications, each with its own relay, on one real Redis: a hub
 * send made on instance A reaches the clients connected to instance B, over
 * WebSockets and over Server-Sent Events, through stock `@microsoft/signalr`
 * clients. Then Redis drops every connection, and delivery has to come back
 * on its own.
 *
 * Needs a Redis server on REDIS_TEST_URL. CI starts one (relay-ci.yml) and
 * this suite refuses to run there without it. Outside CI, with no URL, the
 * suite is not declared at all, so nothing reads as a skipped pass.
 */

import { QuasarConnection } from "@c9up/quasar";
import { Ignitor } from "@c9up/ream";
import { createHyperServerFactory } from "@c9up/ream/bootstrap";
import {
	HttpTransportType,
	type HubConnection,
	HubConnectionBuilder,
	LogLevel,
} from "@microsoft/signalr";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { Hub, type HubContext } from "../../src/Hub.js";
import { RedisRelayTransport } from "../../src/RedisRelayTransport.js";
import { Relay } from "../../src/Relay.js";
import { SignalRAdapter } from "../../src/SignalRAdapter.js";
import { registerHubRoutes } from "../../src/SignalRTransport.js";

const url = process.env.REDIS_TEST_URL;
if (url === undefined && process.env.CI !== undefined) {
	throw new Error(
		"REDIS_TEST_URL is not set: relay's CI starts a Redis service for this suite.",
	);
}

class RoomHub extends Hub {
	async onJoin(ctx: HubContext, room: string): Promise<string> {
		ctx.joinGroup(`room:${room}`);
		return `joined ${room}`;
	}

	async onSay(ctx: HubContext, room: string, text: string): Promise<void> {
		ctx.group(`room:${room}`).send("said", { text });
	}
}

interface Instance {
	ignitor: Ignitor;
	relay: Relay;
	connection: QuasarConnection;
	base: string;
}

/** Waits until `check` holds, or fails with `what` after `ms`. */
async function until(
	check: () => boolean,
	what: string,
	ms = 10_000,
): Promise<void> {
	const deadline = Date.now() + ms;
	while (!check()) {
		if (Date.now() > deadline) throw new Error(`timed out waiting for ${what}`);
		await new Promise((resolve) => setTimeout(resolve, 25));
	}
}

if (url !== undefined) {
	describe("relay > hub sends across two instances on one Redis", () => {
		const instances: Instance[] = [];
		const open: HubConnection[] = [];
		let admin: QuasarConnection;
		// One channel per run, so two runs on one server never hear each other.
		const channel = `relay-test::${process.pid}::${Date.now()}`;

		async function start(name: string): Promise<Instance> {
			const serverFactory = createHyperServerFactory();
			const connection = new QuasarConnection(name, { url });
			const relay = new Relay({
				transport: new RedisRelayTransport(connection),
				transportChannel: channel,
			});
			const hub = new RoomHub();
			relay.hub("/hubs/rooms", hub);
			const ignitor: Ignitor = await new Ignitor({
				port: 0,
				gracefulShutdown: false,
				serverFactory,
			})
				.routes((router) => {
					registerHubRoutes(
						router,
						{ path: "/hubs/rooms", hub, adapter: new SignalRAdapter(hub) },
						{ webSockets: () => ignitor.getServer().supportsWebSockets() },
					);
				})
				.httpServer()
				.start();
			await relay.startTransport();
			const instance = {
				ignitor,
				relay,
				connection,
				base: `http://127.0.0.1:${await ignitor.port()}`,
			};
			instances.push(instance);
			return instance;
		}

		async function connect(
			instance: Instance,
			transport: HttpTransportType,
		): Promise<HubConnection> {
			const connection = new HubConnectionBuilder()
				.withUrl(`${instance.base}/hubs/rooms`, { transport })
				.configureLogging(LogLevel.None)
				.build();
			await connection.start();
			open.push(connection);
			return connection;
		}

		function heardBy(connection: HubConnection): unknown[] {
			const heard: unknown[] = [];
			connection.on("said", (payload: unknown) => {
				heard.push(payload);
			});
			return heard;
		}

		let a: Instance;
		let b: Instance;

		beforeAll(async () => {
			admin = new QuasarConnection("admin", { url });
			a = await start("a");
			b = await start("b");
		});

		afterAll(async () => {
			await Promise.all(open.map((connection) => connection.stop()));
			for (const instance of instances) {
				await instance.relay.shutdown();
				await instance.ignitor.stop();
				await instance.connection.quit();
			}
			await admin?.quit();
		});

		it("delivers a group send made on A to a WebSocket and an SSE client on B", {
			timeout: 30_000,
		}, async () => {
			const sender = await connect(a, HttpTransportType.WebSockets);
			const onSocket = await connect(b, HttpTransportType.WebSockets);
			const onStream = await connect(b, HttpTransportType.ServerSentEvents);
			const outsider = await connect(b, HttpTransportType.WebSockets);
			expect(await onSocket.invoke("join", "kitchen")).toBe("joined kitchen");
			expect(await onStream.invoke("join", "kitchen")).toBe("joined kitchen");
			const socketHeard = heardBy(onSocket);
			const streamHeard = heardBy(onStream);
			const outsiderHeard = heardBy(outsider);

			await sender.invoke("say", "kitchen", "from A");

			await until(
				() => socketHeard.length > 0 && streamHeard.length > 0,
				"B's clients to hear A's send",
			);
			expect(socketHeard).toEqual([{ text: "from A" }]);
			expect(streamHeard).toEqual([{ text: "from A" }]);
			expect(outsiderHeard).toEqual([]);
		});

		it("delivers again once Redis has dropped every connection", {
			timeout: 30_000,
		}, async () => {
			const sender = await connect(a, HttpTransportType.WebSockets);
			const listener = await connect(b, HttpTransportType.WebSockets);
			await listener.invoke("join", "after-restart");
			const heard = heardBy(listener);

			// Every client but this one: both instances' command and subscriber
			// sockets go, as they do when the server restarts.
			await admin.ioConnection.call("CLIENT", "KILL", "SKIPME", "yes");
			await until(
				() =>
					[a, b].every(
						(instance) =>
							instance.connection.status === "ready" &&
							instance.connection.subscriberStatus === "ready",
					),
				"both instances to reconnect to Redis",
			);
			// ioredis resubscribes after reconnecting; the channel is live once
			// Redis counts both instances on it again.
			let subscribers = 0;
			const deadline = Date.now() + 10_000;
			while (subscribers < 2 && Date.now() < deadline) {
				const reply: unknown = await admin.ioConnection.call(
					"PUBSUB",
					"NUMSUB",
					channel,
				);
				subscribers = Array.isArray(reply) ? Number(reply[1]) : 0;
				if (subscribers < 2) await new Promise((r) => setTimeout(r, 25));
			}
			expect(subscribers).toBe(2);

			await sender.invoke("say", "after-restart", "back");
			await until(() => heard.length > 0, "B to hear A after the reconnect");
			expect(heard).toEqual([{ text: "back" }]);
		});
	});
}

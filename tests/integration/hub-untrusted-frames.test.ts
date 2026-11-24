/**
 * What an anonymous client can send a hub, on the native server: frames
 * faster than the hub answers, a `__proto__` key in an argument, and a
 * handshake for a connection it never opens. Each is bounded or refused.
 *
 * The sizes are small on purpose: they prove the bound, not the attack.
 */

import { Ignitor } from "@c9up/ream";
import { createHyperServerFactory } from "@c9up/ream/bootstrap";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { Hub, type HubContext } from "../../src/Hub.js";
import { SignalRAdapter } from "../../src/SignalRAdapter.js";
import { registerHubRoutes } from "../../src/SignalRTransport.js";

const RS = "\x1e";
const HANDSHAKE = `{"protocol":"json","version":1}${RS}`;
const MAX_QUEUED = 8;

class WorkHub extends Hub {
	started = 0;
	ticks = 0;

	/** Any method that awaits: a query, an HTTP call. */
	async onWork(): Promise<void> {
		this.started++;
		await new Promise((resolve) => setTimeout(resolve, 25));
	}

	async onTick(): Promise<void> {
		this.ticks++;
	}

	async onSave(_ctx: HubContext, patch: object): Promise<unknown> {
		const settings = Object.assign({ theme: "light" }, patch);
		return {
			argumentOwnKeys: Object.getOwnPropertyNames(patch),
			ownKeys: Object.keys(settings),
			isAdmin: Reflect.get(settings, "isAdmin") ?? null,
			plainObject: Object.getPrototypeOf(settings) === Object.prototype,
		};
	}
}

const SAVE = `{"type":1,"invocationId":"1","target":"save","arguments":[{"theme":"dark","__proto__":{"isAdmin":true}}]}${RS}`;
const SAVED = {
	type: 3,
	invocationId: "1",
	result: {
		argumentOwnKeys: ["theme"],
		ownKeys: ["theme"],
		isAdmin: null,
		plainObject: true,
	},
};

describe("relay > what a client sends a hub is bounded and cleaned", () => {
	let ignitor: Ignitor;
	let base = "";
	const hub = new WorkHub();
	// `tokenTtlMs` is lowered only to keep the unclaimed-token test short.
	const adapter = new SignalRAdapter(hub, {
		maxQueuedMessages: MAX_QUEUED,
		tokenTtlMs: 300,
	});

	beforeAll(async () => {
		const webSockets = (): boolean => ignitor.getServer().supportsWebSockets();
		ignitor = await new Ignitor({
			port: 0,
			gracefulShutdown: false,
			serverFactory: createHyperServerFactory(),
		})
			.routes((router) => {
				registerHubRoutes(
					router,
					{ path: "/hubs/work", hub, adapter },
					{ webSockets },
				);
			})
			.httpServer()
			.start();
		base = `http://127.0.0.1:${await ignitor.port()}`;
	});

	afterAll(async () => {
		await ignitor.stop();
	});

	async function negotiate(): Promise<{
		status: number;
		id: string;
		token: string;
	}> {
		const res = await fetch(`${base}/hubs/work/negotiate?negotiateVersion=1`, {
			method: "POST",
		});
		const body: unknown = await res.json();
		const read = (key: string): string => {
			const value: unknown = Reflect.get(Object(body), key);
			return typeof value === "string" ? value : "";
		};
		return {
			status: res.status,
			id: read("connectionId"),
			token: read("connectionToken"),
		};
	}

	/** A socket past its handshake, what it received, and how it closed. */
	async function openSocket() {
		const { token } = await negotiate();
		const ws = new WebSocket(
			`${base.replace("http", "ws")}/hubs/work?id=${token}`,
		);
		const frames: string[] = [];
		ws.addEventListener("message", (event) => frames.push(String(event.data)));
		const closed = new Promise<{ code: number; reason: string }>((resolve) => {
			ws.addEventListener("close", (event) => {
				resolve({ code: event.code, reason: event.reason });
			});
		});
		await new Promise<void>((resolve, reject) => {
			ws.addEventListener("open", () => resolve());
			ws.addEventListener("error", () => reject(new Error("refused")));
		});
		ws.send(HANDSHAKE);
		return { ws, frames, closed };
	}

	async function until(done: () => boolean, ms = 3000): Promise<void> {
		const deadline = Date.now() + ms;
		while (!done() && Date.now() < deadline) {
			await new Promise((resolve) => setTimeout(resolve, 10));
		}
	}

	it("closes a socket with 1013 once its backlog passes maxQueuedMessages", async () => {
		const { ws, closed } = await openSocket();
		hub.started = 0;
		const sent = 40;

		const frame = `{"type":1,"target":"work","arguments":[]}${RS}`;
		for (let i = 0; i < sent; i++) ws.send(frame);

		expect(await closed).toEqual({
			code: 1013,
			reason: "Too many hub messages waiting",
		});
		// The one running when the bound was hit may finish; what waited is
		// dropped, so nowhere near everything the client sent ever starts.
		await new Promise((resolve) => setTimeout(resolve, 150));
		expect(hub.started).toBeLessThanOrEqual(MAX_QUEUED + 1);
		expect(hub.started).toBeLessThan(sent);
	});

	it("refuses one message that packs more invocations than may wait", async () => {
		const { ws, closed } = await openSocket();
		hub.ticks = 0;

		ws.send(`{"type":1,"target":"tick","arguments":[]}${RS}`.repeat(200));

		expect((await closed).code).toBe(1013);
		expect(hub.ticks).toBe(0);
	});

	it("runs every invocation of a message within the bound", async () => {
		const { ws, frames } = await openSocket();
		hub.ticks = 0;

		ws.send(
			`{"type":1,"target":"tick","arguments":[]}${RS}`.repeat(MAX_QUEUED - 1) +
				`{"type":1,"invocationId":"last","target":"tick","arguments":[]}${RS}`,
		);

		await until(() => frames.some((frame) => frame.includes('"last"')));
		expect(hub.ticks).toBe(MAX_QUEUED);
		ws.close();
	});

	it("hands a hub method its argument without the __proto__ key (socket)", async () => {
		const { ws, frames } = await openSocket();

		ws.send(SAVE);

		await until(() => frames.some((frame) => frame.includes('"type":3')));
		const completion = frames.find((frame) => frame.includes('"type":3')) ?? "";
		expect(JSON.parse(completion.replace(RS, ""))).toEqual(SAVED);
		ws.close();
	});

	it("hands a hub method its argument without the __proto__ key (upstream POST)", async () => {
		const { token } = await negotiate();
		const stream = await fetch(`${base}/hubs/work?id=${token}`, {
			headers: { accept: "text/event-stream" },
		});
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

		// The raw body: the application's body parser is not on this path.
		const post = await fetch(`${base}/hubs/work?id=${token}`, {
			method: "POST",
			headers: { "content-type": "application/json" },
			body: HANDSHAKE + SAVE,
		});

		expect(post.status).toBe(200);
		await until(() => received.includes('"type":3'));
		const line = received
			.split("\n")
			.find((candidate) => candidate.includes('"type":3'));
		expect(
			JSON.parse((line ?? "").replace("data: ", "").replace(RS, "")),
		).toEqual(SAVED);
		await reader.cancel();
		await pump.catch(() => {});
	});

	it("keeps no handshake record for a connection that never opens its stream", async () => {
		const negotiated = [];
		for (let i = 0; i < 5; i++) {
			const one = await negotiate();
			expect(one.status).toBe(200);
			// The upstream POST of a client that never opens its stream.
			const post = await fetch(`${base}/hubs/work?id=${one.token}`, {
				method: "POST",
				body: HANDSHAKE,
			});
			expect(post.status).toBe(400);
			expect(await post.json()).toMatchObject({
				error: { code: "E_RELAY_NOT_CONNECTED" },
			});
			negotiated.push(one);
		}

		expect(
			negotiated.filter((one) => adapter.hasCompletedHandshake(one.id)),
		).toEqual([]);
		// The tokens run out, and nothing of those connections is left.
		await new Promise((resolve) => setTimeout(resolve, 400));
		expect(
			negotiated.filter((one) => adapter.resolveToken(one.token) !== undefined),
		).toEqual([]);
		expect(
			negotiated.filter((one) => adapter.hasCompletedHandshake(one.id)),
		).toEqual([]);
	});
});

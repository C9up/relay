/**
 * A hub's group sends and broadcasts cross instances through the relay's bus:
 * a client joins a group on the instance it is connected to, so a send made
 * on another instance has to reach it there. Delivered once — the sending
 * instance ignores its own publication — and never handed back to the bus.
 */

import { describe, expect, it, vi } from "vitest";
import { Hub, type HubContext } from "../../src/Hub.js";
import { Relay, type RelayTransport } from "../../src/Relay.js";

class RoomHub extends Hub {
	async onJoin(ctx: HubContext, room: string): Promise<void> {
		ctx.joinGroup(`room:${room}`);
	}

	async onSay(ctx: HubContext, room: string, text: string): Promise<void> {
		ctx.group(`room:${room}`).send("said", { text });
	}

	async onAnnounce(ctx: HubContext, text: string): Promise<void> {
		ctx.broadcast("announced", { text });
	}
}

/** One in-process bus delivering every publication to every subscriber. */
function sharedBus(): RelayTransport & { published: unknown[] } {
	const handlers: Array<(message: unknown) => void> = [];
	const published: unknown[] = [];
	return {
		published,
		publish: (_channel, message) => {
			published.push(message);
			// Through JSON, as a real bus carries it.
			const wire: unknown = JSON.parse(JSON.stringify(message));
			for (const handler of handlers) handler(wire);
		},
		subscribe: (_channel, handler) => {
			handlers.push(handler);
		},
	};
}

interface Received {
	event: string;
	data: unknown;
}

function connect(hub: Hub, id: string): { ctx: HubContext; got: Received[] } {
	const got: Received[] = [];
	const ctx = hub.registerClient({
		id,
		groups: new Set(),
		auth: { isAuthenticated: false },
		send: (event, data) => {
			got.push({ event, data });
		},
	});
	return { ctx, got };
}

async function twoInstances(bus: RelayTransport) {
	const a = new Relay({ transport: bus });
	const b = new Relay({ transport: bus });
	const hubA = new RoomHub();
	const hubB = new RoomHub();
	a.hub("/hubs/rooms", hubA);
	b.hub("/hubs/rooms", hubB);
	await a.startTransport();
	await b.startTransport();
	return { a, b, hubA, hubB };
}

describe("hub sends across instances", () => {
	it("delivers a group send made on A to the members connected to B, once", async () => {
		const { hubA, hubB } = await twoInstances(sharedBus());
		const onA = connect(hubA, "a1");
		const onB = connect(hubB, "b1");
		const outsider = connect(hubB, "b2");
		await hubA.dispatch("a1", "join", "kitchen");
		await hubB.dispatch("b1", "join", "kitchen");

		await hubA.dispatch("a1", "say", "kitchen", "hello");

		expect(onA.got).toEqual([{ event: "said", data: { text: "hello" } }]);
		expect(onB.got).toEqual([{ event: "said", data: { text: "hello" } }]);
		expect(outsider.got).toEqual([]);
	});

	it("delivers a broadcast to every client of the hub on every instance", async () => {
		const { hubA, hubB } = await twoInstances(sharedBus());
		const onA = connect(hubA, "a1");
		const onB = connect(hubB, "b1");

		await hubB.dispatch("b1", "announce", "closing");

		expect(onA.got).toEqual([
			{ event: "announced", data: { text: "closing" } },
		]);
		expect(onB.got).toEqual([
			{ event: "announced", data: { text: "closing" } },
		]);
	});

	it("publishes each send once, and a received one is never published again", async () => {
		const bus = sharedBus();
		const { hubA, hubB } = await twoInstances(bus);
		connect(hubA, "a1");
		connect(hubB, "b1");
		await hubA.dispatch("a1", "say", "kitchen", "hello");
		expect(bus.published).toEqual([
			{
				type: "hub",
				hub: "/hubs/rooms",
				target: { kind: "group", group: "room:kitchen" },
				event: "said",
				data: { text: "hello" },
				origin: expect.any(String),
			},
		]);
	});

	it("keeps a hub mounted at another path out of it", async () => {
		const bus = sharedBus();
		const a = new Relay({ transport: bus });
		const b = new Relay({ transport: bus });
		const hubA = new RoomHub();
		const other = new RoomHub();
		a.hub("/hubs/rooms", hubA);
		b.hub("/hubs/other", other);
		await a.startTransport();
		await b.startTransport();
		connect(hubA, "a1");
		const elsewhere = connect(other, "b1");
		await hubA.dispatch("a1", "announce", "x");
		expect(elsewhere.got).toEqual([]);
	});

	it("drops a malformed hub message from the bus", async () => {
		const handlers: Array<(message: unknown) => void> = [];
		const bus: RelayTransport = {
			publish: () => {},
			subscribe: (_channel, handler) => {
				handlers.push(handler);
			},
		};
		const relay = new Relay({ transport: bus });
		const hub = new RoomHub();
		relay.hub("/hubs/rooms", hub);
		await relay.startTransport();
		const client = connect(hub, "c1");
		const malformed: unknown[] = [
			{ type: "hub", hub: "/hubs/rooms", event: "x", data: 1 },
			{
				type: "hub",
				hub: "/hubs/rooms",
				target: { kind: "group" },
				event: "x",
				data: 1,
			},
			{
				type: "hub",
				hub: "/hubs/rooms",
				target: { kind: "group", group: "" },
				event: "x",
				data: 1,
			},
			{
				type: "hub",
				hub: "/hubs/rooms",
				target: { kind: "nobody" },
				event: "x",
				data: 1,
			},
			{
				type: "hub",
				hub: "/hubs/rooms",
				target: { kind: "all" },
				event: "",
				data: 1,
			},
			{ type: "hub", hub: "/hubs/rooms", target: { kind: "all" }, event: "x" },
			{ type: "hub", hub: "", target: { kind: "all" }, event: "x", data: 1 },
			{
				type: "hub",
				hub: "/hubs/rooms",
				target: { kind: "all" },
				event: "x",
				data: 1,
				origin: 3,
			},
		];
		for (const message of malformed) {
			for (const handler of handlers) handler(message);
		}
		expect(client.got).toEqual([]);
		for (const handler of handlers) {
			handler({
				type: "hub",
				hub: "/hubs/rooms",
				target: { kind: "all" },
				event: "x",
				data: 1,
			});
		}
		expect(client.got).toEqual([{ event: "x", data: 1 }]);
	});

	it("still delivers locally, and says so, when the publish fails", async () => {
		const write = vi.spyOn(process.stderr, "write").mockReturnValue(true);
		try {
			const bus: RelayTransport = {
				publish: () => Promise.reject(new Error("bus down")),
				subscribe: () => {},
			};
			const relay = new Relay({ transport: bus });
			const hub = new RoomHub();
			relay.hub("/hubs/rooms", hub);
			await relay.startTransport();
			const client = connect(hub, "c1");
			await hub.dispatch("c1", "announce", "x");
			expect(client.got).toEqual([{ event: "announced", data: { text: "x" } }]);
			await vi.waitFor(() => {
				expect(
					write.mock.calls.some((call) =>
						String(call[0]).includes("hub '/hubs/rooms'"),
					),
				).toBe(true);
			});
		} finally {
			write.mockRestore();
		}
	});

	it("refuses one hub instance mounted twice", () => {
		const hub = new RoomHub();
		new Relay({ transport: sharedBus() }).hub("/hubs/rooms", hub);
		expect(() =>
			new Relay({ transport: sharedBus() }).hub("/hubs/rooms", hub),
		).toThrow(/already mounted/);
		expect(() =>
			new Relay({ transport: sharedBus() }).hub("/hubs/rooms", hub),
		).toThrow(expect.objectContaining({ code: "E_RELAY_HUB_ALREADY_MOUNTED" }));
	});

	it("refuses a second hub at a path already mounted", () => {
		const relay = new Relay({ transport: sharedBus() });
		relay.hub("/hubs/rooms", new RoomHub());
		expect(() => relay.hub("/hubs/rooms", new RoomHub())).toThrow(
			expect.objectContaining({ code: "E_RELAY_HUB_PATH_TAKEN" }),
		);
	});

	it("stays local without a bus", async () => {
		const relay = new Relay();
		const hub = new RoomHub();
		relay.hub("/hubs/rooms", hub);
		const client = connect(hub, "c1");
		await hub.dispatch("c1", "announce", "x");
		expect(client.got).toEqual([{ event: "announced", data: { text: "x" } }]);
	});
});

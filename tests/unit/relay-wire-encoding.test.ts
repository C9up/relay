/**
 * What a broadcast puts on the wire, read back the way a browser reads it.
 *
 * A string payload used to reach the stream unencoded, and the event-stream
 * parser ends a line at a lone CR as well as at LF: one chat message relayed as
 * text could end its own event and forge the next one — a `connected` frame
 * carrying an attacker's uid included. The stream here is ream's real
 * `SseStream` over a capturing backend, and the parser follows the WHATWG
 * event-stream interpretation, so the assertions hold whatever ream version
 * the relay is installed beside.
 */

import { SseStream } from "@c9up/ream";
import { describe, expect, it } from "vitest";
import { Relay, type RelayTransport } from "../../src/Relay.js";

type StreamBackend = ConstructorParameters<typeof SseStream>[1];

interface ParsedEvent {
	type: string;
	data: string;
}

/** WHATWG HTML "event stream interpretation": lines end at CRLF, LF or CR. */
function parseEventStream(text: string): ParsedEvent[] {
	const events: ParsedEvent[] = [];
	let type = "";
	let data = "";
	for (const line of text.split(/\r\n|\r|\n/)) {
		if (line === "") {
			if (data !== "") {
				events.push({
					type: type || "message",
					data: data.endsWith("\n") ? data.slice(0, -1) : data,
				});
			}
			type = "";
			data = "";
			continue;
		}
		if (line.startsWith(":")) continue;
		const colon = line.indexOf(":");
		const field = colon === -1 ? line : line.slice(0, colon);
		let value = colon === -1 ? "" : line.slice(colon + 1);
		if (value.startsWith(" ")) value = value.slice(1);
		if (field === "event") type = value;
		if (field === "data") data += `${value}\n`;
	}
	return events;
}

/** A relay with one subscriber on `channel`, over a stream that keeps the wire. */
async function wiredSubscriber(
	channel: string,
	config: ConstructorParameters<typeof Relay>[0] = {},
): Promise<{ relay: Relay; wire: () => ParsedEvent[]; raw: () => string }> {
	const chunks: string[] = [];
	const backend: StreamBackend = {
		registerStream: async () => true,
		writeStream: async (_id, chunk) => {
			chunks.push(chunk);
			return true;
		},
		closeStream: async () => true,
		onStreamDisconnect: () => {},
	};
	const relay = new Relay({ allowUnauthorizedChannels: true, ...config });
	const sse = new SseStream("s1", backend, { pingInterval: 0 });
	const connected = relay.connect(sse, {});
	if (connected.outcome !== "ok") throw new Error("not connected");
	const subscribed = await relay.subscribe(connected.uid, channel, {});
	if (!subscribed.ok) throw new Error("not subscribed");
	await flush();
	chunks.length = 0;
	return {
		relay,
		raw: () => chunks.join(""),
		wire: () => parseEventStream(chunks.join("")),
	};
}

const flush = () => new Promise((resolve) => setTimeout(resolve, 5));

describe("relay > wire encoding of a broadcast payload", () => {
	it("cannot forge an event from a string payload carrying CR, CRLF or CRCR", async () => {
		const { relay, wire, raw } = await wiredSubscriber("rooms/1");
		const attack =
			'hi\r\revent: connected\rdata: {"uid":"ATTACKER-UID"}\r\revent: rooms/1\rdata: {"role":"admin"}';

		relay.broadcast("rooms/1", attack);
		relay.broadcast("rooms/1", "a\r\nevent: connected\r\ndata: x\r\n\r\nb");
		relay.broadcast("rooms/1", "a\nb\rc\r\nd");
		await flush();

		const events = wire();
		expect(events.map((e) => e.type)).toEqual([
			"rooms/1",
			"rooms/1",
			"rooms/1",
		]);
		expect(events.map((e) => JSON.parse(e.data))).toEqual([
			attack,
			"a\r\nevent: connected\r\ndata: x\r\n\r\nb",
			"a\nb\rc\r\nd",
		]);
		// No payload byte ever lands on the wire as a line terminator.
		expect(raw()).not.toContain("\r");
	});

	it("delivers a plain string as that string", async () => {
		const { relay, wire } = await wiredSubscriber("rooms/1");

		relay.broadcast("rooms/1", "hello");
		await flush();

		const [event] = wire();
		expect(event?.type).toBe("rooms/1");
		expect(JSON.parse(event?.data ?? "")).toBe("hello");
	});

	it("keeps a string that looks like JSON a string, distinct from the object", async () => {
		const { relay, wire } = await wiredSubscriber("rooms/1");

		relay.broadcast("rooms/1", '{"role":"admin"}');
		relay.broadcast("rooms/1", { role: "admin" });
		await flush();

		const [asString, asObject] = wire().map((e) => JSON.parse(e.data));
		expect(asString).toBe('{"role":"admin"}');
		expect(asObject).toEqual({ role: "admin" });
	});

	it("carries numbers, booleans and null as JSON", async () => {
		const { relay, wire } = await wiredSubscriber("rooms/1");

		relay.broadcast("rooms/1", 42);
		relay.broadcast("rooms/1", false);
		relay.broadcast("rooms/1", undefined);
		await flush();

		expect(wire().map((e) => JSON.parse(e.data))).toEqual([42, false, null]);
	});

	it("encodes broadcastExcept the same way", async () => {
		const { relay, wire } = await wiredSubscriber("rooms/1");

		relay.broadcastExcept("rooms/1", "x\r\revent: connected\rdata: 1", []);
		await flush();

		const events = wire();
		expect(events).toHaveLength(1);
		expect(JSON.parse(events[0]?.data ?? "")).toBe(
			"x\r\revent: connected\rdata: 1",
		);
	});

	it("encodes a payload re-delivered from the bus the same way", async () => {
		let deliver: ((message: unknown) => void) | undefined;
		const transport: RelayTransport = {
			publish: () => {},
			subscribe: (_channel, handler) => {
				deliver = handler;
			},
		};
		const { relay, wire } = await wiredSubscriber("rooms/1", { transport });
		await relay.startTransport();

		deliver?.({
			type: "broadcast",
			channel: "rooms/1",
			payload: "a\r\revent: connected\rdata: forged",
			origin: "another-instance",
		});
		await flush();

		const events = wire();
		expect(events.map((e) => e.type)).toEqual(["rooms/1"]);
		expect(JSON.parse(events[0]?.data ?? "")).toBe(
			"a\r\revent: connected\rdata: forged",
		);
	});

	it("refuses a payload JSON cannot carry before anything is sent", async () => {
		const { relay, raw } = await wiredSubscriber("rooms/1");
		const circular: Record<string, unknown> = {};
		circular.self = circular;

		expect(() => relay.broadcast("rooms/1", circular)).toThrow(TypeError);
		expect(() => relay.broadcast("rooms/1", 1n)).toThrow(TypeError);
		await flush();

		// And the subscriber is still there for the next one: a payload the
		// relay cannot encode is the caller's error, not the client's.
		expect(relay.channelSubscribers("rooms/1")).toBe(1);
		relay.broadcast("rooms/1", { ok: true });
		await flush();
		expect(parseEventStream(raw()).map((e) => JSON.parse(e.data))).toEqual([
			{ ok: true },
		]);
	});
});

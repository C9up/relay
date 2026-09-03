import { describe, expect, it } from "vitest";
import { FakeRelay } from "../../src/testing/FakeRelay.js";

describe("FakeRelay — broadcast capture", () => {
	it("broadcast captures the channel + data", () => {
		const r = new FakeRelay();
		const reached = r.broadcast("notifications", { msg: "hi" });
		expect(reached).toBe(0);
		expect(r.getSent()).toHaveLength(1);
		expect(r.getSent()[0].channel).toBe("notifications");
	});

	it("broadcast always returns 0 (no real clients in fake mode)", () => {
		const r = new FakeRelay();
		expect(r.broadcast("a", null)).toBe(0);
		expect(r.broadcast("b", null)).toBe(0);
	});

	it("getSent returns a defensive snapshot", () => {
		const r = new FakeRelay();
		r.broadcast("c", { x: 1 });
		const snap = r.getSent();
		snap[0].channel = "mutated";
		expect(r.getSent()[0].channel).toBe("c");
	});

	it("reset clears the captured array", () => {
		const r = new FakeRelay();
		r.broadcast("a", null);
		r.broadcast("b", null);
		r.reset();
		expect(r.getSent()).toHaveLength(0);
	});
});

describe("FakeRelay — assertSent", () => {
	it("passes when at least one broadcast matches the channel", () => {
		const r = new FakeRelay();
		r.broadcast("alerts", { level: "warn" });
		expect(() => r.assertSent("alerts")).not.toThrow();
	});

	it("throws when no broadcast matches the channel", () => {
		const r = new FakeRelay();
		r.broadcast("alerts", null);
		expect(() => r.assertSent("comments")).toThrow(
			/no captured broadcast matches/,
		);
	});

	it("dataMatches narrows the assertion", () => {
		const r = new FakeRelay();
		r.broadcast("alerts", { level: "warn" });
		r.broadcast("alerts", { level: "error" });
		expect(() =>
			r.assertSent("alerts", {
				dataMatches: (d) => (d as { level: string }).level === "error",
			}),
		).not.toThrow();
		expect(() =>
			r.assertSent("alerts", {
				dataMatches: (d) => (d as { level: string }).level === "trace",
			}),
		).toThrow(/no captured broadcast matches/);
	});

	it("function predicate gives full event access", () => {
		const r = new FakeRelay();
		r.broadcast("alerts", "high-priority");
		expect(() =>
			r.assertSent("alerts", (e) => e.data === "high-priority"),
		).not.toThrow();
	});

	it("error message includes captured summary", () => {
		const r = new FakeRelay();
		r.broadcast("alerts", { level: "info" });
		let err: unknown;
		try {
			r.assertSent("does-not-exist");
		} catch (e) {
			err = e;
		}
		expect(String(err)).toContain("Captured (1)");
		expect(String(err)).toContain("alerts");
	});
});

describe("FakeRelay — assertNotSent", () => {
	it("passes when no broadcast matches the channel", () => {
		const r = new FakeRelay();
		r.broadcast("a", null);
		expect(() => r.assertNotSent("b")).not.toThrow();
	});

	it("throws when at least one broadcast matches", () => {
		const r = new FakeRelay();
		r.broadcast("a", null);
		expect(() => r.assertNotSent("a")).toThrow(
			/at least one captured broadcast matches/,
		);
	});
});

describe("FakeRelay > payload parity with the real relay", () => {
	it("captures an absent payload as null, the way a real broadcast sends it", () => {
		const relay = new FakeRelay();

		relay.broadcast("feed", undefined);

		expect(relay.getSent()).toEqual([
			{ channel: "feed", data: null, clients: 0 },
		]);
	});
});

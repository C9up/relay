/**
 * What is registered before the provider binds its instance must survive.
 *
 * `services/main` hands out a Proxy that builds a default `Relay` on first
 * property access, so an application with no provider still has a usable
 * surface. The provider then builds its own — configured — instance and takes
 * the slot. Anything already registered on the default was left behind on an
 * object nothing served from, and the channel answered
 * `E_CHANNEL_NO_AUTHORIZER` at request time even though the application had
 * written the authorizer: both calls read as `relay.authorize(...)`.
 *
 * The whole module graph is re-imported per test. The slot has to be genuinely
 * EMPTY for the first access to build the lazy default this is about, and the
 * provider must call the same copy of `setRelay` the accessor exposes.
 */
import { describe, expect, it, vi } from "vitest";
import type { RelayContext, Relay as RelayType } from "../../src/Relay.js";
import type { RelayAppContext } from "../../src/RelayProvider.js";

async function freshGraph() {
	vi.resetModules();
	const { Relay } = await import("../../src/Relay.js");
	const { default: RelayProvider } = await import("../../src/RelayProvider.js");
	const { default: relay } = await import("../../src/services/main.js");
	return { Relay, RelayProvider, relay };
}

type Graph = Awaited<ReturnType<typeof freshGraph>>;

function makeApp(): RelayAppContext & {
	bind(token: unknown, v: unknown): void;
} {
	const bindings = new Map<unknown, () => unknown>();
	const cache = new Map<unknown, unknown>();
	return {
		bind(token: unknown, value: unknown) {
			bindings.set(token, () => value);
		},
		container: {
			singleton(token: unknown, factory: () => unknown) {
				bindings.set(token, factory);
			},
			async resolve<T = unknown>(token: unknown): Promise<T> {
				if (cache.has(token)) return cache.get(token) as T;
				const factory = bindings.get(token);
				if (!factory) throw new Error("not registered");
				const value = await factory();
				cache.set(token, value);
				return value as T;
			},
			has: (token: unknown) => bindings.has(token),
		},
		config: { get: () => undefined },
	} as unknown as RelayAppContext & { bind(token: unknown, v: unknown): void };
}

/** Run the provider's full lifecycle and hand back the instance it bound. */
async function served(g: Graph, app = makeApp()): Promise<RelayType> {
	const provider = new g.RelayProvider(app);
	provider.register();
	await provider.boot();
	await provider.start();
	await provider.ready();
	return app.container.resolve<RelayType>(g.Relay);
}

const authed = (id: string) =>
	({
		auth: { isAuthenticated: true, user: { id } },
	}) as unknown as RelayContext;

const sink = () =>
	({
		isOpen: () => true,
		send: async () => true,
		onClose: () => {},
		end: async () => {},
	}) as never;

/** Connect `id` and hand back the uid the relay issued — `subscribe` needs one. */
function connected(instance: RelayType, id: string): string {
	const outcome = instance.connect(id, sink(), { auth: authed(id).auth });
	if (outcome.outcome !== "ok") throw new Error(`connect: ${outcome.outcome}`);
	return outcome.uid;
}

describe("relay > registrations made before the provider bound its instance", () => {
	it("keeps an authorizer written through the accessor", async () => {
		const g = await freshGraph();
		g.relay.authorize("users/:id/notifications", (_ctx, { id }) => id === "1");

		const instance = await served(g);
		expect(
			await instance.subscribe(
				connected(instance, "1"),
				"users/1/notifications",
				authed("1"),
			),
		).toMatchObject({ ok: true });
		expect(
			await instance.subscribe(
				connected(instance, "2"),
				"users/2/notifications",
				authed("2"),
			),
		).toMatchObject({ code: "E_CHANNEL_FORBIDDEN" });
	});

	it("keeps a hub mounted through the accessor", async () => {
		const g = await freshGraph();
		g.relay.hub("/hubs/chat", { name: "chat" } as never);

		const instance = await served(g);
		expect(instance.mountedHubs().map((m) => m.path)).toContain("/hubs/chat");
	});

	it("keeps a lifecycle listener registered through the accessor", async () => {
		const g = await freshGraph();
		const seen: string[] = [];
		g.relay.on("subscribe", (event) => {
			seen.push(event.channel);
		});
		g.relay.authorize("open/*", () => true);

		const instance = await served(g);
		await instance.subscribe(
			connected(instance, "3"),
			"open/anything",
			authed("3"),
		);
		expect(seen).toEqual(["open/anything"]);
	});

	it("applies a route customizer registered after the provider started", async () => {
		// The host starts its providers BEFORE importing preloads, so a
		// customizer written in `start/services.ts` does not exist yet when
		// `start()` runs. Registering the routes at `ready()` is what lets it
		// reach the routes it is meant to decorate.
		const g = await freshGraph();
		g.relay.registerRoutes((route) => {
			route.middleware("auth");
		});

		const decorated: string[] = [];
		const builder = (path: string) => {
			const self = {
				middleware(...names: string[]) {
					decorated.push(`${path}:${names.join(",")}`);
					return self;
				},
				use: () => self,
				guard: () => self,
				role: () => self,
				permission: () => self,
			};
			return self;
		};
		const app = makeApp();
		app.bind("router", {
			get: (path: string) => builder(path),
			post: (path: string) => builder(path),
		});

		await served(g, app);
		expect(decorated).toContain("/__relay/events:auth");
	});

	it("lets the served instance win when both declared the same pattern", async () => {
		const g = await freshGraph();
		g.relay.authorize("shared", () => true);

		const instance = await served(g);
		instance.authorize("shared", () => false);

		expect(
			await instance.subscribe(connected(instance, "4"), "shared", authed("4")),
		).toMatchObject({ code: "E_CHANNEL_FORBIDDEN" });
	});
});

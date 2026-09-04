/**
 * The provider: what it binds, and the routes it puts on the host router.
 *
 * This is the path an application actually takes — the Relay class is
 * well covered and the wiring around it was not, which is the half that
 * decides whether a browser can connect at all.
 */
import { describe, expect, it, vi } from "vitest";
import {
	Relay,
	type RelayConfig,
	type RelaySseStream,
} from "../../src/Relay.js";
import RelayProvider from "../../src/RelayProvider.js";
import { getRelay } from "../../src/services/main.js";

type Handler = (ctx: unknown) => Promise<void> | void;

/** A router that records what was mounted on it. */
function fakeRouter() {
	const routes = new Map<string, Handler>();
	const builder = { as: () => builder, middleware: () => builder };
	return {
		routes,
		get(path: string, handler: Handler) {
			routes.set(`GET ${path}`, handler);
			return builder;
		},
		post(path: string, handler: Handler) {
			routes.set(`POST ${path}`, handler);
			return builder;
		},
	};
}

/** A container that caches, because `singleton` is the whole point. */
function fakeApp(
	options: { withRouter?: boolean; relayConfig?: RelayConfig } = {},
) {
	const factories = new Map<unknown, () => unknown>();
	const built = new Map<unknown, unknown>();
	const router = fakeRouter();
	const container = {
		singleton(token: unknown, factory: () => unknown) {
			factories.set(token, factory);
		},
		has(token: unknown) {
			return token === "router"
				? options.withRouter === true
				: factories.has(token);
		},
		async resolve<T>(token: unknown): Promise<T> {
			if (token === "router") return router as unknown as T;
			if (!built.has(token)) built.set(token, await factories.get(token)?.());
			return built.get(token) as T;
		},
	};
	return {
		router,
		factories,
		app: {
			container,
			config: {
				get: <T>() => options.relayConfig as T | undefined,
			},
		},
	};
}

/** A response double recording what the handler answered with. */
function fakeResponse() {
	const sent: Array<{ status?: number; body?: unknown }> = [];
	const frames: Array<{ event: string; data: unknown }> = [];
	let status: number | undefined;
	const response = {
		sent,
		frames,
		sseEnded: false,
		sseOpened: false,
		status(code: number) {
			status = code;
			return response;
		},
		header() {
			return response;
		},
		json(body: unknown) {
			sent.push({ status, body });
		},
		noContent() {
			sent.push({ status });
		},
		async sse(): Promise<RelaySseStream> {
			response.sseOpened = true;
			return {
				id: "s-1",
				isOpen: () => true,
				async send(event: string, data: unknown) {
					frames.push({ event, data });
					return true;
				},
				onClose() {},
				async end() {
					response.sseEnded = true;
				},
			};
		},
	};
	return response;
}

function fakeRequest(query: Record<string, string> = {}, body: unknown = {}) {
	return {
		header: () => undefined,
		body: () => body,
		qs: () => query,
		url: () => "/__relay/events",
		raw: () => "",
	};
}

describe("relay > provider > what it binds", () => {
	it("registers the relay under its class and the `relay` alias", async () => {
		const { app, factories } = fakeApp();

		new RelayProvider(app as never).register();

		expect(factories.has(Relay)).toBe(true);
		expect(await app.container.resolve(Relay)).toBeInstanceOf(Relay);
		expect(await app.container.resolve("relay")).toBe(
			await app.container.resolve(Relay),
		);
	});

	it("populates the service accessor at boot", async () => {
		const { app } = fakeApp();
		const provider = new RelayProvider(app as never);

		provider.register();
		await provider.boot();

		// Reachable from anywhere, without the container.
		expect(getRelay()).toBe(await app.container.resolve(Relay));
	});
});

describe("relay > provider > the routes", () => {
	it("mounts the three SSE endpoints when the host is Ream", async () => {
		const { app, router } = fakeApp({ withRouter: true });
		const provider = new RelayProvider(app as never);

		provider.register();
		await provider.boot();
		await provider.start();
		await provider.ready();

		expect([...router.routes.keys()].sort()).toEqual([
			"GET /__relay/events",
			"POST /__relay/subscribe",
			"POST /__relay/unsubscribe",
		]);
	});

	it("mounts nothing when the host registers no router", async () => {
		// A non-Ream host: broadcast and authorize still work through the
		// singleton, and the application wires its own SSE route.
		const { app, router } = fakeApp({ withRouter: false });
		const provider = new RelayProvider(app as never);

		provider.register();
		await provider.boot();
		await provider.start();
		await provider.ready();

		expect(router.routes.size).toBe(0);
	});

	it("closes a capped connection with an error frame, not half-open", async () => {
		const { app, router } = fakeApp({
			withRouter: true,
			relayConfig: { maxClients: 1 },
		});
		const provider = new RelayProvider(app as never);
		provider.register();
		await provider.boot();
		await provider.start();
		await provider.ready();

		const events = router.routes.get("GET /__relay/events");
		const first = fakeResponse();
		await events?.({
			request: fakeRequest(),
			response: first,
			auth: { isAuthenticated: true, user: { id: "u-a" } },
		});
		expect(first.sseEnded).toBe(false);

		const capped = fakeResponse();
		await events?.({
			request: fakeRequest(),
			response: capped,
			auth: { isAuthenticated: true, user: { id: "u-b" } },
		});

		// The stream is already open by the time the cap is known, so the
		// client is told why and the stream is closed. It used to be
		// documented as unreachable in tests, and as losing the frame to a
		// registry race that has since been fixed.
		expect(capped.frames).toEqual([
			{ event: "error", data: { code: "E_MAX_CLIENTS" } },
		]);
		expect(capped.sseEnded).toBe(true);
	});

	it("refuses a uid hint that claims someone else, without opening a stream", async () => {
		const { app, router } = fakeApp({ withRouter: true });
		const provider = new RelayProvider(app as never);
		provider.register();
		await provider.boot();
		await provider.start();
		await provider.ready();

		const response = fakeResponse();
		await router.routes.get("GET /__relay/events")?.({
			request: fakeRequest({ uid: "somebody-else" }),
			response,
			auth: { isAuthenticated: true, user: { id: "u-a" } },
		});

		// Answering after the upgrade would leave a half-open stream behind;
		// this one is a pure auth-versus-claim comparison and is buffered.
		expect(response.sseOpened).toBe(false);
		expect(response.sent[0]?.status).toBe(403);
	});

	it("opens the stream when the hint matches the authenticated user", async () => {
		const { app, router } = fakeApp({ withRouter: true });
		const provider = new RelayProvider(app as never);
		provider.register();
		await provider.boot();
		await provider.start();
		await provider.ready();

		const response = fakeResponse();
		await router.routes.get("GET /__relay/events")?.({
			request: fakeRequest({ uid: "u-a" }),
			response,
			auth: { isAuthenticated: true, user: { id: "u-a" } },
		});

		expect(response.sseOpened).toBe(true);
	});
});

describe("relay > provider > shutdown", () => {
	it("releases the bus so a redeploy leaks no transport handle", async () => {
		const { app } = fakeApp();
		const provider = new RelayProvider(app as never);
		provider.register();
		await provider.boot();

		const relay = await app.container.resolve<Relay>(Relay);
		const release = vi.spyOn(relay, "shutdown");

		await provider.shutdown();

		expect(release).toHaveBeenCalled();
	});
});

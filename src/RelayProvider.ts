import "./augmentations.js";
import { Hub } from "./Hub.js";
import { Relay, type RelayConfig, type RelayRouteBuilder } from "./Relay.js";
import { SignalRAdapter } from "./SignalRAdapter.js";
import { registerHubRoutes } from "./SignalRTransport.js";
import { clearRelay, getRelay, setRelay } from "./services/main.js";

interface RelayContainer {
	singleton(token: unknown, factory: () => unknown): void;
	resolve<T = unknown>(token: unknown): Promise<T>;
	has(token: unknown): boolean;
}

interface RelayConfigStore {
	get<T = unknown>(key: string): T | undefined;
}

export interface RelayAppContext {
	container: RelayContainer;
	config: RelayConfigStore;
}

/**
 * RelayProvider — registers the Relay singleton in the host container
 * and auto-binds the three SSE routes (`/__relay/events`,
 * `/__relay/subscribe`, `/__relay/unsubscribe`) when the host is Ream
 * (detected by importing `@c9up/ream/services/router`).
 *
 * The duck-typed `RelayAppContext` keeps this provider usable in any
 * framework that exposes a Container — non-Ream hosts get the
 * singleton bindings and skip the route registration silently.
 *
 * @example
 *   // reamrc.ts
 *   providers: [() => import('@c9up/relay/provider')]
 *
 *   // start/realtime.ts
 *   import relay from '@c9up/relay/services/main'
 *
 *   relay.authorize('users/:id/notifications', (ctx, { id }) =>
 *     ctx.auth?.user?.id === id
 *   )
 */
export default class RelayProvider {
	constructor(protected app: RelayAppContext) {}

	register(): void {
		this.app.container.singleton(Relay, () => {
			const config = this.app.config.get<RelayConfig>("relay");
			const instance = new Relay(config);
			setRelay(instance);
			return instance;
		});
		this.app.container.singleton("relay", () =>
			this.app.container.resolve<Relay>(Relay),
		);
	}

	async boot(): Promise<void> {
		// Force-resolve the Relay singleton so `setRelay` runs even when the app
		// never imports it explicitly.
		const relay = await this.app.container.resolve<Relay>(Relay);
		setRelay(relay);

		// Hand the relay the router, so a declaration made in a preload —
		// `relay.registerRoutes(...)`, `relay.hub(...)` — becomes a mounted route
		// AT THAT MOMENT.
		//
		// The routes used to be built here in `ready()`, which runs after the
		// socket is already listening: a request arriving in that window got a
		// 404 from a route the application had asked for. Mounting on
		// declaration closes the window, and it is what upstream's Transmit
		// does — `transmit.registerRoutes()` builds the routes itself.
		//
		// Resolve the host router from the container, where Ream registers it as
		// `'router'` (Ignitor), instead of importing
		// `@c9up/ream/services/router` — which keeps relay runtime-agnostic. A
		// non-Ream host never registers `'router'`: broadcast / authorize still
		// work through the singleton and the user wires their own SSE routes.
		if (!this.app.container.has("router")) return;
		const router = await this.app.container.resolve<ReamRouter>("router");
		relay.useMounter({
			mountEndpoints: (target) => registerRelayRoutes(router, target),
			mountHub: (target, mounted) =>
				registerMountedHub(router, target, mounted),
		});
	}

	async start(): Promise<void> {}

	async ready(): Promise<void> {
		// Nothing is mounted here any more. What IS worth saying is that an
		// application which mounted a hub but never asked for the SSE endpoints
		// has a hub no browser can reach: the client opens `/__relay/events`
		// first, and that route only exists once `registerRoutes()` is called.
		if (!this.app.container.has("router")) return;
		const relay = await this.app.container.resolve<Relay>(Relay);
		if (relay.mountedHubs().length > 0 && !relay.hasRegisteredRoutes()) {
			console.warn(
				"[relay] A hub is mounted but relay.registerRoutes() was never called, so /__relay/events does not exist and no client can connect. Call it from the preload where the hub is declared.",
			);
		}
	}

	async shutdown(): Promise<void> {
		// Graceful shutdown: release the Relay's bus subscription/connection so
		// a multi-instance deploy doesn't leak transport handles on SIGTERM.
		// No-op for single-instance relays (no transport configured).
		const relay = await this.app.container.resolve<Relay>(Relay);
		await relay.shutdown();
		// And release the module-level singleton, while it is still ours: a
		// stopped application left a shut-down Relay reachable through
		// `services/main`, so a broadcast written anywhere in the process went
		// to a transport that had already let go of its subscription.
		if (getRelay() === relay) clearRelay();
	}
}

interface ReamRequest {
	header(name: string): string | undefined;
	body(): Promise<unknown> | unknown;
	qs?(): Record<string, unknown>;
	// Declared because the hub routes read them, and ream's Request has both.
	// Leaving them out is what made this context fail to satisfy HubHttpContext,
	// and the caller reach for a double cast rather than describe the object it
	// actually had.
	url(includeQs?: boolean): string;
	raw(): string;
}
interface ReamResponse {
	status(code: number): ReamResponse;
	header(name: string, value: string): ReamResponse;
	json(data: unknown): void;
	noContent(): void;
	sse(): Promise<{
		id: string;
		isOpen(): boolean;
		send(event: string, data: unknown, eventId?: string): Promise<boolean>;
		onClose(cb: () => void): void;
		end(): Promise<void>;
	}>;
}
interface ReamHttpContext {
	request: ReamRequest;
	response: ReamResponse;
	auth?: {
		isAuthenticated: boolean;
		user?: { id: string; [key: string]: unknown };
	};
}
interface ReamRouter {
	get(
		path: string,
		handler: (ctx: ReamHttpContext) => Promise<void> | void,
	): RelayRouteBuilder;
	post(
		path: string,
		handler: (ctx: ReamHttpContext) => Promise<void> | void,
	): RelayRouteBuilder;
}

/**
 * Mount every hub recorded with `relay.hub(path, hub)`.
 *
 * Each gets the three routes SignalR's SSE transport needs, and the same route
 * customizer the relay's own routes get — a hub is as much in need of `auth`
 * middleware as the event stream is.
 */
function registerMountedHub(
	router: ReamRouter,
	relay: Relay,
	mounted: { path: string; hub: unknown; adapter?: unknown },
): void {
	{
		// `relay.hub()` takes its hub structurally — the relay records it and
		// never calls into it — so the concrete types only become knowable here,
		// where they are used. Checked rather than asserted: a wrong object
		// would otherwise fail on the first frame, three layers away from the
		// `relay.hub(...)` line that passed it.
		const hub = mounted.hub;
		if (typeof hub !== "object" || hub === null || !isHub(hub)) {
			throw new TypeError(
				`The object mounted at "${mounted.path}" is not a Hub — it must extend Hub from @c9up/relay.`,
			);
		}
		const adapter = mounted.adapter;
		if (
			adapter !== undefined &&
			(typeof adapter !== "object" || adapter === null || !isAdapter(adapter))
		) {
			throw new TypeError(
				`The adapter given for the hub at "${mounted.path}" is not a SignalRAdapter.`,
			);
		}
		const routes = registerHubRoutes(router, {
			path: mounted.path,
			hub,
			adapter: adapter ?? new SignalRAdapter(hub),
		});
		for (const route of [routes.negotiate, routes.stream, routes.send]) {
			relay.applyRouteCustomization(route);
		}
	}
}

function isHub(value: object): value is Hub {
	return (
		value instanceof Hub ||
		(typeof Reflect.get(value, "dispatch") === "function" &&
			typeof Reflect.get(value, "registerClient") === "function" &&
			typeof Reflect.get(value, "authFor") === "function")
	);
}

function isAdapter(value: object): value is SignalRAdapter {
	return (
		value instanceof SignalRAdapter ||
		(typeof Reflect.get(value, "handleFrame") === "function" &&
			typeof Reflect.get(value, "resolveToken") === "function")
	);
}

function registerRelayRoutes(router: ReamRouter, relay: Relay): void {
	const events = router.get("/__relay/events", async (ctx) => {
		// The query `uid` is only a HINT now — the relay derives the
		// canonical uid from `ctx.auth` (or generates one for anonymous
		// clients). Forwarding the hint lets us 403 when an authenticated
		// client tries to claim someone else's id; absent or matching hint
		// proceeds normally. The hint is NEVER trusted as the connection
		// identity — see Relay.connect() docs.
		const uidHint = readQueryParam(ctx.request, "uid");

		// Pre-flight uid-hint check BEFORE upgrading to SSE.
		//
		// A hint that claims someone else is a pure auth-vs-claim
		// comparison, so it is answered with a real 403 rather than a 200
		// stream carrying an error frame. That distinction is what a client
		// can act on: an EventSource sees a successful connection either
		// way, and only the status code tells a fetch caller that the
		// request was refused.
		//
		// The `capped` case cannot be answered this early — the slot count
		// is instance state that `relay.connect` owns — so it is handled
		// below on the open stream.
		const authUserId = ctx.auth?.isAuthenticated
			? ctx.auth.user?.id
			: undefined;
		if (
			uidHint !== undefined &&
			authUserId !== undefined &&
			uidHint !== authUserId
		) {
			ctx.response.status(403).json({
				error: {
					code: "E_UID_HIJACK",
					message: "uid hint does not match authenticated user",
				},
			});
			return;
		}

		const sse = await ctx.response.sse();
		const outcome = relay.connect(uidHint, sse, { auth: ctx.auth });
		if (outcome.outcome === "capped") {
			// Cap reached. The SSE writer is already open — send an error
			// frame and close cleanly so the client sees a structured
			// shutdown instead of a half-open connection.
			//
			// The frame does arrive: the stream registry keeps a closed
			// entry until its receiver has been taken, so a send followed
			// immediately by an end no longer loses the body.
			await sse.send("error", { code: "E_MAX_CLIENTS" });
			await sse.end();
		} else if (outcome.outcome === "forbidden") {
			// The pre-flight above catches the hint-mismatch case before
			// we upgrade to SSE, so this branch is now effectively dead —
			// kept defensively in case `relay.connect` grows additional
			// `forbidden` predicates that don't depend on the uid hint
			// alone.
			await sse.send("error", {
				code: "E_UID_HIJACK",
				message: outcome.reason,
			});
			await sse.end();
		}
		// outcome === 'ok' → the canonical uid is already shipped to the
		// client via the `connected` SSE frame inside Relay.connect.
	});
	relay.applyRouteCustomization(events);

	const subscribe = router.post("/__relay/subscribe", async (ctx) => {
		const body = await ctx.request.body();
		const result = await relay.subscribe(
			field(body, "uid"),
			field(body, "channel"),
			{
				auth: ctx.auth,
			},
		);
		if (!result.ok) {
			ctx.response.status(result.status).json({
				error: { code: result.code, message: result.code },
			});
			return;
		}
		ctx.response.noContent();
	});
	relay.applyRouteCustomization(subscribe);

	const unsubscribe = router.post("/__relay/unsubscribe", async (ctx) => {
		const body = await ctx.request.body();
		const r = relay.unsubscribe(field(body, "uid"), field(body, "channel"), {
			auth: ctx.auth,
		});
		if (r === "forbidden") {
			ctx.response.status(403).json({
				error: { code: "E_NOT_OWNER", message: "E_NOT_OWNER" },
			});
			return;
		}
		ctx.response.noContent();
	});
	relay.applyRouteCustomization(unsubscribe);
}

/**
 * One field off a parsed request body, whatever the body turned out to be.
 *
 * The body is `unknown` because that is what a POST carries. Describing it as
 * `{ uid?: string }` did not make it one — the relay took the values on that
 * word, and a channel that arrived as an object reached the pattern matcher and
 * threw out of the route. It hands the value over as `unknown` now, and the
 * relay is the one that decides whether it can use it.
 */
function field(body: unknown, name: string): unknown {
	if (typeof body !== "object" || body === null) return undefined;
	return Reflect.get(body, name);
}

function readQueryParam(
	request: ReamRequest,
	name: string,
): string | undefined {
	const qs = request.qs?.();
	if (!qs) return undefined;
	const v = qs[name];
	return typeof v === "string" ? v : undefined;
}

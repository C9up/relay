import { Relay, type RelayConfig, type RelayRouteBuilder } from "./Relay.js";
import { setRelay } from "./services/main.js";

interface RelayContainer {
	singleton(token: unknown, factory: () => unknown): void;
	resolve<T = unknown>(token: unknown): T;
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
		// Force-resolve the Relay singleton so `setRelay` runs even
		// when the app never imports it explicitly. Apps can then call
		// `relay.registerRoutes(...)` from a preload BEFORE the routes
		// are actually registered (that happens in `start()` below).
		const relay = this.app.container.resolve<Relay>(Relay);
		setRelay(relay);
	}

	async start(): Promise<void> {
		// Routes are registered in `start()` — AFTER preloads have run.
		// Apps' `relay.registerRoutes(customizer)` calls (which typically
		// live in start/services.ts) are guaranteed to have been
		// recorded by the time the customizer is applied to each route.
		//
		// Resolve the host router from the container, where Ream registers it as
		// `'router'` (Ignitor) — instead of importing `@c9up/ream/services/router`
		// — which keeps relay runtime-agnostic. A non-Ream host never registers
		// `'router'`: broadcast / authorize still work via the singleton and the
		// user wires their own SSE routes. Past the guard Ream IS present, so
		// route-registration failures (duplicate route, router throwing, a bad
		// customizer) surface instead of being hidden as "host is not Ream".
		if (!this.app.container.has("router")) return;
		const router = this.app.container.resolve<ReamRouter>("router");
		const relay = this.app.container.resolve<Relay>(Relay);
		registerRelayRoutes(router, relay);
	}

	async ready(): Promise<void> {}

	async shutdown(): Promise<void> {
		// Graceful shutdown: release the Relay's bus subscription/connection so
		// a multi-instance deploy doesn't leak transport handles on SIGTERM.
		// No-op for single-instance relays (no transport configured).
		const relay = this.app.container.resolve<Relay>(Relay);
		await relay.shutdown();
	}
}

interface ReamRequest {
	header(name: string): string | undefined;
	body(): Promise<unknown> | unknown;
	qs?(): Record<string, unknown>;
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
		// Previously: we always upgraded to SSE first, then `relay.connect`
		// would return `forbidden`, the handler sent an `error` frame and
		// called `sse.end()` — which removes the stream entry from the
		// Rust registry. The response builder then called `take_receiver`
		// AFTER removal, returning None → 500 E_STREAM_UNKNOWN. The TS
		// handler's "open + close" in the same tick races the Rust response
		// build that hasn't picked up the receiver yet.
		//
		// The hint-mismatch case is a pure auth-vs-claim comparison and
		// can be answered with a buffered 403 response that never opens a
		// stream. The `capped` case still needs an SSE upgrade (the slot
		// count is genuine instance state), but it's rarely hit and is
		// noted as residual TODO if/when an app actually trips it.
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
			// NOTE: this hits the same close/take_receiver race as the
			// hint-mismatch case used to. In practice "capped" only fires
			// when maxClients is reached, so the test suite doesn't
			// exercise it. The proper fix is on the Rust side
			// (StreamRegistry::close should drop the sender but leave the
			// entry until take_receiver consumes the receiver). Filed as
			// follow-up; until then a maxed-out relay logs the error.
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
		const body = (await ctx.request.body()) as
			| { uid?: string; channel?: string }
			| undefined;
		const result = await relay.subscribe(body?.uid, body?.channel, {
			auth: ctx.auth,
		});
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
		const body = (await ctx.request.body()) as
			| { uid?: string; channel?: string }
			| undefined;
		const r = relay.unsubscribe(body?.uid, body?.channel, {
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

function readQueryParam(
	request: ReamRequest,
	name: string,
): string | undefined {
	const qs = request.qs?.();
	if (!qs) return undefined;
	const v = qs[name];
	return typeof v === "string" ? v : undefined;
}

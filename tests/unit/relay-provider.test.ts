import { describe, expect, it } from "vitest";
import { Relay, type RelayConfig } from "../../src/Relay.js";
import RelayProvider, {
	type RelayAppContext,
} from "../../src/RelayProvider.js";

function makeApp(relayConfig?: RelayConfig): {
	app: RelayAppContext;
	bindings: Map<unknown, () => unknown>;
} {
	const bindings = new Map<unknown, () => unknown>();
	const cache = new Map<unknown, unknown>();
	const app: RelayAppContext = {
		container: {
			singleton(token, factory) {
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
			has(token: unknown): boolean {
				return cache.has(token) || bindings.has(token);
			},
		},
		config: {
			get<T = unknown>(key: string): T | undefined {
				if (key === "relay" && relayConfig) return relayConfig as T;
				return undefined;
			},
		},
	};
	return { app, bindings };
}

describe("relay > RelayProvider", () => {
	it("registers Relay class + 'relay' string token resolving to the same singleton", async () => {
		const { app } = makeApp();
		new RelayProvider(app).register();

		const byClass = await app.container.resolve(Relay);
		const byToken = await app.container.resolve("relay");
		expect(byClass).toBeInstanceOf(Relay);
		expect(byToken).toBe(byClass);
	});

	it("instantiates Relay successfully with a user-provided 'relay' config", async () => {
		const { app } = makeApp({ allowUnauthorizedChannels: true });
		new RelayProvider(app).register();
		expect(await app.container.resolve(Relay)).toBeInstanceOf(Relay);
	});

	it("instantiates Relay with defaults when no 'relay' config is set", async () => {
		const { app } = makeApp();
		new RelayProvider(app).register();
		expect(await app.container.resolve(Relay)).toBeInstanceOf(Relay);
	});
});

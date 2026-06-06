import type { AppContext } from "@c9up/ream";
import { describe, expect, it } from "vitest";
import { Relay, type RelayConfig } from "../../src/Relay.js";
import RelayProvider from "../../src/RelayProvider.js";

function makeApp(relayConfig?: RelayConfig): {
	app: AppContext;
	bindings: Map<unknown, () => unknown>;
} {
	const bindings = new Map<unknown, () => unknown>();
	const cache = new Map<unknown, unknown>();
	const app: AppContext = {
		container: {
			singleton(token, factory) {
				bindings.set(token, factory);
			},
			resolve<T = unknown>(token: unknown): T {
				if (cache.has(token)) return cache.get(token) as T;
				const factory = bindings.get(token);
				if (!factory) throw new Error("not registered");
				const value = factory();
				cache.set(token, value);
				return value as T;
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
	it("registers Relay class + 'relay' string token resolving to the same singleton", () => {
		const { app } = makeApp();
		new RelayProvider(app).register();

		const byClass = app.container.resolve(Relay);
		const byToken = app.container.resolve("relay");
		expect(byClass).toBeInstanceOf(Relay);
		expect(byToken).toBe(byClass);
	});

	it("instantiates Relay successfully with a user-provided 'relay' config", () => {
		const { app } = makeApp({ allowUnauthorizedChannels: true });
		new RelayProvider(app).register();
		expect(app.container.resolve(Relay)).toBeInstanceOf(Relay);
	});

	it("instantiates Relay with defaults when no 'relay' config is set", () => {
		const { app } = makeApp();
		new RelayProvider(app).register();
		expect(app.container.resolve(Relay)).toBeInstanceOf(Relay);
	});
});

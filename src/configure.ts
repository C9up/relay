/**
 * `ream configure @c9up/relay` — wire realtime broadcasting in one command.
 *
 * The provider alone is not enough: it reads `config/relay.ts`, and a package
 * registered without one falls back to a default that is rarely the one an
 * application wants. Writing both together is what makes `ream add` mean
 * installed AND working.
 */

interface Codemods {
	addProvider(importPath: string): Promise<void>;
	writeFile(
		filePath: string,
		content: string,
		options?: { force?: boolean },
	): Promise<void>;
}

export async function configure(codemods: Codemods): Promise<void> {
	await codemods.addProvider("@c9up/relay/provider");
	await codemods.writeFile(
		"config/relay.ts",
		`import { defineConfig, transports } from '@c9up/relay'

export default defineConfig({
  // Let a client subscribe to a channel no authorizer covers. Leave false:
  // an authorizer is what decides who may listen.
  allowUnauthorizedChannels: false,

  // Milliseconds between keep-alive frames, or false for none. A stream that
  // carries no traffic is one a proxy closes: nginx gives an idle upstream
  // sixty seconds by default.
  pingInterval: 30_000,

  // Without a transport, a broadcast reaches the SSE clients of the instance
  // that made it and no further. Uncomment as soon as there are two.
  // transport: { driver: transports.redis({ connection: 'main' }) },
})`,
	);
}

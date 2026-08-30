# @c9up/relay

> Realtime client transport for the Ream framework — SSE + WebSocket Hub + SignalR.
>
> **Transport status:** only SSE has a working server transport today. `Hub` and
> `SignalRAdapter` implement their protocols in full, but Ream's Rust HTTP server
> exposes no WebSocket upgrade point, so the caller must supply the socket itself.

Part of **[Ream](https://github.com/C9up/ream)** — a Rust-powered, AdonisJS-compatible Node.js framework. Independent, publishable package.

## Installation

```bash
pnpm add @c9up/relay
```

`ream add @c9up/relay` installs it, registers the provider and writes
`config/relay.ts`. To do it by hand:

```ts
// reamrc.ts
providers: [
  () => import('@c9up/relay/provider'),
]
```

## Usage

Running more than one instance? A broadcast only reaches the SSE clients of the
instance that made it, unless you give relay a bus:

```ts
// config/relay.ts
import { defineConfig, transports } from '@c9up/relay'

export default defineConfig({
  transport: transports.redis({ connection: 'main' }),
})
```

## Entry points

- `@c9up/relay` — main API
- `@c9up/relay/provider` — Ream IoC provider
- `@c9up/relay/services/main` — container service accessor
- `@c9up/relay/testing` — test fakes & helpers

## License

MIT

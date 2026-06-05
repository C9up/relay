# @c9up/relay

> Realtime client transport for the Ream framework — SSE + WebSocket Hub + SignalR.

Part of **[Ream](https://github.com/C9up/ream)** — a Rust-powered, AdonisJS-compatible Node.js framework. Independent, publishable package.

## Installation

```bash
pnpm add @c9up/relay
ream configure @c9up/relay
```

## Usage

Register the provider in your app, then configure it under `config/relay.ts`:

```ts
// reamrc.ts
providers: [
  () => import('@c9up/relay/provider'),
]
```

## Entry points

- `@c9up/relay` — main API
- `@c9up/relay/provider` — Ream IoC provider
- `@c9up/relay/services/main` — container service accessor
- `@c9up/relay/testing` — test fakes & helpers

## License

MIT

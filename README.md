# @c9up/relay

> Realtime client transport for the Ream framework — SSE event streams and
> SignalR hubs.

### Which transports run

Both of them run over Server-Sent Events, which is what Ream's HTTP server
serves and a first-class SignalR transport besides:

- the **event stream** — `authorize` / `broadcast` / `on`, on the three routes
  the provider registers;
- **hubs** — `relay.hub('/hubs/chat', new ChatHub())` registers SignalR's
  negotiate, downstream and upstream routes, and a stock `@microsoft/signalr`
  client configured with `HttpTransportType.ServerSentEvents` speaks to it
  unchanged.

**WebSockets are the transport that is missing.** Ream's Rust HTTP server
exposes no upgrade point, so a hub reached over `HttpTransportType.WebSockets`
needs a socket you supply and drive yourself — `SignalRAdapter` is written for
that, parsing an inbound frame and handing back the frames to send without
owning a socket of its own.

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
  transport: { driver: transports.redis({ connection: 'main' }) },
})
```

`transport` takes `{ driver, channel? }`, and `null` for no bus at all. The
channel is what every instance publishes on and listens to — leave it out and
it is `relay::broadcast`. Passing the driver on its own
(`transport: transports.redis(…)`, with `transportChannel` beside it) is the
form this config had before and still resolves.

### Keeping a stream alive

An SSE connection carrying no traffic is indistinguishable from a hung one to
everything between the client and the process: nginx closes an idle upstream at
sixty seconds by default, and most load balancers and mobile networks are less
patient still. The client reconnects, so the symptom is not silence but a
connection that drops and reopens forever, losing whatever was published in
between. `pingInterval` is what stops that — off by default, in milliseconds:

```ts
export default defineConfig({
  pingInterval: 30_000,
})
```

The frames go out on the `$$relay/ping` event, which no application channel can
collide with. Browser clients using `EventSource` never see them unless they
add a listener for that name.

### Channels nobody authorized

`allowUnauthorizedChannels` is `false`, so a channel with no `authorize(...)`
covering it refuses subscribers. Set it to `true` to serve them instead.

## Entry points

- `@c9up/relay` — main API
- `@c9up/relay/provider` — Ream IoC provider
- `@c9up/relay/services/main` — container service accessor
- `@c9up/relay/testing` — test fakes & helpers

## License

MIT

# BroadwayCloudPubSub

[![CI](https://github.com/dashbitco/broadway_cloud_pub_sub/actions/workflows/ci.yml/badge.svg)](https://github.com/dashbitco/broadway_cloud_pub_sub/actions/workflows/ci.yml)

A Google Cloud Pub/Sub connector for [Broadway](https://github.com/dashbitco/broadway).

Documentation can be found at [https://hexdocs.pm/broadway_cloud_pub_sub](https://hexdocs.pm/broadway_cloud_pub_sub).

## What's in the box

* `BroadwayCloudPubSub.Producer`: Broadway producer using the gRPC
  [StreamingPull][gcp-streamingpull] API. Messages are pushed by the server over
  a persistent bidirectional stream, giving low latency and high throughput with
  automatic lease extension and server-side flow control. **This is the
  recommended producer**, in line with Google's own [guidance][gcp-streamingpull]
  that StreamingPull is what their first-party client libraries use "where
  possible".
* `BroadwayCloudPubSub.Pull.Producer`: Broadway producer using the unary HTTP
  [Pull][gcp-pull-api] API. Retained for environments where gRPC is unavailable
  or undesired, and for the cases Google lists as Pull-only: when you need
  strict control over the number of messages pulled per request, tight control
  over client memory and CPU, or when your subscriber acts as a proxy to
  another pull-oriented system.
* `BroadwayCloudPubSub.Streaming.Client`: Behaviour for custom gRPC client implementations.
* `BroadwayCloudPubSub.Pull.Client`: Behaviour for custom HTTP pull client implementations.

[gcp-streamingpull]: https://cloud.google.com/pubsub/docs/pull#streamingpull_api
[gcp-pull-api]: https://cloud.google.com/pubsub/docs/pull#pull_api

## Installation

Add `:broadway_cloud_pub_sub` to your dependencies, along with an HTTP/2 adapter
for `:grpc`:

```elixir
def deps do
  [
    {:broadway_cloud_pub_sub, "~> 2.0"},
    {:goth, "~> 1.3"},
    {:grpc, "~> 1.0"},
    {:protobuf, "~> 0.12"},
    # Pick one HTTP/2 adapter:
    {:gun, "~> 2.0"},
    # or
    # {:mint, "~> 1.5"},
    # {:castore, "~> 1.0"}
  ]
end
```

> The [goth](https://hexdocs.pm/goth) package handles Google Authentication and
> is required for the default token generator.
>
> The `grpc` and `protobuf` packages are required by
> `BroadwayCloudPubSub.Producer`. You must pick one HTTP/2 adapter for the gRPC
> connection and add it to your `mix.exs`: either `:gun`, or `:mint` together
> with `:castore`.
>
> If you only use `BroadwayCloudPubSub.Pull.Producer` you may omit `:grpc`,
> `:protobuf`, and the adapter packages.

## Usage

```elixir
Broadway.start_link(MyBroadway,
  name: MyBroadway,
  producer: [
    module: {BroadwayCloudPubSub.Producer,
      goth: MyGoth,
      subscription: "projects/my-project/subscriptions/my-subscription",
      max_outstanding_messages: 1000
    }
  ],
  processors: [default: [concurrency: 10]]
)
```

See `BroadwayCloudPubSub.Producer` for the full option reference, including flow
control, reconnection backoff, graceful shutdown, and telemetry.

### HTTP/2 adapter

The producer supports two adapters. Both are optional dependencies of `:grpc`,
so you select one by adding it to your application's `mix.exs` (see
[Installation](#installation)).

- `:gun` (default): [Gun](https://github.com/ninenines/gun) HTTP/2 client.
  Add `{:gun, "~> 2.0"}` to your deps.
- `:mint`: [Mint](https://github.com/elixir-mint/mint) HTTP/2 client.
  Add `{:mint, "~> 1.5"}` and `{:castore, "~> 1.0"}` to your deps.

Then select the adapter in your producer config:

```elixir
{BroadwayCloudPubSub.Producer,
 goth: MyGoth,
 subscription: "projects/my-project/subscriptions/my-subscription",
 adapter: :mint}
```

### Using the HTTP pull producer

If gRPC is not available in your environment or you prefer to use the HTTP pull method, use `BroadwayCloudPubSub.Pull.Producer`:

```elixir
Broadway.start_link(MyBroadway,
  name: MyBroadway,
  producer: [
    module: {BroadwayCloudPubSub.Pull.Producer,
      goth: MyGoth,
      subscription: "projects/my-project/subscriptions/my-subscription"
    }
  ],
  processors: [default: [concurrency: 10]]
)
```

### Upgrading from 1.x

See the [2.0 upgrade guide](docs/upgrade_to_2.0.md) for the full list of breaking
changes and step-by-step migration instructions from pull producer to gRPC streaming producer.

## License

Copyright 2019 Michael Crumm \
Copyright 2020 Dashbit

  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.

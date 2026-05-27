# BroadwayCloudPubSub

[![CI](https://github.com/dashbitco/broadway_cloud_pub_sub/actions/workflows/ci.yml/badge.svg)](https://github.com/dashbitco/broadway_cloud_pub_sub/actions/workflows/ci.yml)

A Google Cloud Pub/Sub connector for [Broadway](https://github.com/dashbitco/broadway).

Documentation can be found at [https://hexdocs.pm/broadway_cloud_pub_sub](https://hexdocs.pm/broadway_cloud_pub_sub).

This project provides:

* `BroadwayCloudPubSub.Producer` - A GenStage producer that continuously receives messages from a Pub/Sub subscription and acknowledges them after being successfully processed.
* `BroadwayCloudPubSub.Streaming.Producer` - A GenStage producer that uses the gRPC StreamingPull API for low-latency, push-based message delivery.
* `BroadwayCloudPubSub.Client` - A generic behaviour to implement Pub/Sub clients.
* `BroadwayCloudPubSub.PullClient` - Default REST client used by `BroadwayCloudPubSub.Producer`.

## Installation

Add `:broadway_cloud_pub_sub` to the list of dependencies in `mix.exs`:

```elixir
def deps do
  [
    {:broadway_cloud_pub_sub, "~> 1.0"},
    {:goth, "~> 1.3"}
  ]
end
```

> Note the [goth](https://hexdocs.pm/goth) package, which handles Google Authentication, is required for the default token generator.

If you are using `BroadwayCloudPubSub.Streaming.Producer`, also add the gRPC dependencies:

```elixir
def deps do
  [
    {:broadway_cloud_pub_sub, "~> 0.10.0"},
    {:goth, "~> 1.3"},
    {:grpc, "~> 1.0"},
    {:protobuf, "~> 0.12"}
  ]
end
```

## Usage

Configure Broadway with one or more producers using `BroadwayCloudPubSub.Producer`:

```elixir
Broadway.start_link(MyBroadway,
  name: MyBroadway,
  producer: [
    module: {BroadwayCloudPubSub.Producer,
      goth: MyGoth,
      subscription: "projects/my-project/subscriptions/my-subscription"
    }
  ]
)
```

## Streaming Usage

For lower latency and higher throughput workloads, use `BroadwayCloudPubSub.Streaming.Producer`.
It opens a persistent bidirectional gRPC stream to Pub/Sub and receives messages as the server
pushes them, rather than polling via HTTP.

```elixir
Broadway.start_link(MyBroadway,
  name: MyBroadway,
  producer: [
    module: {BroadwayCloudPubSub.Streaming.Producer,
      goth: MyGoth,
      subscription: "projects/my-project/subscriptions/my-subscription",
      max_outstanding_messages: 1000
    }
  ]
)
```

### gRPC adapter

The streaming producer supports two HTTP/2 adapters, both provided by the `grpc` dependency:

- `:gun` (default) — Uses the [Gun](https://github.com/ninenines/gun) HTTP/2 client. This is the
  traditional adapter and works out of the box with the standard `grpc` dependency.
- `:mint` — Uses the [Mint](https://github.com/elixir-mint/mint) HTTP/2 client. Mint may be
  preferable in environments where Gun is not available or not desired.

```elixir
Broadway.start_link(MyBroadway,
  name: MyBroadway,
  producer: [
    module: {BroadwayCloudPubSub.Streaming.Producer,
      goth: MyGoth,
      subscription: "projects/my-project/subscriptions/my-subscription",
      adapter: :mint
    }
  ]
)
```

See `BroadwayCloudPubSub.Streaming.Producer` for the full list of configuration options,
including flow control (`max_outstanding_messages`, `max_outstanding_bytes`), reconnection
backoff, and shutdown behaviour.

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

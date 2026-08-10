# pickpoint (Rust SDK)

Official Rust SDK for [Pickpoint](https://pickpoint.io) — a geolocation platform with four APIs under one key:

| API | What it does |
|-----|----------------|
| **Geocoding** | Address ↔ coordinates (forward, reverse, place lookup) |
| **Address search** | Typeahead / autocomplete for address inputs |
| **Routing** | Routes, matrices, optimized multi-stop, elevation |
| **Device tracking** | Register devices over HTTP; stream live GPS over WebSocket / gRPC |

Built for maps, delivery, logistics, and anything that needs places, routes, or live location. Data is OpenStreetMap-backed; HTTP responses are plain JSON / GeoJSON. Docs: [pickpoint.io/docs](https://pickpoint.io/docs).

**This crate** is the idiomatic Rust client for that platform:

| Module | Import | Role |
|--------|--------|------|
| root | `pickpoint` | HTTP: geocode, search, routing, devices, client-tokens |
| [`tracking`](#tracking) | `pickpoint::tracking` | Realtime tracks (WebSocket by default, gRPC supported) |
| `tracking::v2` | `pickpoint::tracking::v2` | Generated protobuf (`tracking.v2`) |

Apache-2.0. Go sibling: [`github.com/pickpoint/go-sdk`](https://github.com/pickpoint/go-sdk). JS sibling: [`@pickpoint/sdk`](https://github.com/pickpoint/pickpoint-js). Wire schema: [`pickpoint-proto`](https://github.com/pickpoint/pickpoint-proto).

```toml
[dependencies]
pickpoint = "2"
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
```

```
rust-sdk/
  src/                 # HTTP client (root modules)
  src/tracking/        # tracking session client
  src/tracking/v2/     # protobuf stubs
```

---

## Public API

One `Client`, one auth session, whole public HTTP surface:

```rust
use pickpoint::{query, Client, Config};

#[tokio::main]
async fn main() -> pickpoint::Result<()> {
    let pp = Client::new(Config::with_api_key(
        std::env::var("PICKPOINT_API_KEY").expect("PICKPOINT_API_KEY"),
    ))
    .await?;

    let places = pp.forward(query([("q", "Berlin"), ("limit", "5")])).await?;
    println!("{places:?}");

    let _rev = pp
        .reverse(query([("lat", "52.52"), ("lon", "13.405")]))
        .await?;
    let _search = pp.search(query([("q", "Alexanderplatz")])).await?;
    let _route = pp
        .route(serde_json::json!({
            "locations": [
                {"lat": 52.52, "lon": 13.40},
                {"lat": 52.53, "lon": 13.42},
            ],
            "costing": "auto",
        }))
        .await?;

    let list = pp
        .devices()
        .list(pickpoint::DeviceListQuery {
            take: Some(25),
            ..Default::default()
        })
        .await?;
    let _ = list;
    Ok(())
}
```

### API map

| Method | HTTP | Notes |
|--------|------|--------|
| `forward` / `geocoding().forward` | `GET /v2/geocode/forward` | Nominatim-style; returns `Vec<Value>` |
| `reverse` / `geocoding().reverse` | `GET /v2/geocode/reverse` | `Option<Value>` |
| `lookup` / `geocoding().lookup` | `GET /v2/address/lookup` | e.g. `osm_ids` |
| `forward_batch` / `reverse_batch` / `lookup_batch` | same | Geocoding **only**; conveyor ≤20 in flight |
| `search` / `address().search` | `GET /v2/address/search` | Photon autocomplete |
| `route` / `optimized_route` / `matrix` / `locate` / `elevation` | `POST /v2/route…` | Valhalla JSON body → `Value` |
| `devices().list` / `get` / `create` / `update` / `delete` | `/v2/devices` | Typed structs |
| `devices().command` | `POST …/command` | Payload `&[u8]` (SDK base64-encodes) |
| `mint_client_tokens` | `POST /v2/client-tokens` | Package helper; needs secret `api_key` |

Query params for geocode/address are `Query` (`HashMap<String, String>`) — use [`query`] helper or pass whatever the public API accepts (`q`, `lat`, `lon`, `limit`, `accept-language`, …).

### Auth

Provide **exactly one** of:

| Field | Header | Use |
|-------|--------|-----|
| `api_key` | `x-api-key` | Backends, workers, CLIs |
| `client_auth` | `Authorization: Bearer` | Short-lived pair; auto-refresh |
| `access_token` | `Authorization: Bearer` | Static token, no refresh |

Keep the secret API key on the server. For client apps mint **client-tokens** and pass `client_auth`.

```rust
use pickpoint::{mint_client_tokens, Client, ClientAuth, Config};

let pair = mint_client_tokens(
    &Config::with_api_key(std::env::var("PICKPOINT_API_KEY").unwrap()),
    &["geocoding".into(), "address".into(), "routing".into(), "devices".into()],
    Some(600),
)
.await?;

let pp = Client::new(Config::with_client_auth(ClientAuth {
    access_token: pair.access_token,
    refresh_token: pair.refresh_token,
    expires_at: pair.expires_at,
}))
.await?;
```

Refresh behavior (same as Go/JS):

1. Proactive refresh at **~50% of access TTL** (single-flight).
2. On HTTP **401**, one refresh + retry.
3. If refresh fails → `err.is_auth()`.

### Config

```rust
use std::time::Duration;
use pickpoint::Config;

let cfg = Config::with_api_key("…")
    .base_url("https://api.pickpoint.io") // default
    .timeout(Duration::from_secs(30))
    .max_retries(3)
    .retry_base(Duration::from_secs(1))
    .concurrency(20);
```

| Constant | Value |
|----------|--------|
| `DEFAULT_BASE_URL` | `https://api.pickpoint.io` |
| `DEFAULT_TIMEOUT` | 30s |
| `DEFAULT_MAX_RETRIES` | 3 |
| `DEFAULT_RETRY_BASE` | 1s (`MIN_RETRY_BASE` = 200ms) |
| `MAX_CONCURRENCY` | 20 |

### Errors

```rust
match pp.devices().get("uid").await {
    Err(e) if e.is_not_found() => { /* 404 */ }
    Err(e) if e.is_auth() => { /* 401 / 402 / 403 */ }
    Err(e) if e.is_conflict() => { /* 409 */ }
    Err(e) => {
        if let pickpoint::Error::Api(api) = e {
            eprintln!("status={} code={} body={:?}", api.status, api.code, api.body);
        }
    }
    Ok(_) => {}
}
```

---

## Tracking

Realtime publisher / listener over **binary WebSocket** (`tracking.v2.proto` subprotocol). **gRPC** via `Transport::Grpc`.

```rust
use pickpoint::tracking::{self, DeviceAuth};
use pickpoint::tracking::v2::LatLng;

#[tokio::main]
async fn main() -> Result<(), tracking::Error> {
    let client = tracking::connect(tracking::Config {
        endpoint: "wss://tracking.pickpoint.io".into(), // local: "ws://127.0.0.1:3100"
        device: Some(DeviceAuth {
            client_id: device_uid,
            client_secret: device_secret,
        }),
        ..Default::default()
    })
    .await?;

    let track_uid = client
        .start_track(
            Some(LatLng {
                latitude: 55.75,
                longitude: 37.61,
                ..Default::default()
            }),
            vec![],
        )
        .await?;
    let _ = track_uid;

    let (seq, ok) = client
        .publish(LatLng {
            latitude: 55.76,
            longitude: 37.62,
            ..Default::default()
        })
        .await;
    let _ = (seq, ok); // managed client_seq; ok=false if rate-limited locally

    client.stop_track(None).await?;
    client.close().await?;
    Ok(())
}
```

### Auth modes

| Config | Role |
|--------|------|
| `device: Some(DeviceAuth { … })` | Publisher (device) |
| `listener: Some(ListenerAuth { … })` | Dashboard / subscriber JWT |

Exactly one of `device` / `listener` is required.

### Main methods

| Method | Purpose |
|--------|---------|
| `start_track` / `start_track_meta` | Open a track; returns `track_uid` |
| `publish` | Point on active track (managed `client_seq`); capped at **50 Hz** |
| `resume` | Manual resume; auto-reconnect also resumes |
| `stop_track` | End track |
| `send_event` | Opaque event ≤4 KiB; capped at **1 Hz** |
| `subscribe` | Listener: subscribe to a device UID |
| `recv` | Next `ServerMsg` |
| `recv_command` / `ack_command` | Inbound commands |
| `close` | Tear down session |

Limits enforced client-side: `MAX_PUBLISH_HZ = 50`, `MAX_EVENT_BYTES = 4 KiB`, `MAX_EVENT_HZ = 1`.

---

## Develop

```bash
cargo test
cargo clippy --all-targets -- -D warnings
cargo fmt
```

Live geocode batch e2e (skipped unless the key is set; 1000 requests each):

```bash
PICKPOINT_API_KEY=… cargo test --test e2e_geocode_batch -- --nocapture
# optional: PICKPOINT_BASE_URL=https://api.pickpoint.io  (default: https://beta-api.pickpoint.io)
```

### CI & release

- **PR** → `.github/workflows/ci.yml` (`fmt`, `clippy`, `test`)
- **Push to `main`** (untagged HEAD) → bump **patch** in `Cargo.toml`, tag `vX.Y.Z`, `cargo publish` (OIDC) + GitHub Release in the same job  
  (tag push via `GITHUB_TOKEN` does not start new workflows — publish cannot wait on the tag event)
- **Manual tag `v*`** (pushed by a human) → publish + GitHub Release

Minor/major: bump `version` in `Cargo.toml` in a PR, merge with `[skip release]` in the commit message, then:

```bash
git tag v2.1.0
git push origin v2.1.0
```

crates.io Trusted Publishing must match this workflow: repo `rust-sdk`, workflow `release.yml` (leave Environment empty).

Protobuf stubs under `src/tracking/v2` are generated from [`pickpoint-proto`](https://github.com/pickpoint/pickpoint-proto) by `build.rs` when the sibling checkout is present; otherwise committed stubs are used. Regenerate:

```bash
# with ../pickpoint-proto available
cargo build
```

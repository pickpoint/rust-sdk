# pickpoint (Rust SDK)

Official Rust SDK for [Pickpoint](https://pickpoint.io) — a geolocation platform with four APIs under one key:

| API | What it does |
|-----|----------------|
| **Geocoding** | Address ↔ coordinates (forward, reverse, place lookup) |
| **Address search** | Typeahead / autocomplete for address inputs |
| **Routing** | Routes, matrices, optimized multi-stop, elevation |
| **Device tracking** | Register devices over HTTP; stream live GPS over WebSocket |

Built for maps, delivery, logistics, and anything that needs places, routes, or live location. Data is OpenStreetMap-backed; HTTP responses are plain JSON / GeoJSON. Docs: [pickpoint.io/docs](https://pickpoint.io/docs).

**This crate** is the idiomatic Rust client for that platform:

| Module | Import | Role |
|--------|--------|------|
| root | `pickpoint` | HTTP: geocode, search, routing, devices, client-tokens |
| [`tracking`](#tracking) | `pickpoint::tracking` | Live GPS over WebSocket (`tracking.v2`) |

Apache-2.0. Go sibling: [`github.com/pickpoint/go-sdk`](https://github.com/pickpoint/go-sdk). JS sibling: [`@pickpoint/sdk`](https://github.com/pickpoint/pickpoint-js). Wire schema: [`pickpoint-proto`](https://github.com/pickpoint/pickpoint-proto).

```toml
[dependencies]
pickpoint = "2"
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
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

Live GPS is a **separate** WebSocket session: `wss://tracking.pickpoint.io/v2/ws`, subprotocol `tracking.v2`. It is not `pickpoint::Client` (that one is HTTP).

A dropped socket is not a new trip. The SDK reconnects and **Resumes** the same `track_uid`.

First `publish` starts the trip if none is live. `close` sends `TrackStop` then hangs up. Call `start_track` only to supersede (new order / `TRACK_NOT_FOUND`) or to set a route.

### Device (publisher)

```rust
use pickpoint::tracking::{self, DeviceAuth, LatLng};

#[tokio::main]
async fn main() -> Result<(), tracking::Error> {
    let session = tracking::connect(tracking::Config {
        endpoint: "wss://tracking.pickpoint.io".into(), // host; SDK appends /v2/ws
        device: Some(DeviceAuth {
            client_id: device_uid,     // from devices.create — not the HTTP API key
            client_secret: device_secret,
        }),
        ..Default::default()
    })
    .await?;

    session.publish(LatLng::new(55.75, 37.61)).await; // TrackStart if idle, then GPS
    session.close().await?; // TrackStop + hang up
    Ok(())
}
```

### Listener (dashboard)

The JWT is the **client-token** `access_token` — same one as HTTP `client_auth`. Mint it on your backend with scope `devices` (API key never goes in the dashboard).

```rust
use pickpoint::{mint_client_tokens, Config};
use pickpoint::tracking::{self, ListenerAuth, ServerEvt};

let pair = mint_client_tokens(
    &Config::with_api_key(std::env::var("PICKPOINT_API_KEY").unwrap()),
    &["devices".into()],
    Some(600),
)
.await?;

let session = tracking::connect(tracking::Config {
    endpoint: "wss://tracking.pickpoint.io".into(),
    listener: Some(ListenerAuth {
        access_token: pair.access_token,
    }),
    subscribe: vec![device_uid],
    ..Default::default()
})
.await?;

loop {
    match session.recv().await? {
        ServerEvt::LocationAdded { point, .. } => { // live fan-out; publisher never sees Loc
            println!("{} {}", point.latitude, point.longitude);
        }
        ServerEvt::Error { message, .. } => eprintln!("{message}"),
        _ => {}
    }
}
```

Wire format: [`pickpoint-proto`](https://github.com/pickpoint/pickpoint-proto).

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

- **PR to `dev`** → `.github/workflows/ci.yml` (`fmt`, `clippy`, `test`)
- **Merge `dev` → `main`** (untagged HEAD) → bump **patch** in `Cargo.toml`, tag `vX.Y.Z`, `cargo publish` (OIDC) + GitHub Release in the same job  
  (tag push via `GITHUB_TOKEN` does not start new workflows — publish cannot wait on the tag event)
- **Manual tag `v*`** (pushed by a human) → publish + GitHub Release

Minor/major: bump `version` in `Cargo.toml` in a PR, merge with `[skip release]` in the commit message, then:

```bash
git tag v2.1.0
git push origin v2.1.0
```

crates.io Trusted Publishing must match this workflow: repo `rust-sdk`, workflow `release.yml` (leave Environment empty).

## Contributing

Fork and open a PR against **`dev`**. [CONTRIBUTING.md](CONTRIBUTING.md).

mod common;

use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use axum::extract::Request;
use axum::http::header::AUTHORIZATION;
use common::{json_response, read_body, DynServer};
use pickpoint::{mint_client_tokens as mint_tokens, query, Client, ClientAuth, Config};
use serde_json::json;
use tokio::sync::Mutex;

fn expires_at_ms(from_now: Duration) -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
        + from_now.as_millis() as i64
}

#[tokio::test]
async fn client_auth_refresh_on_401() {
    let n = Arc::new(AtomicU32::new(0));
    let n2 = n.clone();
    let srv = DynServer::start(move |req: Request| {
        let n = n2.clone();
        async move {
            if req.uri().path().contains("/client-tokens/refresh") {
                return json_response(
                    200,
                    serde_json::to_vec(&json!({
                        "accessToken": "access-2",
                        "refreshToken": "refresh-2",
                        "expiresAt": expires_at_ms(Duration::from_secs(60)),
                    }))
                    .unwrap(),
                );
            }
            let auth = req
                .headers()
                .get(AUTHORIZATION)
                .and_then(|v| v.to_str().ok())
                .unwrap_or("")
                .to_string();
            let i = n.fetch_add(1, Ordering::SeqCst) + 1;
            if i == 1 {
                assert_eq!(auth, "Bearer access-1");
                return json_response(401, b"{}");
            }
            assert_eq!(auth, "Bearer access-2");
            json_response(200, br#"[{"ok":true}]"#)
        }
    })
    .await;

    let c = Client::new(Config {
        base_url: Some(srv.base_url.clone()),
        client_auth: Some(ClientAuth {
            access_token: "access-1".into(),
            refresh_token: "refresh-1".into(),
            expires_at: expires_at_ms(Duration::from_secs(60)),
        }),
        ..Default::default()
    })
    .await
    .unwrap();

    let out = c.forward(query([("q", "a")])).await.unwrap();
    assert_eq!(out.len(), 1);
}

#[tokio::test]
async fn single_flight_refresh() {
    let refreshes = Arc::new(AtomicU32::new(0));
    let refreshes2 = refreshes.clone();
    let srv = DynServer::start(move |req: Request| {
        let refreshes = refreshes2.clone();
        async move {
            if req.uri().path().contains("/refresh") {
                refreshes.fetch_add(1, Ordering::SeqCst);
                tokio::time::sleep(Duration::from_millis(40)).await;
                return json_response(
                    200,
                    serde_json::to_vec(&json!({
                        "accessToken": "access-fresh",
                        "refreshToken": "refresh-2",
                        "expiresAt": expires_at_ms(Duration::from_secs(120)),
                    }))
                    .unwrap(),
                );
            }
            let auth = req
                .headers()
                .get(AUTHORIZATION)
                .and_then(|v| v.to_str().ok())
                .unwrap_or("");
            assert_eq!(auth, "Bearer access-fresh");
            json_response(200, br#"[{"ok":true}]"#)
        }
    })
    .await;

    let c = Client::new(Config {
        base_url: Some(srv.base_url.clone()),
        client_auth: Some(ClientAuth {
            access_token: "stale".into(),
            refresh_token: "refresh-1".into(),
            expires_at: expires_at_ms(Duration::from_millis(80)),
        }),
        ..Default::default()
    })
    .await
    .unwrap();

    tokio::time::sleep(Duration::from_millis(50)).await;

    let mut handles = Vec::new();
    for _ in 0..4 {
        let c = c.clone();
        handles.push(tokio::spawn(async move {
            c.forward(query([("q", "x")])).await.unwrap();
        }));
    }
    for h in handles {
        h.await.unwrap();
    }
    assert_eq!(refreshes.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn refresh_rotation_second_client_fails() {
    let valid = Arc::new(Mutex::new("refresh-1".to_string()));
    let valid2 = valid.clone();
    let srv = DynServer::start(move |req: Request| {
        let valid = valid2.clone();
        async move {
            if req.uri().path().contains("/refresh") {
                let body = read_body(req).await;
                let v: serde_json::Value = serde_json::from_slice(&body).unwrap_or_default();
                let tok = v["refreshToken"].as_str().unwrap_or("");
                let mut guard = valid.lock().await;
                let ok = tok == guard.as_str();
                if ok {
                    *guard = "refresh-2".into();
                }
                drop(guard);
                if !ok {
                    return json_response(401, b"{}");
                }
                return json_response(
                    200,
                    serde_json::to_vec(&json!({
                        "accessToken": "a2",
                        "refreshToken": "refresh-2",
                        "expiresAt": expires_at_ms(Duration::from_secs(60)),
                    }))
                    .unwrap(),
                );
            }
            json_response(200, b"[]")
        }
    })
    .await;

    async fn mk(base: String) -> Client {
        Client::new(Config {
            base_url: Some(base),
            client_auth: Some(ClientAuth {
                access_token: "a1".into(),
                refresh_token: "refresh-1".into(),
                expires_at: expires_at_ms(Duration::from_millis(50)),
            }),
            ..Default::default()
        })
        .await
        .unwrap()
    }

    let a = mk(srv.base_url.clone()).await;
    tokio::time::sleep(Duration::from_millis(40)).await;
    a.forward(query([("q", "a")])).await.unwrap();

    let b = mk(srv.base_url.clone()).await;
    tokio::time::sleep(Duration::from_millis(40)).await;
    let err = b.forward(query([("q", "b")])).await.unwrap_err();
    assert!(err.is_auth(), "{err:?}");
}

#[tokio::test]
async fn unauthorized_retry_exactly_once() {
    let hits = Arc::new(AtomicU32::new(0));
    let hits2 = hits.clone();
    let srv = DynServer::start(move |req: Request| {
        let hits = hits2.clone();
        async move {
            if req.uri().path().contains("/refresh") {
                return json_response(
                    200,
                    serde_json::to_vec(&json!({
                        "accessToken": "a2",
                        "refreshToken": "r2",
                        "expiresAt": expires_at_ms(Duration::from_secs(60)),
                    }))
                    .unwrap(),
                );
            }
            hits.fetch_add(1, Ordering::SeqCst);
            json_response(401, b"{}")
        }
    })
    .await;

    let c = Client::new(Config {
        base_url: Some(srv.base_url.clone()),
        client_auth: Some(ClientAuth {
            access_token: "a1".into(),
            refresh_token: "r1".into(),
            expires_at: expires_at_ms(Duration::from_secs(60)),
        }),
        ..Default::default()
    })
    .await
    .unwrap();

    let err = c.forward(query([("q", "x")])).await.unwrap_err();
    assert!(err.is_auth());
    assert_eq!(hits.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn proactive_refresh_halfway_ttl() {
    let refreshed = Arc::new(AtomicBool::new(false));
    let refreshed2 = refreshed.clone();
    let srv = DynServer::start(move |req: Request| {
        let refreshed = refreshed2.clone();
        async move {
            if req.uri().path().contains("/refresh") {
                refreshed.store(true, Ordering::SeqCst);
                return json_response(
                    200,
                    serde_json::to_vec(&json!({
                        "accessToken": "a2",
                        "refreshToken": "r2",
                        "expiresAt": expires_at_ms(Duration::from_secs(60)),
                    }))
                    .unwrap(),
                );
            }
            json_response(200, b"[]")
        }
    })
    .await;

    let ttl = Duration::from_millis(200);
    let c = Client::new(Config {
        base_url: Some(srv.base_url.clone()),
        client_auth: Some(ClientAuth {
            access_token: "a1".into(),
            refresh_token: "r1".into(),
            expires_at: expires_at_ms(ttl),
        }),
        ..Default::default()
    })
    .await
    .unwrap();

    c.forward(query([("q", "early")])).await.unwrap();
    assert!(!refreshed.load(Ordering::SeqCst));
    tokio::time::sleep(ttl * 55 / 100 + Duration::from_millis(10)).await;
    c.forward(query([("q", "late")])).await.unwrap();
    assert!(refreshed.load(Ordering::SeqCst));
}

#[tokio::test]
async fn mixed_fan_out_shares_401_refresh() {
    let refreshes = Arc::new(AtomicU32::new(0));
    let refreshes2 = refreshes.clone();
    let srv = DynServer::start(move |req: Request| {
        let refreshes = refreshes2.clone();
        async move {
            if req.uri().path().contains("/refresh") {
                refreshes.fetch_add(1, Ordering::SeqCst);
                tokio::time::sleep(Duration::from_millis(20)).await;
                return json_response(
                    200,
                    serde_json::to_vec(&json!({
                        "accessToken": "a2",
                        "refreshToken": "r2",
                        "expiresAt": expires_at_ms(Duration::from_secs(60)),
                    }))
                    .unwrap(),
                );
            }
            let auth = req
                .headers()
                .get(AUTHORIZATION)
                .and_then(|v| v.to_str().ok())
                .unwrap_or("");
            if auth == "Bearer a1" {
                return json_response(401, b"{}");
            }
            let path = req.uri().path();
            if path.contains("/address/search") {
                return json_response(200, br#"{"features":[]}"#);
            }
            if path.contains("/devices") {
                return json_response(200, br#"{"data":[],"total":0}"#);
            }
            json_response(200, br#"[{"ok":true}]"#)
        }
    })
    .await;

    let c = Client::new(Config {
        base_url: Some(srv.base_url.clone()),
        client_auth: Some(ClientAuth {
            access_token: "a1".into(),
            refresh_token: "r1".into(),
            expires_at: expires_at_ms(Duration::from_secs(60)),
        }),
        ..Default::default()
    })
    .await
    .unwrap();

    let c1 = c.clone();
    let c2 = c.clone();
    let c3 = c.clone();
    let (a, b, d) = tokio::join!(
        async move { c1.forward(query([("q", "a")])).await },
        async move { c2.search(query([("q", "b")])).await },
        async move {
            c3.devices()
                .list(pickpoint::DeviceListQuery::default())
                .await
        },
    );
    a.unwrap();
    b.unwrap();
    d.unwrap();
    assert_eq!(refreshes.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn mint_tokens_ok() {
    let srv = DynServer::start(|req: Request| async move {
        assert_eq!(
            req.headers().get("x-api-key").and_then(|v| v.to_str().ok()),
            Some("secret")
        );
        let body = read_body(req).await;
        assert!(std::str::from_utf8(&body)
            .unwrap()
            .contains("\"geocoding\""));
        json_response(
            200,
            serde_json::to_vec(&json!({
                "accessToken": "a",
                "refreshToken": "r",
                "expiresAt": 123,
                "expiresIn": 600,
                "scopes": ["geocoding"],
            }))
            .unwrap(),
        )
    })
    .await;

    let pair = mint_tokens(
        &Config::with_api_key("secret").base_url(srv.base_url),
        &["geocoding".into()],
        Some(600),
    )
    .await
    .unwrap();
    assert_eq!(pair.access_token, "a");
}

#[tokio::test]
async fn mint_tokens_empty_scopes() {
    let srv = DynServer::start(|req: Request| async move {
        assert_eq!(
            req.headers().get("x-api-key").and_then(|v| v.to_str().ok()),
            Some("secret")
        );
        let body = read_body(req).await;
        let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(v["scopes"].as_array().unwrap().len(), 0);
        json_response(
            200,
            serde_json::to_vec(&json!({
                "accessToken": "a",
                "refreshToken": "r",
                "expiresAt": 1,
                "scopes": ["geocoding"],
            }))
            .unwrap(),
        )
    })
    .await;

    let pair = mint_tokens(
        &Config::with_api_key("secret").base_url(srv.base_url),
        &[],
        None,
    )
    .await
    .unwrap();
    assert_eq!(pair.access_token, "a");
}

#[tokio::test]
async fn mint_tokens_with_scopes() {
    let srv = DynServer::start(|req: Request| async move {
        let body = read_body(req).await;
        assert!(std::str::from_utf8(&body).unwrap().contains("\"devices\""));
        json_response(
            200,
            serde_json::to_vec(&json!({
                "accessToken": "a",
                "refreshToken": "r",
                "expiresAt": 1,
            }))
            .unwrap(),
        )
    })
    .await;

    mint_tokens(
        &Config::with_api_key("k").base_url(srv.base_url),
        &["geocoding".into(), "devices".into()],
        Some(600),
    )
    .await
    .unwrap();
}

#[tokio::test]
async fn mint_requires_api_key() {
    let err = mint_tokens(&Config::default(), &[], None)
        .await
        .unwrap_err();
    assert!(err.is_invalid_config());
}

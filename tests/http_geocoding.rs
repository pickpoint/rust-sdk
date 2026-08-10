mod common;

use std::collections::HashMap;
use std::sync::atomic::{AtomicI32, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::extract::Request;
use common::{json_response, DynServer};
use pickpoint::{query, Client, Config};
use tokio::sync::Mutex;

#[tokio::test]
async fn geocode_empty_on_400() {
    let srv =
        DynServer::start(
            |_req: Request| async move { json_response(400, br#"{"message":"bad"}"#) },
        )
        .await;
    let c = Client::new(Config::with_api_key("k").base_url(srv.base_url))
        .await
        .unwrap();
    let out = c.forward(query([("q", "x")])).await.unwrap();
    assert!(out.is_empty());
}

#[tokio::test]
async fn forward_batch_concurrency() {
    let inflight = Arc::new(AtomicI32::new(0));
    let max = Arc::new(AtomicI32::new(0));
    let inflight2 = inflight.clone();
    let max2 = max.clone();
    let srv = DynServer::start(move |_req: Request| {
        let inflight = inflight2.clone();
        let max = max2.clone();
        async move {
            let cur = inflight.fetch_add(1, Ordering::SeqCst) + 1;
            max.fetch_max(cur, Ordering::SeqCst);
            tokio::time::sleep(Duration::from_millis(20)).await;
            inflight.fetch_sub(1, Ordering::SeqCst);
            json_response(200, br#"[{"ok":true}]"#)
        }
    })
    .await;

    let c = Client::new(
        Config::with_api_key("k")
            .base_url(srv.base_url)
            .concurrency(4),
    )
    .await
    .unwrap();
    let qs: Vec<_> = (0..12).map(|_| query([("q", "x")])).collect();
    let out = c.forward_batch(qs).await.unwrap();
    assert_eq!(out.len(), 12);
    assert!(max.load(Ordering::SeqCst) <= 4);
}

#[tokio::test]
async fn batch_pipeline_fills_slots() {
    let started: Arc<Mutex<HashMap<String, Instant>>> = Arc::new(Mutex::new(HashMap::new()));
    let started2 = started.clone();
    let srv = DynServer::start(move |req: Request| {
        let started = started2.clone();
        async move {
            let q = req
                .uri()
                .query()
                .unwrap_or("")
                .split('&')
                .find_map(|p| p.strip_prefix("q="))
                .unwrap_or("")
                .to_string();
            started.lock().await.insert(q.clone(), Instant::now());
            if q == "slow" {
                tokio::time::sleep(Duration::from_millis(80)).await;
            } else {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
            json_response(200, br#"[{"ok":true}]"#)
        }
    })
    .await;

    let c = Client::new(
        Config::with_api_key("k")
            .base_url(srv.base_url)
            .concurrency(2),
    )
    .await
    .unwrap();
    c.forward_batch(vec![
        query([("q", "slow")]),
        query([("q", "a")]),
        query([("q", "b")]),
        query([("q", "c")]),
    ])
    .await
    .unwrap();

    let map = started.lock().await;
    let slow_at = *map.get("slow").unwrap();
    let a_at = *map.get("a").unwrap();
    let b_at = *map.get("b").unwrap();
    assert!(
        b_at.duration_since(a_at) <= Duration::from_millis(40),
        "b started too late after a — wave batching?"
    );
    assert!(
        b_at < slow_at + Duration::from_millis(60),
        "b should overlap slow"
    );
}

#[tokio::test]
async fn batch_abort_on_403() {
    let hits = Arc::new(AtomicI32::new(0));
    let hits2 = hits.clone();
    let srv = DynServer::start(move |req: Request| {
        let hits = hits2.clone();
        async move {
            hits.fetch_add(1, Ordering::SeqCst);
            let q = req.uri().query().unwrap_or("");
            if q.contains("q=bad") {
                return json_response(403, b"{}");
            }
            tokio::time::sleep(Duration::from_millis(80)).await;
            json_response(200, br#"[{"ok":true}]"#)
        }
    })
    .await;

    let c = Client::new(
        Config::with_api_key("k")
            .base_url(srv.base_url)
            .concurrency(4),
    )
    .await
    .unwrap();
    let err = c
        .forward_batch(vec![
            query([("q", "bad")]),
            query([("q", "a")]),
            query([("q", "b")]),
            query([("q", "c")]),
            query([("q", "d")]),
            query([("q", "e")]),
        ])
        .await
        .unwrap_err();
    assert!(err.is_auth());
    assert!(hits.load(Ordering::SeqCst) < 6);
}

#[tokio::test]
async fn batch_preserves_order() {
    let srv = DynServer::start(|req: Request| async move {
        let q = req.uri().query().unwrap_or("");
        if q.contains("q=slow") {
            tokio::time::sleep(Duration::from_millis(60)).await;
            return json_response(200, br#"[{"id":"slow"}]"#);
        }
        json_response(200, br#"[{"id":"fast"}]"#)
    })
    .await;

    let c = Client::new(
        Config::with_api_key("k")
            .base_url(srv.base_url)
            .concurrency(10),
    )
    .await
    .unwrap();
    let out = c
        .forward_batch(vec![
            query([("q", "slow")]),
            query([("q", "fast1")]),
            query([("q", "fast2")]),
        ])
        .await
        .unwrap();
    assert_eq!(out[0][0]["id"], "slow");
    assert_eq!(out[1][0]["id"], "fast");
}

#[tokio::test]
async fn retry_budget_per_slot() {
    let attempts: Arc<Mutex<HashMap<String, i32>>> = Arc::new(Mutex::new(HashMap::new()));
    let attempts2 = attempts.clone();
    let srv = DynServer::start(move |req: Request| {
        let attempts = attempts2.clone();
        async move {
            let q = req
                .uri()
                .query()
                .unwrap_or("")
                .split('&')
                .find_map(|p| p.strip_prefix("q="))
                .unwrap_or("")
                .to_string();
            let n = {
                let mut g = attempts.lock().await;
                let e = g.entry(q.clone()).or_insert(0);
                *e += 1;
                *e
            };
            if q == "flaky" && n < 3 {
                return json_response(503, b"{}");
            }
            json_response(200, format!(r#"[{{"q":"{q}"}}]"#))
        }
    })
    .await;

    let c = Client::new(
        Config::with_api_key("k")
            .base_url(srv.base_url)
            .max_retries(5)
            .retry_base(pickpoint::MIN_RETRY_BASE),
    )
    .await
    .unwrap();
    let out = c
        .forward_batch(vec![query([("q", "flaky")]), query([("q", "ok")])])
        .await
        .unwrap();
    let map = attempts.lock().await;
    assert_eq!(map["flaky"], 3);
    assert_eq!(map["ok"], 1);
    assert_eq!(out[0][0]["q"], "flaky");
}

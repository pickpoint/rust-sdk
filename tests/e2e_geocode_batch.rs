//! Live e2e geocode batch (skipped unless `PICKPOINT_API_KEY` is set).

use std::time::Instant;

use pickpoint::{query, Client, Config};

const E2E_BATCH_SIZE: usize = 1000;

async fn e2e_client() -> Option<Client> {
    let key = std::env::var("PICKPOINT_API_KEY").ok()?;
    let base = std::env::var("PICKPOINT_BASE_URL")
        .unwrap_or_else(|_| "https://beta-api.pickpoint.io".into());
    Some(
        Client::new(
            Config::with_api_key(key)
                .base_url(base)
                .timeout(std::time::Duration::from_secs(60)),
        )
        .await
        .unwrap(),
    )
}

#[tokio::test]
async fn e2e_forward_batch_1000() {
    let Some(c) = e2e_client().await else {
        eprintln!("skipping: PICKPOINT_API_KEY not set");
        return;
    };
    let qs: Vec<_> = (0..E2E_BATCH_SIZE)
        .map(|_| query([("q", "Berlin"), ("limit", "1")]))
        .collect();
    let start = Instant::now();
    let out = c.forward_batch(qs).await.unwrap();
    let wall = start.elapsed();
    assert_eq!(out.len(), E2E_BATCH_SIZE);
    for (i, slot) in out.iter().enumerate() {
        assert!(!slot.is_empty(), "slot {i} empty");
    }
    eprintln!("forward batch n={E2E_BATCH_SIZE} wall={wall:?}");
}

#[tokio::test]
async fn e2e_reverse_batch_1000() {
    let Some(c) = e2e_client().await else {
        eprintln!("skipping: PICKPOINT_API_KEY not set");
        return;
    };
    let qs: Vec<_> = (0..E2E_BATCH_SIZE)
        .map(|_| query([("lat", "52.5163"), ("lon", "13.3777")]))
        .collect();
    let start = Instant::now();
    let out = c.reverse_batch(qs).await.unwrap();
    let wall = start.elapsed();
    assert_eq!(out.len(), E2E_BATCH_SIZE);
    for (i, slot) in out.iter().enumerate() {
        assert!(slot.is_some(), "slot {i} nil");
    }
    eprintln!("reverse batch n={E2E_BATCH_SIZE} wall={wall:?}");
}

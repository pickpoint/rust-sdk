use httpmock::prelude::*;
use pickpoint::{query, Client, Config, DeviceListQuery, Error};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Notify;

mod common;

use axum::extract::Request;
use common::{json_response, DynServer};

#[tokio::test]
async fn invalid_config() {
    assert!(Client::new(Config::default()).await.is_err());
    assert!(Client::new(Config {
        api_key: Some("a".into()),
        access_token: Some("b".into()),
        ..Default::default()
    })
    .await
    .is_err());
}

#[tokio::test]
async fn forward_and_search_share_api_key() {
    let server = MockServer::start();

    let forward = server.mock(|when, then| {
        when.method(GET)
            .path("/v2/geocode/forward")
            .header("x-api-key", "secret");
        then.status(200)
            .header("content-type", "application/json")
            .body(r#"[{"display_name":"Berlin"}]"#);
    });
    let search = server.mock(|when, then| {
        when.method(GET)
            .path("/v2/address/search")
            .header("x-api-key", "secret");
        then.status(200)
            .header("content-type", "application/json")
            .body(r#"{"type":"FeatureCollection","features":[]}"#);
    });
    let devices = server.mock(|when, then| {
        when.method(GET)
            .path("/v2/devices")
            .header("x-api-key", "secret");
        then.status(200)
            .header("content-type", "application/json")
            .body(r#"{"data":[{"uid":"d1","name":"A","type":"car"}],"total":1}"#);
    });

    let c = Client::new(Config::with_api_key("secret").base_url(server.base_url()))
        .await
        .unwrap();

    let places = c.forward(query([("q", "Berlin")])).await.unwrap();
    assert_eq!(places.len(), 1);
    c.search(query([("q", "Berlin")])).await.unwrap();
    let list = c.devices().list(DeviceListQuery::default()).await.unwrap();
    assert_eq!(list.total, 1);

    forward.assert();
    search.assert();
    devices.assert();
}

#[tokio::test]
async fn geocode_empty_on_4xx() {
    let server = MockServer::start();
    server.mock(|when, then| {
        when.method(GET).path("/v2/geocode/forward");
        then.status(404).body("not found");
    });
    server.mock(|when, then| {
        when.method(GET).path("/v2/geocode/reverse");
        then.status(400).body("bad");
    });

    let c = Client::new(Config::with_api_key("k").base_url(server.base_url()))
        .await
        .unwrap();
    let places = c.forward(query([("q", "nowhere")])).await.unwrap();
    assert!(places.is_empty());
    let rev = c
        .reverse(query([("lat", "0"), ("lon", "0")]))
        .await
        .unwrap();
    assert!(rev.is_none());
}

#[tokio::test]
async fn devices_404() {
    let server = MockServer::start();
    server.mock(|when, then| {
        when.method(GET).path("/v2/devices/missing");
        then.status(404).body(r#"{"message":"Device not found"}"#);
    });
    let c = Client::new(Config::with_api_key("k").base_url(server.base_url()))
        .await
        .unwrap();
    let err = c.devices().get("missing").await.unwrap_err();
    assert!(err.is_not_found());
}

#[tokio::test]
async fn devices_conflict_409() {
    let server = MockServer::start();
    server.mock(|when, then| {
        when.method(POST).path("/v2/devices/u1/command");
        then.status(409).body(r#"{"message":"device offline"}"#);
    });
    let c = Client::new(Config::with_api_key("k").base_url(server.base_url()))
        .await
        .unwrap();
    let err = c.devices().command("u1", b"x").await.unwrap_err();
    assert!(err.is_conflict());
}

#[tokio::test]
async fn command_base64() {
    let server = MockServer::start();
    let m = server.mock(|when, then| {
        when.method(POST)
            .path("/v2/devices/uid-1/command")
            .json_body(serde_json::json!({"payload": "aGk="}));
        then.status(200)
            .header("content-type", "application/json")
            .body(r#"{"delivered":1}"#);
    });
    let c = Client::new(Config::with_api_key("k").base_url(server.base_url()))
        .await
        .unwrap();
    let out = c.devices().command("uid-1", b"hi").await.unwrap();
    assert_eq!(out.delivered, 1);
    m.assert();
}

#[tokio::test]
async fn address_search_400_throws() {
    let server = MockServer::start();
    server.mock(|when, then| {
        when.method(GET).path("/v2/address/search");
        then.status(400)
            .body(r#"{"message":"bad","errorCode":400}"#);
    });
    let c = Client::new(Config::with_api_key("k").base_url(server.base_url()))
        .await
        .unwrap();
    match c.search(query([("q", "x")])).await {
        Err(Error::Api(api)) => assert_eq!(api.status, 400),
        other => panic!("{other:?}"),
    }
}

#[tokio::test]
async fn routing_400_throws() {
    let server = MockServer::start();
    server.mock(|when, then| {
        when.method(POST).path("/v2/route");
        then.status(400)
            .body(r#"{"message":"bad","errorCode":400}"#);
    });
    let c = Client::new(Config::with_api_key("k").base_url(server.base_url()))
        .await
        .unwrap();
    match c.route(serde_json::json!({})).await {
        Err(Error::Api(api)) => assert_eq!(api.status, 400),
        other => panic!("{other:?}"),
    }
}

#[tokio::test]
async fn request_cancel_aborts() {
    let started = Arc::new(Notify::new());
    let started2 = started.clone();
    let _srv = DynServer::start(move |_req: Request| {
        let started = started2.clone();
        async move {
            started.notify_one();
            std::future::pending::<()>().await;
            json_response(200, b"{}")
        }
    })
    .await;

    let c = Client::new(
        Config::with_api_key("k")
            .base_url(_srv.base_url.clone())
            .timeout(Duration::from_secs(60)),
    )
    .await
    .unwrap();

    let handle = tokio::spawn({
        let c = c.clone();
        async move { c.search(query([("q", "x")])).await }
    });
    tokio::time::timeout(Duration::from_secs(2), started.notified())
        .await
        .expect("handler never started");
    // Abort the in-flight HTTP request (Go context cancel parity).
    handle.abort();
    assert!(handle.await.unwrap_err().is_cancelled());
}

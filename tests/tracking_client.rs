mod common;

use std::sync::atomic::{AtomicI32, Ordering};
use std::sync::Arc;
use std::time::Duration;

use common::tracking_mock::{server_error, MockOpts, MockServer, MOCK_DEVICE_UID};
use pickpoint::tracking::{
    connect, ClientCmd, Config, ConnectionState, DeviceAuth, ErrorCode, LatLng, ListenerAuth,
    ServerEvt, MAX_EVENT_BYTES, MAX_PUBLISH_HZ, MIN_PUBLISH_INTERVAL,
};

#[tokio::test]
async fn publish_rate_limit() {
    let ms = MockServer::start(true, None).await;
    let c = connect(Config {
        endpoint: ms.url.clone(),
        device: Some(DeviceAuth {
            client_id: "c".into(),
            client_secret: "s".into(),
        }),
        disable_reconnect: true,
        ..Default::default()
    })
    .await
    .unwrap();

    c.start_track(
        Some(LatLng {
            latitude: 1.0,
            longitude: 2.0,
            ..Default::default()
        }),
        vec![],
    )
    .await
    .unwrap();

    let mut accepted = 0;
    for i in 0..(MAX_PUBLISH_HZ * 3) {
        let (_, ok) = c
            .publish(LatLng {
                latitude: i as f64,
                longitude: 0.0,
                ..Default::default()
            })
            .await;
        if ok {
            accepted += 1;
        }
    }
    assert_eq!(accepted, 1);
    assert_eq!(c.client_seq().await, 1);

    tokio::time::sleep(MIN_PUBLISH_INTERVAL + Duration::from_millis(5)).await;
    let (seq, ok) = c
        .publish(LatLng {
            latitude: 9.0,
            longitude: 9.0,
            ..Default::default()
        })
        .await;
    assert!(ok);
    assert_eq!(seq, 2);
    c.close().await.unwrap();
}

#[tokio::test]
async fn send_event_limits() {
    let ms = MockServer::start(true, None).await;
    let c = connect(Config {
        endpoint: ms.url.clone(),
        device: Some(DeviceAuth {
            client_id: "c".into(),
            client_secret: "s".into(),
        }),
        disable_reconnect: true,
        ..Default::default()
    })
    .await
    .unwrap();
    c.start_track(None, vec![]).await.unwrap();

    assert!(c.send_event(vec![0u8; MAX_EVENT_BYTES + 1]).await.is_err());
    let ok = c.send_event(b"a".to_vec()).await.unwrap();
    assert!(ok);
    let ok = c.send_event(b"b".to_vec()).await.unwrap();
    assert!(!ok);
    c.close().await.unwrap();
}

#[tokio::test]
async fn resume_after_publish() {
    let ms = MockServer::start(true, None).await;
    let c = connect(Config {
        endpoint: ms.url.clone(),
        device: Some(DeviceAuth {
            client_id: "c".into(),
            client_secret: "s".into(),
        }),
        disable_reconnect: true,
        ..Default::default()
    })
    .await
    .unwrap();

    let uid = c
        .start_track(
            Some(LatLng {
                latitude: 1.0,
                longitude: 1.0,
                ..Default::default()
            }),
            vec![],
        )
        .await
        .unwrap();
    assert!(
        c.publish(LatLng {
            latitude: 2.0,
            longitude: 2.0,
            ..Default::default()
        })
        .await
        .1
    );

    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    while tokio::time::Instant::now() < deadline {
        if c.last_acked_seq().await >= 1 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(c.last_acked_seq().await, 1);

    let acked = c.resume(uid, 1).await.unwrap();
    assert_eq!(acked, 0);
    ms.wait_msg(
        |m| matches!(m, ClientCmd::Resume { .. }),
        Duration::from_secs(2),
    )
    .await;
    c.close().await.unwrap();
}

#[tokio::test]
async fn listener_subscribe_and_location() {
    let on_msg: common::tracking_mock::OnMsg = Arc::new(|msg, conn| {
        if let ClientCmd::Subscribe { device_uid, .. } = msg {
            tokio::spawn(async move {
                tokio::time::sleep(Duration::from_millis(20)).await;
                conn.send(&ServerEvt::LocationAdded {
                    device_uid: device_uid.clone(),
                    track_uid: "11111111-1111-1111-1111-111111111111".into(),
                    client_seq: 3,
                    sub: 1,
                    point: LatLng {
                        latitude: 1.5,
                        longitude: 2.5,
                        ..Default::default()
                    },
                })
                .await;
            });
        }
    });
    let ms = MockServer::start(true, Some(on_msg)).await;
    let c = connect(Config {
        endpoint: ms.url.clone(),
        listener: Some(ListenerAuth {
            access_token: "jwt".into(),
        }),
        disable_reconnect: true,
        ..Default::default()
    })
    .await
    .unwrap();

    c.subscribe(MOCK_DEVICE_UID).await.unwrap();

    let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
    while tokio::time::Instant::now() < deadline {
        let msg = c.recv().await.unwrap();
        match msg {
            ServerEvt::LocationAdded { point, .. } => {
                assert_eq!(point.latitude, 1.5);
                c.close().await.unwrap();
                return;
            }
            ServerEvt::Subscribed { .. } => continue,
            _ => {}
        }
    }
    panic!("no location");
}

#[tokio::test]
async fn auth_error_without_refresh_closes() {
    let on_msg: common::tracking_mock::OnMsg = Arc::new(|msg, conn| {
        if matches!(msg, ClientCmd::TrackStart { .. }) {
            let conn = conn.clone();
            tokio::spawn(async move {
                conn.send(&server_error(ErrorCode::Auth, "bad creds")).await;
            });
        }
    });
    let ms = MockServer::start_opts(MockOpts {
        auto: false,
        on_msg: Some(on_msg),
        ..Default::default()
    })
    .await;

    let c = connect(Config {
        endpoint: ms.url.clone(),
        device: Some(DeviceAuth {
            client_id: "c".into(),
            client_secret: "s".into(),
        }),
        reconnect_min_delay: Duration::from_millis(10),
        reconnect_max_delay: Duration::from_millis(20),
        ..Default::default()
    })
    .await
    .unwrap();

    let err = c.start_track(None, vec![]).await.unwrap_err();
    assert_eq!(err.code, ErrorCode::Auth);

    let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
    while tokio::time::Instant::now() < deadline {
        if c.state().await == ConnectionState::Closed {
            c.close().await.ok();
            return;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("expected closed state");
}

#[tokio::test]
async fn auth_error_refresh_redials() {
    let hellos = Arc::new(AtomicI32::new(0));
    let hellos2 = hellos.clone();
    let (tx, mut rx) = tokio::sync::mpsc::channel::<()>(1);
    let tx = Arc::new(tx);

    let before: common::tracking_mock::BeforeHello = Arc::new(move |_idx, _conn| {
        hellos2.fetch_add(1, Ordering::SeqCst);
    });
    let on_msg: common::tracking_mock::OnMsg = Arc::new(|msg, conn| {
        if matches!(msg, ClientCmd::TrackStart { .. }) {
            let conn = conn.clone();
            tokio::spawn(async move {
                conn.send(&server_error(ErrorCode::Unauthorized, "expired"))
                    .await;
            });
        }
    });
    let ms = MockServer::start_opts(MockOpts {
        auto: false,
        before_hello: Some(before),
        on_msg: Some(on_msg),
        ..Default::default()
    })
    .await;

    let tx2 = tx.clone();
    let c = connect(Config {
        endpoint: ms.url.clone(),
        device: Some(DeviceAuth {
            client_id: "c".into(),
            client_secret: "s".into(),
        }),
        reconnect_min_delay: Duration::from_millis(15),
        reconnect_max_delay: Duration::from_millis(40),
        hello_timeout: Duration::from_secs(2),
        refresh_auth: Some(Arc::new(move || {
            let tx = tx2.clone();
            Box::pin(async move {
                let _ = tx.send(()).await;
                Ok((
                    Some(DeviceAuth {
                        client_id: "c2".into(),
                        client_secret: "s2".into(),
                    }),
                    None,
                ))
            })
        })),
        ..Default::default()
    })
    .await
    .unwrap();

    let c2 = c.clone();
    tokio::spawn(async move {
        let _ = c2.start_track(None, vec![]).await;
    });

    tokio::time::timeout(Duration::from_secs(3), rx.recv())
        .await
        .expect("refreshAuth not called")
        .unwrap();

    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while tokio::time::Instant::now() < deadline {
        if hellos.load(Ordering::SeqCst) >= 2 {
            c.close().await.ok();
            return;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("expected second hello after refresh");
}

#[tokio::test]
async fn publish_starts_track_and_close_stops() {
    let ms = MockServer::start(true, None).await;
    let c = connect(Config {
        endpoint: ms.url.clone(),
        device: Some(DeviceAuth {
            client_id: "c".into(),
            client_secret: "s".into(),
        }),
        disable_reconnect: true,
        ..Default::default()
    })
    .await
    .unwrap();

    c.publish(LatLng::new(55.75, 37.61)).await;
    ms.wait_msg(
        |m| matches!(m, ClientCmd::TrackStart { .. }),
        Duration::from_secs(2),
    )
    .await;

    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    while c.track_uid().await.is_empty() {
        assert!(
            tokio::time::Instant::now() < deadline,
            "track_uid after auto-start"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    c.close().await.unwrap();
    ms.wait_msg(
        |m| matches!(m, ClientCmd::TrackStop { .. }),
        Duration::from_secs(2),
    )
    .await;
}

mod common;

use std::sync::Arc;
use std::time::Duration;

use common::tracking_mock::{server_error, MockOpts, MockServer};
use pickpoint::tracking::v2::{client_msg, ErrorCode, LatLng, Relocate};
use pickpoint::tracking::{connect, Config, ConnectionState, DeviceAuth};

#[tokio::test]
async fn reconnect_sends_resume_not_track_start() {
    let ms = MockServer::start(true, None).await;
    let c = connect(Config {
        endpoint: ms.url.clone(),
        device: Some(DeviceAuth {
            client_id: "c".into(),
            client_secret: "s".into(),
        }),
        reconnect_min_delay: Duration::from_millis(20),
        reconnect_max_delay: Duration::from_millis(50),
        ..Default::default()
    })
    .await
    .unwrap();

    let uid = c.start_track(None, vec![]).await.unwrap();
    c.publish(LatLng {
        latitude: 1.0,
        longitude: 2.0,
        ..Default::default()
    })
    .await;
    tokio::time::sleep(Duration::from_millis(25)).await;
    c.publish(LatLng {
        latitude: 3.0,
        longitude: 4.0,
        ..Default::default()
    })
    .await;
    assert_eq!(c.client_seq().await, 2);

    let first = ms.wait_conn(Duration::from_secs(2)).await;
    first.close().await;

    let resume = ms
        .wait_msg(
            |m| matches!(m.body, Some(client_msg::Body::Resume(_))),
            Duration::from_secs(8),
        )
        .await;
    if let Some(client_msg::Body::Resume(r)) = resume.body {
        assert_eq!(r.track_uid, uid);
        assert_eq!(r.last_client_seq, 2);
    } else {
        panic!("expected resume");
    }

    let msgs = ms.all_messages().await;
    let starts = msgs
        .iter()
        .filter(|m| matches!(m.body, Some(client_msg::Body::TrackStart(_))))
        .count();
    assert_eq!(starts, 1);

    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while tokio::time::Instant::now() < deadline {
        if c.state().await == ConnectionState::Open {
            c.close().await.unwrap();
            return;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("not open after reconnect");
}

#[tokio::test]
async fn reconnect_track_not_found_clears_cursor() {
    let on_msg: common::tracking_mock::OnMsg = Arc::new(|msg, conn| {
        let conn = conn.clone();
        tokio::spawn(async move {
            match msg.body {
                Some(client_msg::Body::TrackStart(_)) => {
                    conn.send(&pickpoint::tracking::v2::ServerMsg {
                        body: Some(pickpoint::tracking::v2::server_msg::Body::TrackStarted(
                            pickpoint::tracking::v2::TrackStarted {
                                track_uid: "t-gone".into(),
                                metadata: Vec::new(),
                            },
                        )),
                    })
                    .await;
                }
                Some(client_msg::Body::Resume(_)) => {
                    conn.send(&server_error(ErrorCode::TrackNotFound, "track expired"))
                        .await;
                }
                _ => {}
            }
        });
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
        reconnect_min_delay: Duration::from_millis(20),
        reconnect_max_delay: Duration::from_millis(40),
        ..Default::default()
    })
    .await
    .unwrap();

    c.start_track(None, vec![]).await.unwrap();
    assert_eq!(c.track_uid().await, "t-gone");

    let conn = ms.wait_conn(Duration::from_secs(2)).await;
    conn.close().await;

    ms.wait_msg(
        |m| matches!(m.body, Some(client_msg::Body::Resume(_))),
        Duration::from_secs(8),
    )
    .await;

    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while tokio::time::Instant::now() < deadline {
        if c.track_uid().await.is_empty() {
            c.close().await.unwrap();
            return;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("track uid not cleared");
}

#[tokio::test]
async fn relocate_dials_new_endpoint() {
    let target = MockServer::start(true, None).await;
    let gateway = MockServer::start_opts(MockOpts {
        auto: false,
        relocate_on_connect: Some(Relocate {
            endpoint: target.url.clone(),
            retry_after_ms: 10,
        }),
        ..Default::default()
    })
    .await;

    let c = connect(Config {
        endpoint: gateway.url.clone(),
        device: Some(DeviceAuth {
            client_id: "c".into(),
            client_secret: "s".into(),
        }),
        disable_reconnect: true,
        ..Default::default()
    })
    .await
    .unwrap();

    assert_eq!(c.state().await, ConnectionState::Open);
    assert!(target.conn_count().await >= 1);
    let uid = c.start_track(None, vec![]).await.unwrap();
    assert_eq!(uid, "track-mock-1");
    c.close().await.unwrap();
}

#[tokio::test]
async fn queue_flush_after_resume() {
    let pair = Arc::new((std::sync::Mutex::new(true), std::sync::Condvar::new()));
    let pair2 = pair.clone();
    let before: common::tracking_mock::BeforeHello = Arc::new(move |idx, _conn| {
        if idx >= 2 {
            let (lock, cvar) = &*pair2;
            let mut holding = lock.lock().unwrap();
            while *holding {
                holding = cvar.wait(holding).unwrap();
            }
        }
    });

    let ms = MockServer::start_opts(MockOpts {
        auto: true,
        before_hello: Some(before),
        ..Default::default()
    })
    .await;

    let c = connect(Config {
        endpoint: ms.url.clone(),
        device: Some(DeviceAuth {
            client_id: "c".into(),
            client_secret: "s".into(),
        }),
        reconnect_min_delay: Duration::from_millis(20),
        reconnect_max_delay: Duration::from_millis(50),
        ..Default::default()
    })
    .await
    .unwrap();

    c.start_track(None, vec![]).await.unwrap();
    let conn = ms.wait_conn(Duration::from_secs(2)).await;
    conn.close().await;

    let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
    while tokio::time::Instant::now() < deadline {
        if c.state().await == ConnectionState::Reconnecting {
            break;
        }
        tokio::time::sleep(Duration::from_millis(15)).await;
    }
    assert_eq!(c.state().await, ConnectionState::Reconnecting);

    let (seq, ok) = c
        .publish(LatLng {
            latitude: 9.0,
            longitude: 9.0,
            ..Default::default()
        })
        .await;
    assert!(ok);
    assert_eq!(seq, 1);

    {
        let (lock, cvar) = &*pair;
        let mut holding = lock.lock().unwrap();
        *holding = false;
        cvar.notify_all();
    }

    ms.wait_msg(
        |m| matches!(m.body, Some(client_msg::Body::Resume(_))),
        Duration::from_secs(8),
    )
    .await;
    ms.wait_msg(
        |m| {
            matches!(
                m.body,
                Some(client_msg::Body::LocationBatch(_)) | Some(client_msg::Body::LocationAdd(_))
            )
        },
        Duration::from_secs(8),
    )
    .await;
    c.close().await.unwrap();
}

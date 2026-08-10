use std::time::{Duration, Instant};

use pickpoint::tracking::v2::LatLng;
use pickpoint::tracking::v2::{client_msg, server_msg, ClientMsg, Hello, ServerMsg};
use pickpoint::tracking::{
    build_ws_url, can_accept_publish, client_resume, decode_server_msg, encode_client_msg,
    new_backoff, next_delay, next_publish_allowed_at, reset_backoff, stamp_lat_lng, Config,
    DeviceAuth, ListenerAuth, OfflineQueue, MAX_PUBLISH_HZ, MIN_PUBLISH_INTERVAL,
    MIN_PUBLISH_INTERVAL_MS,
};
use prost::Message;

#[test]
fn backoff_full_jitter() {
    let mut state = new_backoff(Duration::from_millis(100), Duration::from_millis(800), 0);
    let values = [0.0, 0.5, 0.999];
    let d0 = next_delay(&mut state, values[0]).unwrap();
    assert_eq!(d0, Duration::ZERO);
    let d1 = next_delay(&mut state, values[1]).unwrap();
    assert_eq!(d1, Duration::from_millis(100));
    let d2 = next_delay(&mut state, values[2]).unwrap();
    assert_eq!(d2, Duration::from_millis(399));
}

#[test]
fn backoff_max_attempts() {
    let mut state = new_backoff(Duration::from_millis(10), Duration::ZERO, 2);
    assert!(next_delay(&mut state, 0.0).is_some());
    assert!(next_delay(&mut state, 0.0).is_some());
    assert!(next_delay(&mut state, 0.0).is_none());
}

#[test]
fn backoff_reset() {
    let mut state = new_backoff(Duration::from_millis(10), Duration::ZERO, 1);
    assert!(next_delay(&mut state, 0.0).is_some());
    assert!(next_delay(&mut state, 0.0).is_none());
    reset_backoff(&mut state);
    assert!(next_delay(&mut state, 0.0).is_some());
}

#[test]
fn offline_queue_ack_through() {
    let mut q = OfflineQueue::new(10);
    let p = |lat| LatLng {
        latitude: lat,
        ..Default::default()
    };
    q.enqueue(1, p(1.0));
    q.enqueue(2, p(2.0));
    q.enqueue(3, p(3.0));
    q.ack_through(2);
    let got = q.peek_all();
    assert_eq!(got.len(), 1);
    assert_eq!(got[0].seq, 3);
}

#[test]
fn offline_queue_drop_oldest() {
    let mut q = OfflineQueue::new(2);
    let p = LatLng {
        latitude: 1.0,
        ..Default::default()
    };
    assert_eq!(q.enqueue(1, p), 0);
    assert_eq!(q.enqueue(2, p), 0);
    assert_eq!(q.enqueue(3, p), 1);
    let got = q.peek_all();
    assert_eq!(got.len(), 2);
    assert_eq!(got[0].seq, 2);
    assert_eq!(got[1].seq, 3);
}

#[test]
fn publish_rate_spacing() {
    assert_eq!(MAX_PUBLISH_HZ, 50);
    assert_eq!(MIN_PUBLISH_INTERVAL_MS, 20);
    let now = Instant::now();
    assert!(can_accept_publish(now, now, 1));
    let next = next_publish_allowed_at(now, now, 1);
    assert_eq!(next, now + Duration::from_millis(20));
    assert!(!can_accept_publish(
        next,
        now + Duration::from_millis(19),
        1
    ));
    assert!(can_accept_publish(next, now + Duration::from_millis(20), 1));
}

#[test]
fn publish_rate_batch_slots() {
    let now = Instant::now();
    let next = next_publish_allowed_at(now, now, 50);
    let want = now + MIN_PUBLISH_INTERVAL * 50;
    assert_eq!(next, want);
    assert!(!can_accept_publish(
        next,
        now + Duration::from_millis(999),
        1
    ));
    assert!(can_accept_publish(
        next,
        now + Duration::from_millis(1000),
        1
    ));
}

#[test]
fn build_ws_url_device() {
    let u = build_ws_url(&Config {
        endpoint: "https://tracking.example.com".into(),
        device: Some(DeviceAuth {
            client_id: "id".into(),
            client_secret: "sec".into(),
        }),
        ..Default::default()
    })
    .unwrap();
    assert_eq!(u.scheme(), "wss");
    assert_eq!(u.path(), "/v2/tracking/ws");
    let q: std::collections::HashMap<_, _> = u.query_pairs().into_owned().collect();
    assert_eq!(q.get("client-id").map(String::as_str), Some("id"));
    assert_eq!(q.get("client-secret").map(String::as_str), Some("sec"));
}

#[test]
fn build_ws_url_listener() {
    let u = build_ws_url(&Config {
        endpoint: "ws://localhost:1".into(),
        listener: Some(ListenerAuth {
            access_token: "jwt".into(),
        }),
        ..Default::default()
    })
    .unwrap();
    let q: std::collections::HashMap<_, _> = u.query_pairs().into_owned().collect();
    assert_eq!(q.get("access-token").map(String::as_str), Some("jwt"));
}

#[test]
fn stamp_lat_lng_default_timestamp() {
    let before = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;
    let p = stamp_lat_lng(LatLng {
        latitude: 1.0,
        longitude: 2.0,
        ..Default::default()
    });
    let after = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;
    let ts = p.timestamp_ms.unwrap();
    assert!(ts >= before && ts <= after);
}

#[test]
fn stamp_lat_lng_preserves_timestamp() {
    let p = stamp_lat_lng(LatLng {
        latitude: 1.0,
        longitude: 2.0,
        timestamp_ms: Some(42),
        ..Default::default()
    });
    assert_eq!(p.timestamp_ms, Some(42));
}

#[test]
fn codec_round_trip_resume() {
    let msg = client_resume("t1", 9);
    let b = encode_client_msg(&msg).unwrap();
    let round = ClientMsg::decode(&b[..]).unwrap();
    match round.body {
        Some(client_msg::Body::Resume(r)) => {
            assert_eq!(r.track_uid, "t1");
            assert_eq!(r.last_client_seq, 9);
        }
        other => panic!("{other:?}"),
    }
}

#[test]
fn codec_round_trip_hello() {
    let msg = ServerMsg {
        body: Some(server_msg::Body::Hello(Hello {
            node_id: "n1".into(),
            shard: 7,
        })),
    };
    let b = prost::Message::encode_to_vec(&msg);
    let got = decode_server_msg(&b).unwrap();
    match got.body {
        Some(server_msg::Body::Hello(h)) => {
            assert_eq!(h.node_id, "n1");
            assert_eq!(h.shard, 7);
        }
        other => panic!("{other:?}"),
    }
}

#[test]
fn golden_resume_wire() {
    let msg = client_resume("track-uid-9", 42);
    let b = encode_client_msg(&msg).unwrap();
    let got = hex::encode(&b);
    assert_eq!(got, "0a0f0a0b747261636b2d7569642d39102a");
}

use url::Url;

use crate::tracking::client::Config;

/// Normalize endpoint to ws/wss and append path + auth query.
pub fn build_ws_url(cfg: &Config) -> Result<Url, String> {
    let mut raw = cfg.endpoint.trim().to_string();
    if raw.is_empty() {
        return Err("tracking: Endpoint is required".into());
    }
    if !raw.contains("://") {
        raw = format!("ws://{raw}");
    }
    let mut u = Url::parse(&raw).map_err(|e| format!("tracking: bad endpoint: {e}"))?;
    match u.scheme() {
        "http" => {
            u.set_scheme("ws")
                .map_err(|_| "tracking: cannot set ws scheme".to_string())?;
        }
        "https" => {
            u.set_scheme("wss")
                .map_err(|_| "tracking: cannot set wss scheme".to_string())?;
        }
        "ws" | "wss" => {}
        other => return Err(format!("tracking: unsupported scheme {other:?}")),
    }
    let path = if cfg.ws_path.is_empty() {
        "/v2/tracking/ws"
    } else {
        cfg.ws_path.as_str()
    };
    u.set_path(path);
    {
        let mut q = u.query_pairs_mut();
        if let Some(device) = &cfg.device {
            q.append_pair("client-id", &device.client_id);
            q.append_pair("client-secret", &device.client_secret);
        } else if let Some(listener) = &cfg.listener {
            q.append_pair("access-token", &listener.access_token);
        }
    }
    Ok(u)
}

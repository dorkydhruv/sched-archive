use axum::{
    Router,
    extract::State,
    http::{Method, StatusCode},
    response::{Html, IntoResponse, Json},
    routing::{get, post},
};
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;
use tower_http::cors::{Any, CorsLayer};
use tower_http::trace::TraceLayer;
use tracing::{info, warn};

use crate::config_store::{ConfigData, ConfigStore, ConfigUpdate};

#[derive(Clone)]
pub struct AppState {
    pub config: ConfigStore,
}

pub async fn start_server(
    config_store: ConfigStore,
    port: u16,
    shutdown: CancellationToken,
) -> std::result::Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let app_state = AppState {
        config: config_store.clone(),
    };

    let app: Router<()> = Router::new()
        .route("/", get(index))
        .route("/config", get(get_config))
        .route("/config", post(update_config))
        .with_state(app_state.clone())
        .layer(
            CorsLayer::new()
                .allow_origin(Any)
                .allow_methods([Method::GET, Method::POST, Method::OPTIONS])
                .allow_headers(Any),
        )
        .layer(TraceLayer::new_for_http());

    let addr = format!("127.0.0.1:{port}");
    let listener = TcpListener::bind(&addr).await?;

    info!("Config UI listening on http://{}", addr);

    axum::serve(listener, app)
        .with_graceful_shutdown(async move {
            shutdown.cancelled().await;
        })
        .await?;
    Ok(())
}

async fn index() -> impl IntoResponse {
    let html = include_str!("../ui/index.html");
    Html(html.to_string())
}

async fn get_config(State(state): State<AppState>) -> impl IntoResponse {
    // ConfigStore::read() is now synchronous (std::sync::RwLock)
    let config = state.config.read();
    let json = config_to_json(&config);
    Json(json)
}

async fn update_config(
    State(state): State<AppState>,
    Json(payload): Json<ConfigUpdate>,
) -> impl IntoResponse {
    // Handle scheduler type switch specially - may need scheduler restart
    let scheduler_type_changed = match &payload.scheduler {
        Some(sched) => matches!(
            sched,
            crate::config_store::SchedulerConfigData::Fifo
                | crate::config_store::SchedulerConfigData::GreedyRevenue
                | crate::config_store::SchedulerConfigData::GreedyThroughput
        ),
        None => false,
    };

    // ConfigStore::update() is now synchronous
    state.config.update(payload);

    if scheduler_type_changed {
        warn!("Scheduler type change detected - full restart required");
        (
            StatusCode::OK,
            Json(serde_json::json!({
                "status": "scheduler_type_changed",
                "message": "Scheduler type changed. Please restart the scheduler."
            })),
        )
    } else {
        (
            StatusCode::OK,
            Json(serde_json::json!({
                "status": "ok"
            })),
        )
    }
}

fn config_to_json(config: &ConfigData) -> serde_json::Value {
    let scheduler = match &config.scheduler {
        crate::config_store::SchedulerConfigData::Batch(batch) => {
            serde_json::json!({
                "type": "Batch",
                "batch": {
                    "keypair_path": batch.keypair_path,
                    "tip": {
                        "vote_account": batch.tip.vote_account,
                        "merkle_authority": batch.tip.merkle_authority,
                        "commission_bps": batch.tip.commission_bps,
                    },
                    "jito": {
                        "http_rpc": batch.jito.http_rpc,
                        "ws_rpc": batch.jito.ws_rpc,
                        "block_engine": batch.jito.block_engine,
                    },
                    "unchecked_capacity": batch.unchecked_capacity,
                    "checked_capacity": batch.checked_capacity,
                    "bundle_capacity": batch.bundle_capacity,
                    "block_fill_cutoff": batch.block_fill_cutoff,
                    "max_check_batches": batch.max_check_batches,
                    "bundle_expiry_ms": batch.bundle_expiry_ms,
                    "progress_timeout_sec": batch.progress_timeout_sec,
                }
            })
        }
        crate::config_store::SchedulerConfigData::Fifo => {
            serde_json::json!({ "type": "Fifo" })
        }
        crate::config_store::SchedulerConfigData::GreedyRevenue => {
            serde_json::json!({ "type": "GreedyRevenue" })
        }
        crate::config_store::SchedulerConfigData::GreedyThroughput => {
            serde_json::json!({ "type": "GreedyThroughput" })
        }
    };

    serde_json::json!({
        "host_name": config.host_name,
        "nats_servers": config.nats_servers,
        "filter_keys": config.filter_keys.iter().map(|k| k.to_string()).collect::<Vec<_>>(),
        "scheduler": scheduler,
    })
}

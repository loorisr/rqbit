use std::num::NonZeroU16;

use axum::{Json, extract::State, response::IntoResponse};
use serde::{Deserialize, Serialize};

use super::ApiState;
use crate::{
    Session,
    api::{EmptyJsonResponse, Result},
    limits::LimitsConfig,
};

pub async fn h_update_session_ratelimits(
    State(state): State<ApiState>,
    Json(limits): Json<LimitsConfig>,
) -> Result<impl IntoResponse> {
    state
        .api
        .session()
        .ratelimits
        .set_upload_bps(limits.upload_bps);
    state
        .api
        .session()
        .ratelimits
        .set_download_bps(limits.download_bps);
    Ok(Json(EmptyJsonResponse {}))
}

pub async fn h_get_session_ratelimits(State(state): State<ApiState>) -> Result<impl IntoResponse> {
    let config = state.api.session().ratelimits.get_config();
    Ok(Json(config))
}

#[derive(Serialize)]
struct ListenPortResponse {
    listen_port: Option<u16>,
    announce_port: Option<u16>,
}

fn listen_port_response(session: &Session) -> Json<ListenPortResponse> {
    Json(ListenPortResponse {
        listen_port: session.listen_addr().map(|a| a.port()),
        announce_port: session.announce_port(),
    })
}

pub async fn h_get_listen_port(State(state): State<ApiState>) -> Result<impl IntoResponse> {
    Ok(listen_port_response(state.api.session()))
}

#[derive(Deserialize)]
pub struct SetListenPortRequest {
    port: NonZeroU16,
}

pub async fn h_set_listen_port(
    State(state): State<ApiState>,
    Json(req): Json<SetListenPortRequest>,
) -> Result<impl IntoResponse> {
    let session = state.api.session();
    session.set_listen_port(req.port).await?;
    Ok(listen_port_response(session))
}

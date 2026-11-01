use std::{
    cmp::Reverse,
    collections::HashMap,
    future,
    hash::Hasher,
    path::PathBuf,
    time::{
        Duration,
        Instant,
    },
};

use crate::{
    client::account::{
        ServerType,
        build_url,
    },
    ui::provider::tu_item::image_type::BACKDROP,
};
use anyhow::{
    Context,
    Result,
    anyhow,
    bail,
};
use arc_swap::ArcSwap;
use chrono::{
    DateTime,
    Utc,
};
use futures_util::{
    StreamExt,
    stream::FuturesUnordered,
};
use moka::future::Cache;
use once_cell::sync::Lazy;
use reqwest::{
    Client,
    Method,
    RequestBuilder,
    Response,
    header::{
        HeaderMap,
        HeaderValue,
    },
};
use serde::{
    Deserialize,
    Serialize,
    de::DeserializeOwned,
};
use serde_json::{
    Value,
    json,
};
use std::sync::Arc;
use tracing::warn;
use url::Url;
use uuid::Uuid;

use super::{
    Account,
    ReqClient,
    compat::{
        self,
        AuthStyle,
        Flavor,
        ServerProfile,
        fields,
    },
    error::UserFacingError,
    picture_source::PictureSource,
    structs::{
        ActivityLogs,
        Back,
        DeleteInfo,
        ExternalIdInfo,
        FilterList,
        ImageItem,
        ImageSearchResult,
        List,
        LoginResponse,
        Media,
        MediaSegmentList,
        MissingEpisodesList,
        PublicServerInfo,
        RemoteSearchInfo,
        ScheduledTask,
        ServerInfo,
        SimpleListItem,
        User,
    },
};
use crate::{
    CLIENT_ID,
    config::version,
    ui::{
        SETTINGS,
        jellyfin_cache_path,
        widgets::{
            filter_panel::FiltersList,
            single_grid::imp::ListType,
        },
    },
    utils::spawn_tokio_without_await,
};

pub static JELLYFIN_CLIENT: Lazy<JellyfinClient> = Lazy::new(JellyfinClient::default);
pub static DEVICE_ID: Lazy<String> = Lazy::new(|| {
    let uuid = SETTINGS.device_uuid();
    if uuid.is_empty() {
        let uuid = Uuid::new_v4().to_string();
        let _ = SETTINGS.set_device_uuid(&uuid);
        uuid
    } else {
        uuid
    }
});

const PROFILE: &str = include_str!("stream_profile.json");

static DEVICE_NAME: Lazy<String> = Lazy::new(|| {
    hostname::get()
        .unwrap_or("Unknown".into())
        .to_string_lossy()
        .to_string()
});

#[derive(PartialEq)]
pub enum BackType {
    Start,
    Stop,
    Back,
}

#[derive(Clone)]
pub struct Session {
    pub account: Account,
    pub url_headers: Option<(Url, HeaderMap)>,
    pub server_name_hash: String,
    /// Resolved compatibility profile for the active account.
    pub profile: ServerProfile,
}

impl Session {
    fn empty() -> Self {
        Self {
            account: Account::default(),
            url_headers: None,
            server_name_hash: String::new(),
            profile: ServerProfile::default(),
        }
    }
}

pub struct JellyfinClient {
    pub session: ArcSwap<Session>,
    pub semaphore: tokio::sync::Semaphore,
    pub client: Client,
    next_up_date_cache: Cache<NextUpDateKey, Option<DateTime<Utc>>>,
    /// Throttles `Sessions/Playing*` reports (see [`ReportGate`]).
    report_gate: tokio::sync::Mutex<ReportGate>,
    /// Per-host concurrency limit: one slow server must not starve the others.
    hosts: std::sync::Mutex<HashMap<String, Arc<tokio::sync::Semaphore>>>,
    /// Last time a failover was attempted, to avoid probing storms.
    failover_at: tokio::sync::Mutex<Option<Instant>>,
}

/// How often a network failure may trigger a line switch.
const FAILOVER_COOLDOWN: Duration = Duration::from_secs(15);

/// One line as the settings dialog needs it.
#[derive(Clone, Debug)]
pub struct LineStatus {
    pub url: String,
    pub label: String,
    pub current: bool,
    pub preferred: bool,
    pub latency_ms: Option<u64>,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum ReportKind {
    Start,
    Progress,
    Stop,
}

/// Playback progress reports are throttled and serialised here.
///
/// The UI fires a report on every 10 s tick, on every seek, on segment skips and
/// on stop, so the same position used to be sent several times in a row; a slow
/// or self-built server then saw bursts of duplicate sessions. Two rules keep it
/// honest without ever losing the important events:
///
/// * `Start`/`Stop` always go through (a repeated `Stop` for the same session is
///   dropped so leaving the page and closing the window does not report twice).
/// * `Progress` only goes when the position actually moved by at least
///   [`PROGRESS_MIN_DELTA_TICKS`], or [`PROGRESS_KEEPALIVE`] has passed since the
///   last one (keeps paused sessions alive without spamming).
///
/// Failures back off to the keepalive interval instead of retrying every tick.
#[derive(Default)]
struct ReportGate {
    key: String,
    kind: Option<ReportKind>,
    ticks: u64,
    sent: Option<Instant>,
    failures: u32,
}

/// 2 seconds in 100 ns ticks: below this a `Progress` report is a duplicate.
const PROGRESS_MIN_DELTA_TICKS: u64 = 20_000_000;

/// Even a frozen position is reported this often so the server keeps the session.
const PROGRESS_KEEPALIVE: Duration = Duration::from_secs(60);

impl ReportGate {
    fn should_send(&self, kind: ReportKind, key: &str, ticks: u64) -> bool {
        match kind {
            ReportKind::Start => true,
            // The same session may be stopped twice (leaving the page, then
            // closing the window); only the first one has to reach the server.
            ReportKind::Stop => !(self.kind == Some(ReportKind::Stop) && self.key == key),
            ReportKind::Progress => {
                if self.key != key {
                    return true;
                }
                let elapsed = self
                    .sent
                    .map(|sent| sent.elapsed())
                    .unwrap_or(PROGRESS_KEEPALIVE);
                if self.failures > 0 {
                    // Do not hammer a server that is rejecting the reports.
                    return elapsed >= PROGRESS_KEEPALIVE;
                }
                ticks.abs_diff(self.ticks) >= PROGRESS_MIN_DELTA_TICKS
                    || elapsed >= PROGRESS_KEEPALIVE
            }
        }
    }

    fn record(&mut self, kind: ReportKind, key: &str, ticks: u64, ok: bool) {
        self.key = key.to_string();
        self.kind = Some(kind);
        self.ticks = ticks;
        self.sent = Some(Instant::now());
        match kind {
            ReportKind::Progress if !ok => {
                self.failures = self.failures.saturating_add(1);
            }
            _ => self.failures = 0,
        }
    }
}

#[derive(Hash, PartialEq, Eq)]
struct NextUpDateKey {
    series_id: String,
    item_id: String,
}

/// Build the authorization headers for one dialect.
///
/// Two spellings exist in the wild: Jellyfin (and newer Emby) expect
/// `Authorization: MediaBrowser Client=…,Token=…`, older Emby builds only accept
/// `X-Emby-authorization: Emby …` plus `X-Emby-Token`. Which one a given server
/// wants is negotiated at runtime, not guessed from its type.
fn build_headers(
    user_id: Option<&str>, token: Option<&str>, style: AuthStyle,
) -> Result<HeaderMap> {
    build_headers_with(user_id, token, style, &DEVICE_ID)
}

/// Build authorization headers for one dialect.
///
/// Two spellings exist in the wild and they are not interchangeable:
///
/// * Emby (and most forks) read `X-Emby-Token`, optionally with a
///   `X-Emby-authorization` scheme line.
/// * Jellyfin reads `Authorization: MediaBrowser …`, `X-Emby-Token`, or the
///   legacy `X-Emby-Authorization` header.
///
/// The important part: a login request is unauthenticated, so a dialect that
/// succeeds at login says nothing about the dialect needed afterwards. Sending
/// `X-Emby-Token` **in every dialect** (and both scheme headers for `Auto`) is
/// harmless for the servers that ignore the extra header and is the only thing
/// that keeps self-built servers working.
fn build_headers_with(
    user_id: Option<&str>, token: Option<&str>, style: AuthStyle, device_id: &str,
) -> Result<HeaderMap> {
    let mut authorization = format!(
        "Client={},Device={},DeviceId={},Version={}",
        CLIENT_ID,
        *DEVICE_NAME,
        device_id,
        version()
    );
    if let Some(user_id) = user_id {
        authorization.push_str(&format!(",UserId={}", user_id));
    }

    let mut headers = HeaderMap::new();
    match style {
        AuthStyle::Emby => {
            headers.insert(
                "X-Emby-authorization",
                HeaderValue::from_str(&format!("Emby {authorization}"))?,
            );
        }
        AuthStyle::MediaBrowser => {
            let mut browser = authorization.clone();
            if let Some(token) = token {
                browser.push_str(&format!(",Token={token}"));
            }
            headers.insert(
                "Authorization",
                HeaderValue::from_str(&format!("MediaBrowser {browser}"))?,
            );
        }
        AuthStyle::Auto => {
            let mut browser = authorization.clone();
            if let Some(token) = token {
                browser.push_str(&format!(",Token={token}"));
            }
            headers.insert(
                "Authorization",
                HeaderValue::from_str(&format!("MediaBrowser {browser}"))?,
            );
            headers.insert(
                "X-Emby-authorization",
                HeaderValue::from_str(&format!("Emby {authorization}"))?,
            );
        }
    }

    // Every dialect gets the token header: Emby requires it, Jellyfin accepts it.
    if let Some(token) = token {
        headers.insert("X-Emby-Token", HeaderValue::from_str(token)?);
    }
    Ok(headers)
}

/// Legacy base URL rule, kept as the fallback when probing fails.
///
/// New code should go through [`compat::resolve_base`], which tries the address
/// as typed, `<address>/emby` and the stripped variant instead of assuming that
/// Emby always lives under `/emby/`.
fn build_base_url(url: Url, server_type: ServerType) -> Result<Url> {
    match server_type {
        ServerType::Emby => Ok(url.join("emby/")?),
        ServerType::Jellyfin => Ok(url),
    }
}

fn generate_hash(s: &str) -> String {
    let mut hasher = fnv::FnvHasher::default();
    hasher.write(s.as_bytes());
    format!("{:x}", hasher.finish())
}

impl Default for JellyfinClient {
    fn default() -> Self {
        Self {
            session: ArcSwap::from_pointee(Session::empty()),
            semaphore: tokio::sync::Semaphore::new(SETTINGS.threads() as usize),
            client: ReqClient::build(),
            next_up_date_cache: Cache::builder()
                .max_capacity(256)
                .time_to_live(Duration::from_hours(2))
                .support_invalidation_closures()
                .build(),
            report_gate: tokio::sync::Mutex::new(ReportGate::default()),
            hosts: std::sync::Mutex::new(HashMap::new()),
            failover_at: tokio::sync::Mutex::new(None),
        }
    }
}

impl JellyfinClient {
    pub fn session(&self) -> arc_swap::Guard<Arc<Session>> {
        self.session.load()
    }

    fn server_type(&self) -> ServerType {
        let session = self.session.load();
        // Nothing probed yet (offline start, probe failed): trust the picker.
        if !session.profile.caps.probed {
            return session.account.server_type.unwrap_or_default();
        }
        // Otherwise the detected flavour wins; the picker was only a hint.
        match session.profile.flavor {
            Flavor::Jellyfin => ServerType::Jellyfin,
            Flavor::Emby | Flavor::FnOs | Flavor::Ugreen => ServerType::Emby,
            Flavor::Custom => session.account.server_type.unwrap_or_default(),
        }
    }

    pub fn flavor(&self) -> Flavor {
        self.session.load().profile.flavor
    }

    pub fn caps(&self) -> compat::Caps {
        let session = self.session.load();
        let caps = session.profile.caps;
        if caps.probed {
            caps
        } else {
            // Fall back to flavour defaults so a failed probe cannot switch a
            // feature off that used to work.
            compat::Caps::with_flavor_defaults(session.profile.flavor)
        }
    }

    pub fn is_jellyfin(&self) -> bool {
        matches!(self.server_type(), ServerType::Jellyfin)
    }

    /// API root currently in use.
    pub fn current_base(&self) -> Option<String> {
        self.session()
            .url_headers
            .as_ref()
            .map(|(url, _)| url.to_string())
    }

    /// Configured lines with the current/perferred markers, without latencies.
    pub fn line_statuses(&self) -> Vec<LineStatus> {
        let session = self.session();
        let current = session.url_headers.as_ref().map(|(url, _)| url.to_string());
        session
            .profile
            .lines
            .iter()
            .map(|line| LineStatus {
                url: line.url.clone(),
                label: line.label.clone(),
                current: Some(&line.url) == current.as_ref(),
                preferred: session.profile.preferred.as_deref() == Some(line.url.as_str()),
                latency_ms: None,
            })
            .collect()
    }

    /// Measure every configured line in parallel (`System/Info/Public` round trip).
    pub async fn measure_lines(&self) -> Vec<LineStatus> {
        let mut statuses = self.line_statuses();
        let urls: Vec<String> = statuses.iter().map(|line| line.url.clone()).collect();

        let latencies = futures_util::future::join_all(
            urls.iter()
                .map(|url| compat::measure(&self.client, url))
                .collect::<Vec<_>>(),
        )
        .await;

        for (status, latency) in statuses.iter_mut().zip(latencies) {
            status.latency_ms = latency;
        }
        statuses
    }

    /// Re-read `System/Ext/ServerDomains` and merge it with the known addresses.
    pub async fn refresh_lines(&self) -> Vec<LineStatus> {
        let session = Session::clone(&self.session());
        let Some((base, headers)) = session.url_headers.clone() else {
            return Vec::new();
        };

        let mut lines = Vec::new();
        if let Ok(configured) = session.account.url() {
            push_line(&mut lines, &configured);
        }
        push_line(&mut lines, &base);
        for line in self.fetch_server_domains(&base, &headers).await {
            if let Ok(url) = Url::parse(&line.url) {
                push_line(&mut lines, &url);
            }
        }

        self.store_lines(lines);
        self.line_statuses()
    }

    /// Switch to a line the user picked. Verifies it first, so a broken line can
    /// never replace a working one.
    pub async fn switch_line(&self, url: &str) -> Result<()> {
        let Ok(base) = Url::parse(url) else {
            bail!("invalid server line: {url}");
        };
        if compat::measure(&self.client, url).await.is_none() {
            bail!("server line did not answer: {url}");
        }
        self.switch_base(base, true).await
    }

    /// Drop the manual pin and move to whichever line answers fastest.
    pub async fn use_automatic_line(&self) -> Result<()> {
        let statuses = self.measure_lines().await;
        let best = statuses
            .iter()
            .filter(|line| line.latency_ms.is_some())
            .min_by_key(|line| line.latency_ms)
            .cloned();

        match best {
            Some(best) if !best.current => {
                let base = Url::parse(&best.url)?;
                self.switch_base(base, false).await?;
            }
            _ => {
                // Nothing better (or already on the best one): just clear the pin.
                let mut session = Session::clone(&self.session());
                session.profile.preferred = None;
                self.store_session(session);
            }
        }
        Ok(())
    }

    async fn switch_base(&self, base: Url, pin: bool) -> Result<()> {
        let mut session = Session::clone(&self.session());
        let headers = build_headers(
            Some(&session.account.user_id),
            Some(&session.account.access_token),
            session.profile.auth,
        )?;
        session.profile.base = base.to_string();
        session.profile.preferred = pin.then(|| base.to_string());
        session.url_headers = Some((base, headers));
        let base = session.profile.base.clone();
        self.store_session(session);
        tracing::info!("switched server line to {base}");
        Ok(())
    }

    /// Move to another configured line after a network failure. Returns whether
    /// the session changed and the request is worth retrying.
    async fn try_failover(&self) -> bool {
        {
            let mut last = self.failover_at.lock().await;
            match *last {
                Some(at) if at.elapsed() < FAILOVER_COOLDOWN => return false,
                _ => *last = Some(Instant::now()),
            }
        }

        let session = Session::clone(&self.session());
        let current = session.url_headers.as_ref().map(|(url, _)| url.to_string());
        let candidates: Vec<String> = session
            .profile
            .lines
            .iter()
            .map(|line| line.url.clone())
            .filter(|url| Some(url) != current.as_ref())
            .collect();

        for url in candidates {
            if compat::measure(&self.client, &url).await.is_none() {
                continue;
            }
            if let Ok(base) = Url::parse(&url)
                && self.switch_base(base, false).await.is_ok()
            {
                return true;
            }
        }
        false
    }

    async fn fetch_server_domains(&self, base: &Url, headers: &HeaderMap) -> Vec<compat::Line> {
        let Ok(url) = base.join("System/Ext/ServerDomains") else {
            return Vec::new();
        };
        let Ok(res) = self
            .client
            .get(url)
            .headers(headers.clone())
            .timeout(compat::PROBE_TIMEOUT)
            .send()
            .await
        else {
            return Vec::new();
        };
        if !res.status().is_success() {
            return Vec::new();
        }
        let Ok(value) = res.json::<serde_json::Value>().await else {
            return Vec::new();
        };
        compat::parse_server_domains(base, &value)
    }

    fn store_lines(&self, lines: Vec<compat::Line>) {
        let mut session = Session::clone(&self.session());
        session.profile.lines = lines;
        self.store_session(session);
    }

    fn store_session(&self, session: Session) {
        let profile = session.profile.clone();
        let servername = session.account.servername.clone();
        self.session.store(Arc::new(session));
        let _ = SETTINGS.update_account_profile(&servername, Some(profile));
    }

    pub async fn init(&self, account: &Account) -> Result<(), Box<dyn std::error::Error>> {
        let hint = account.server_type.unwrap_or_default();
        let raw_url = account.url()?.to_string();
        let mut profile = account.profile.clone().unwrap_or_default();

        // 1) Resolve the API root. Reuse the cached value; otherwise probe every
        //    candidate and, if nothing answers, fall back to the historical rule
        //    so a setup that worked before keeps working.
        let url = match profile.base_url() {
            Some(base) => base,
            None => match compat::resolve_base(&self.client, &raw_url, hint).await {
                Ok(resolved) => {
                    if !profile.caps.probed {
                        profile.flavor = compat::detect_flavor(&resolved.info);
                    }
                    profile.base = resolved.base.to_string();
                    resolved.base
                }
                Err(e) => {
                    warn!("probe for {raw_url} failed ({e}); using legacy base url");
                    if !profile.caps.probed {
                        profile.flavor = match hint {
                            ServerType::Jellyfin => Flavor::Jellyfin,
                            ServerType::Emby => Flavor::Emby,
                        };
                    }
                    build_base_url(account.url()?, hint)?
                }
            },
        };

        // 2) Authorization dialect (negotiated further on 401, see `request`).
        let headers = build_headers(
            Some(&account.user_id),
            Some(&account.access_token),
            profile.auth,
        )?;

        // 3) Capabilities are probed once and cached on the account.
        let mut profile_changed = false;
        if !profile.caps.probed {
            profile.caps = compat::probe_caps(
                &self.client,
                &url,
                &headers,
                Some(&account.user_id),
                profile.flavor,
            )
            .await;
            profile_changed = true;
        }

        // 4) Discover alternate lines once. They are cached too, so a switch in
        //    the settings dialog survives restarts.
        if profile.lines.is_empty() {
            let mut lines = Vec::new();
            push_line(&mut lines, &url);
            if let Ok(configured) = account.url() {
                push_line(&mut lines, &configured);
            }
            if profile.caps.server_domains {
                for line in self.fetch_server_domains(&url, &headers).await {
                    push_line(&mut lines, &Url::parse(&line.url).unwrap_or(url.clone()));
                }
            }
            if !lines.is_empty() {
                profile.lines = lines;
                profile_changed = true;
            }
        }

        if profile_changed {
            let _ = SETTINGS.update_account_profile(&account.servername, Some(profile.clone()));
        }

        self.session.store(Arc::new(Session {
            account: account.clone(),
            profile,
            url_headers: Some((url, headers)),
            server_name_hash: generate_hash(&account.servername),
        }));
        self.next_up_date_cache.invalidate_all();

        crate::ui::provider::set_admin(false);
        let servername = account.servername.clone();
        spawn_tokio_without_await(async move {
            match JELLYFIN_CLIENT.get_current_user().await {
                Ok(r) => {
                    if r.policy.is_administrator {
                        crate::ui::provider::set_admin(true);
                    }
                }
                Err(e) => {
                    let message = e.to_string();
                    if message.contains("401") {
                        // A stored token that is rejected is the single most
                        // confusing state: everything 401s while the account
                        // still looks configured.
                        warn!(
                            "stored credential for {} was rejected (401) - the account needs to \
                             be re-logged in; keeping the configuration",
                            servername
                        );
                    } else {
                        warn!("Failed to authenticate as admin: {message}");
                    }
                }
            }
        });
        Ok(())
    }

    pub async fn request<T>(&self, path: &str, params: &[(&str, &str)]) -> Result<T>
    where
        T: for<'de> Deserialize<'de> + Send + 'static,
    {
        let res = self.get_with_auth_retry(path, params).await?;

        let res = match res.error_for_status() {
            Ok(r) => r,
            Err(e) => {
                let Some(status) = e.status() else {
                    bail!("Failed to get status");
                };
                bail!("{}", status);
            }
        };

        let res_bytes = res.bytes().await?;
        match serde_json::from_slice(&res_bytes) {
            Ok(json) => Ok(json),
            Err(e) => Err(anyhow!(
                "Request Path: {}\nFailed parsing response to json {}: {}",
                path,
                e,
                String::from_utf8_lossy(&res_bytes)
            )),
        }
    }

    /// GET that retries once with the alternate authorization dialect when the
    /// server answers 401. Servers differ: older Emby forks only accept
    /// `X-Emby-authorization`, Jellyfin only accepts `Authorization: MediaBrowser`.
    /// Whichever spelling works is remembered on the session and the account.
    async fn get_with_auth_retry(&self, path: &str, params: &[(&str, &str)]) -> Result<Response> {
        let mut res = self.get_once(path, params).await?;

        if res.status() == reqwest::StatusCode::UNAUTHORIZED {
            let current = self.session().profile.auth;
            let alternate = current.alternate().unwrap_or(AuthStyle::Emby);
            tracing::debug!("401 for {path}, retrying with {alternate:?} authorization");
            let retried = self.retry_with_style(path, params, alternate).await?;
            if retried.status().is_success() {
                self.remember_auth_style(alternate);
            }
            res = retried;

            // Still rejected (a profile recorded a dialect this server does not
            // accept): fall back to sending every dialect at once.
            if res.status() == reqwest::StatusCode::UNAUTHORIZED && current != AuthStyle::Auto {
                tracing::debug!("401 for {path}, retrying with every authorization dialect");
                let retried = self.retry_with_style(path, params, AuthStyle::Auto).await?;
                if retried.status().is_success() {
                    self.remember_auth_style(AuthStyle::Auto);
                }
                res = retried;
            }
        }

        if res.status().is_success() || !compat::ApiFailure::of(res.status()).is_unsupported() {
            return Ok(res);
        }

        // Self-built servers sometimes reject parameters they do not implement
        // instead of ignoring them (`Fields`, grouping flags, …). Retry with
        // progressively smaller parameter sets before surfacing the failure.
        for stage in compat::DEGRADE_STAGES {
            let reduced = compat::strip(params, stage);
            if reduced.len() == params.len() {
                continue;
            }
            tracing::debug!(
                "{path} answered {}; retrying without {stage:?}",
                res.status()
            );
            res = self.get_once(path, &reduced).await?;
            if res.status().is_success() || !compat::ApiFailure::of(res.status()).is_unsupported() {
                break;
            }
        }

        Ok(res)
    }

    async fn get_once(&self, path: &str, params: &[(&str, &str)]) -> Result<Response> {
        let request = self.prepare_request(Method::GET, path, params)?;
        match self.send_request(request).await {
            Ok(res) => Ok(res),
            Err(e) => {
                // A dead line should not make the whole UI fail: move to another
                // configured line once and retry the same request.
                if !self.try_failover().await {
                    return Err(e);
                }
                let request = self.prepare_request(Method::GET, path, params)?;
                self.send_request(request).await
            }
        }
    }

    async fn retry_with_style(
        &self, path: &str, params: &[(&str, &str)], style: AuthStyle,
    ) -> Result<Response> {
        let session = self.session();
        let (url, _) = session
            .url_headers
            .as_ref()
            .context("Client not initialized")?;
        let headers = build_headers(
            Some(&session.account.user_id),
            Some(&session.account.access_token),
            style,
        )?;
        let request = self
            .client
            .request(Method::GET, url.join(path)?)
            .query(params)
            .headers(headers);
        self.send_request(request).await
    }

    fn remember_auth_style(&self, style: AuthStyle) {
        let mut session = Session::clone(&self.session());
        if session.profile.auth == style {
            return;
        }
        session.profile.auth = style;
        let profile = session.profile.clone();
        let servername = session.account.servername.clone();
        self.session.store(Arc::new(session));
        let _ = SETTINGS.update_account_profile(&servername, Some(profile));
    }

    pub async fn request_picture(&self, path: &str, params: &[(&str, &str)]) -> Result<Response> {
        let request = self.prepare_request(Method::GET, path, params)?;
        let res = self.send_request(request).await?;
        Ok(res)
    }

    pub async fn delete(&self, path: &str, params: &[(&str, &str)]) -> Result<Response> {
        let request = self.prepare_request(Method::DELETE, path, params)?;
        self.send_request(request).await
    }

    pub async fn post<B>(&self, path: &str, params: &[(&str, &str)], body: B) -> Result<Response>
    where
        B: Serialize,
    {
        let request = self
            .prepare_request(Method::POST, path, params)?
            .json(&body);
        self.send_request(request).await
    }

    pub async fn post_raw<B>(&self, path: &str, body: B, content_type: &str) -> Result<Response>
    where
        reqwest::Body: From<B>,
    {
        let request = self
            .prepare_request_headers(Method::POST, path, &[], content_type)?
            .body(body);
        self.send_request(request).await
    }

    pub async fn post_json<B, T>(
        &self, path: &str, params: &[(&str, &str)], body: B,
    ) -> Result<T, anyhow::Error>
    where
        B: Serialize,
        T: DeserializeOwned,
    {
        let response = self.post(path, params, body).await?.error_for_status()?;
        let parsed = response.json::<T>().await?;
        Ok(parsed)
    }

    fn prepare_request(
        &self, method: Method, path: &str, params: &[(&str, &str)],
    ) -> Result<RequestBuilder> {
        let s = self.session();
        let (url, headers) = s.url_headers.as_ref().context("Client not initialized")?;
        Ok(self
            .client
            .request(method, url.join(path)?)
            .query(params)
            .headers(headers.clone()))
    }

    fn prepare_request_headers(
        &self, method: Method, path: &str, params: &[(&str, &str)], content_type: &str,
    ) -> Result<RequestBuilder> {
        let s = self.session();
        let (url, headers) = s.url_headers.as_ref().context("Client not initialized")?;
        let mut headers = headers.clone();
        headers.insert(
            reqwest::header::CONTENT_TYPE,
            HeaderValue::from_str(content_type)?,
        );
        Ok(self
            .client
            .request(method, url.join(path)?)
            .query(params)
            .headers(headers))
    }

    async fn send_request(&self, request: RequestBuilder) -> Result<Response> {
        // Limit concurrency per host on top of the global budget: home pages fan
        // out to many endpoints at once and a single low powered NAS would
        // otherwise get the whole burst.
        let host = request
            .try_clone()
            .and_then(|builder| builder.build().ok())
            .and_then(|request| request.url().host_str().map(str::to_string));
        let host_permit = match host {
            Some(host) => Some(self.host_semaphore(&host).acquire_owned().await?),
            None => None,
        };

        let _global = self.semaphore.acquire().await?;
        let res = request.send().await;
        drop(host_permit);

        res.map_err(|e| anyhow!(e.to_user_facing()))
    }

    fn host_semaphore(&self, host: &str) -> Arc<tokio::sync::Semaphore> {
        let limit = (SETTINGS.threads().max(1) as usize).max(2);
        let mut hosts = self.hosts.lock().unwrap_or_else(|e| e.into_inner());
        hosts
            .entry(host.to_string())
            .or_insert_with(|| Arc::new(tokio::sync::Semaphore::new(limit)))
            .clone()
    }

    pub async fn get_current_user(&self) -> Result<User> {
        let s = self.session();
        let path = format!("Users/{}", s.account.user_id);
        let res = self.request(&path, &[]).await?;
        Ok(res)
    }

    pub async fn login(
        &self, server: &str, port: &str, server_type: ServerType, username: &str, password: &str,
    ) -> Result<LoginResponse> {
        Ok(self
            .login_resolved(server, port, server_type, username, password)
            .await?
            .0)
    }

    /// Like [`Self::login`], but also returns the discovered [`ServerProfile`]
    /// (resolved base URL, flavour, authorization style, capabilities) so the
    /// caller can persist it on the account and skip probing on next start.
    ///
    /// Every candidate root and both authorization dialects are tried, because
    /// stock Emby, stock Jellyfin and self-built servers disagree on all of them.
    pub async fn login_resolved(
        &self, server: &str, port: &str, hint: ServerType, username: &str, password: &str,
    ) -> Result<(LoginResponse, ServerProfile)> {
        let raw = build_url(server, port)?.to_string();

        // Probe once to learn the flavour; probing also yields the base that
        // answers like an Emby server, which is then tried first.
        let mut bases: Vec<(Url, Flavor)> = Vec::new();
        match compat::resolve_base(&self.client, &raw, hint).await {
            Ok(resolved) => {
                let flavor = compat::detect_flavor(&resolved.info);
                bases.push((resolved.base, flavor));
            }
            Err(e) => warn!("probe for {raw} failed ({e}); trying candidates blindly"),
        }
        for base in compat::candidates(&raw, hint) {
            if !bases.iter().any(|(known, _)| known == &base) {
                bases.push((base, Flavor::Emby));
            }
        }

        let mut last_error = String::from("no candidate answered");
        for (base, flavor) in bases {
            for style in AuthStyle::Auto.attempts() {
                let Ok(headers) = build_headers(None, None, style) else {
                    continue;
                };
                let Ok(url) = base.join("Users/AuthenticateByName") else {
                    continue;
                };
                let request = self
                    .client
                    .request(Method::POST, url)
                    .headers(headers)
                    .json(&json!({
                        "Username": username,
                        "Pw": password
                    }));

                let res = match self.send_request(request).await {
                    Ok(res) => res,
                    Err(e) => {
                        last_error = format!("{base}: {e}");
                        continue;
                    }
                };

                if !res.status().is_success() {
                    last_error = format!("{base} answered {} ({style:?})", res.status());
                    continue;
                }

                let login: LoginResponse = res.json().await.context("parse login response")?;

                // The login call itself is unauthenticated, so the dialect that
                // got us a token is not necessarily the one that accepts it.
                // Prove the credential before storing it.
                let auth = self
                    .probe_auth_style(&base, &login.user.id, &login.access_token)
                    .await
                    .unwrap_or(AuthStyle::Auto);
                tracing::info!("authorization dialect for {base}: {auth:?}");

                let auth_headers =
                    build_headers(Some(&login.user.id), Some(&login.access_token), auth)?;
                let caps = compat::probe_caps(
                    &self.client,
                    &base,
                    &auth_headers,
                    Some(&login.user.id),
                    flavor,
                )
                .await;
                let profile = ServerProfile {
                    base: base.to_string(),
                    flavor,
                    auth,
                    caps,
                    lines: vec![compat::Line {
                        label: compat::line_label(&base),
                        url: base.to_string(),
                    }],
                    preferred: None,
                };
                tracing::info!("logged in to {base} (flavour {flavor:?}, auth {style:?})");
                return Ok((login, profile));
            }
        }

        bail!("Login failed: {last_error}")
    }

    /// Find the dialect this server actually accepts for authenticated calls.
    ///
    /// `Auto` (which sends every header) is tried first: when it works there is
    /// no need to know which single header the server read.
    async fn probe_auth_style(&self, base: &Url, user_id: &str, token: &str) -> Option<AuthStyle> {
        let Ok(url) = base.join(&format!("Users/{user_id}")) else {
            return None;
        };

        for style in [AuthStyle::Auto, AuthStyle::Emby, AuthStyle::MediaBrowser] {
            let Ok(headers) = build_headers(Some(user_id), Some(token), style) else {
                continue;
            };
            let res = self
                .client
                .get(url.clone())
                .headers(headers)
                .timeout(compat::PROBE_TIMEOUT)
                .send()
                .await;
            match res {
                Ok(res) if res.status().is_success() => return Some(style),
                Ok(res) => {
                    tracing::debug!("auth probe with {style:?} answered {}", res.status());
                }
                Err(e) => {
                    tracing::debug!("auth probe with {style:?} failed: {e}");
                }
            }
        }
        None
    }

    pub fn get_item_direct_play_url(
        &self, container: &str, item_id: &str, media_source_id: &str, play_session_id: Option<&str>,
    ) -> Result<String> {
        let s = self.session();
        let (url, _) = s.url_headers.as_ref().context("Client not initialized")?;
        let path = format!("Videos/{}/stream.{}", item_id, container);
        let mut url = url.join(&path).context("Failed to build item stream URL")?;
        let mut query_pairs = url.query_pairs_mut();
        query_pairs
            .append_pair("Static", "true")
            .append_pair("deviceId", &DEVICE_ID)
            .append_pair("MediaSourceId", media_source_id);
        if let Some(play_session_id) = play_session_id {
            query_pairs.append_pair("PlaySessionId", play_session_id);
        }
        // Emby-family servers read `api_key`, Jellyfin accepts both spellings.
        if s.profile.flavor.is_emby_family() || matches!(self.server_type(), ServerType::Emby) {
            query_pairs.append_pair("api_key", &s.account.access_token);
        } else {
            query_pairs.append_pair("ApiKey", &s.account.access_token);
        }
        drop(query_pairs);
        Ok(url.to_string())
    }

    pub async fn search(
        &self, query: &str, filter: &[&str], start_index: &str, filters_list: &FiltersList,
    ) -> Result<List> {
        let s = self.session();
        let filter_str = filter.join(",");
        let path = format!("Users/{}/Items", s.account.user_id);
        let mut params = vec![
            ("Fields", fields::LIST),
            ("IncludeItemTypes", &filter_str),
            ("IncludeSearchTypes", &filter_str),
            ("StartIndex", start_index),
            ("SortBy", "SortName"),
            ("SortOrder", "Ascending"),
            ("EnableImageTypes", "Primary,Backdrop,Thumb,Banner"),
            ("ImageTypeLimit", "1"),
            ("Recursive", "true"),
            ("SearchTerm", query),
            ("GroupProgramsBySeries", "true"),
            ("Limit", "50"),
        ];
        let kv = filters_list.to_kv();
        kv.iter().for_each(|(k, v)| {
            params.push((k.as_str(), v.as_str()));
        });
        self.request(&path, &params).await
    }

    pub async fn get_episodes(&self, id: &str, season_id: &str, start_index: u32) -> Result<List> {
        let s = self.session();
        let path = format!("Shows/{id}/Episodes");
        let params = [
            ("Fields", fields::SYNC),
            ("Limit", "50"),
            ("StartIndex", &start_index.to_string()),
            ("ImageTypeLimit", "1"),
            ("SeasonId", season_id),
            ("UserId", &s.account.user_id),
        ];
        self.request(&path, &params).await
    }

    pub async fn get_episodes_all(&self, id: &str, season_id: &str) -> Result<List> {
        let s = self.session();
        let path = format!("Shows/{id}/Episodes");
        let params = [
            ("Fields", fields::SYNC),
            ("ImageTypeLimit", "1"),
            ("SeasonId", season_id),
            ("UserId", &s.account.user_id),
        ];
        self.request(&path, &params).await
    }

    pub async fn get_item_info(&self, id: &str) -> Result<SimpleListItem> {
        let s = self.session();
        let path = format!("Users/{}/Items/{}", s.account.user_id, id);
        let params = [("Fields", "ShareLevel")];
        self.request(&path, &params).await
    }

    pub async fn get_edit_info(&self, id: &str) -> Result<Value> {
        let s = self.session();
        let path = format!("Users/{}/Items/{}", s.account.user_id, id);
        let params = [("Fields", "ChannelMappingInfo")];
        self.request(&path, &params).await
    }

    pub async fn post_item(&self, id: &str, body: Value) -> Result<Response> {
        let path = format!("Items/{id}");
        self.post(&path, &[], body).await
    }

    pub async fn get_resume(&self, limit: u32) -> Result<List> {
        let s = self.session();
        let path = format!("Users/{}/Items/Resume", s.account.user_id);
        let params = [
            ("Recursive", "true"),
            ("Fields", fields::RESUME_RATING),
            ("EnableImageTypes", "Primary,Backdrop,Thumb,Banner"),
            ("ImageTypeLimit", "1"),
            ("MediaTypes", "Video"),
            ("Limit", &limit.to_string()),
        ];
        self.request(&path, &params).await
    }

    pub fn next_up_date_cutoff(&self) -> String {
        (chrono::Utc::now() - chrono::Duration::days(365))
            .to_rfc3339_opts(chrono::SecondsFormat::Millis, true)
    }

    pub async fn get_next_up(&self, limit: u32, next_up_date_cutoff: &str) -> Result<List> {
        if !self.is_jellyfin() {
            bail!("Next up is not supported on Emby");
        }
        let s = self.session();
        let limit = limit.to_string();
        let params = [
            ("Limit", limit.as_str()),
            (
                "Fields",
                "PrimaryImageAspectRatio,DateCreated,Path,MediaSourceCount",
            ),
            ("UserId", &s.account.user_id),
            ("ImageTypeLimit", "1"),
            ("EnableImageTypes", "Primary,Backdrop,Banner,Thumb"),
            ("EnableTotalRecordCount", "false"),
            ("DisableFirstEpisode", "true"),
            ("NextUpDateCutoff", next_up_date_cutoff),
            ("EnableResumable", "false"),
            ("EnableRewatching", "false"),
        ];
        self.request("Shows/NextUp", &params).await
    }

    /// Get next up and resume items, merge them and sort by last played date in client side.
    pub async fn get_next_up_merged(&self, limit: u32, next_up_date_cutoff: &str) -> Result<List> {
        if !self.is_jellyfin() {
            bail!("Next up is not supported on Emby");
        }
        let (resume, next_up) = tokio::try_join!(
            self.get_resume(limit),
            self.get_next_up(limit, next_up_date_cutoff)
        )?;

        let date_futures = next_up
            .items
            .iter()
            .map(|item| async move { (item.id.clone(), self.get_next_up_date(item).await) })
            .collect::<FuturesUnordered<_>>();

        let next_up_dates = date_futures
            .filter_map(|(id, date)| future::ready(date.map(|d| (id, d))))
            .collect::<HashMap<_, _>>()
            .await;

        let mut items = resume.items;
        items.extend(next_up.items);
        items.sort_by_key(|item| {
            Reverse(
                next_up_dates
                    .get(&item.id)
                    .copied()
                    .or_else(|| {
                        item.user_data
                            .as_ref()
                            .and_then(|user_data| user_data.last_played_date)
                    })
                    .unwrap_or(DateTime::<Utc>::MIN_UTC),
            )
        });
        items.truncate(limit as usize);

        Ok(List {
            total_record_count: items.len() as u32,
            items,
        })
    }

    async fn get_next_up_date(&self, item: &SimpleListItem) -> Option<DateTime<Utc>> {
        let series_id = item.series_id.as_ref()?;
        let user_id = &self.session().account.user_id;
        let item_id = &item.id;
        let key = NextUpDateKey {
            series_id: series_id.to_owned(),
            item_id: item_id.to_owned(),
        };
        self.next_up_date_cache
            .get_with(key, async move {
                let path = format!("Shows/{series_id}/Episodes");
                let params = [
                    ("UserId", user_id.as_str()),
                    ("AdjacentTo", item_id.as_str()),
                    ("Limit", "1"),
                ];
                let list: Result<List> = self.request(&path, &params).await;
                list.inspect_err(|e| {
                    warn!("Failed to get next up last played time: {}", e);
                })
                .ok()
                .and_then(|list| {
                    list.items.first().and_then(|episode| {
                        episode
                            .user_data
                            .as_ref()
                            .and_then(|user_data| user_data.last_played_date)
                    })
                })
            })
            .await
    }

    fn invalidate_next_up_date(&self, series_id: String) {
        let _ = self
            .next_up_date_cache
            .invalidate_entries_if(move |key, _| key.series_id == series_id);
    }

    pub async fn get_image_items(&self, id: &str) -> Result<Vec<ImageItem>> {
        let path = format!("Items/{id}/Images");
        self.request(&path, &[]).await
    }

    pub async fn get_image(&self, source: PictureSource) -> Result<PathBuf> {
        let mut path = jellyfin_cache_path().await;
        path.push(source.cache_key());

        if tokio::fs::metadata(&path).await.is_ok_and(|m| m.is_file()) {
            return Ok(path);
        }

        let (request_path, max_height, max_width, tag) = match source {
            PictureSource::Item {
                id,
                tag,
                image_type,
                image_index,
            } => {
                let mut request_path = format!("Items/{id}/Images/{image_type}");
                if let Some(image_index) = image_index {
                    request_path.push_str(&format!("/{image_index}"));
                }
                let (max_height, max_width) = if image_type == BACKDROP {
                    ("800", "1280")
                } else {
                    ("300", "300")
                };
                (request_path, max_height, max_width, tag)
            }
            PictureSource::User { id, tag } => {
                (format!("Users/{id}/Images/Primary"), "50", "50", tag)
            }
            PictureSource::Url { .. } => unreachable!(),
        };

        let params = [
            ("maxHeight", max_height),
            ("maxWidth", max_width),
            ("tag", tag.as_str()),
        ];
        let response = self.request_picture(&request_path, &params).await?;
        if response.status() == reqwest::StatusCode::NOT_FOUND {
            bail!("Image not found");
        }
        let response = response.error_for_status()?;
        let bytes = response.bytes().await?;
        if bytes.is_empty() {
            bail!("Image is empty");
        }
        tokio::fs::write(&path, bytes).await?;
        Ok(path)
    }

    // Only support base64 encoded images
    pub async fn post_image<B>(
        &self, id: &str, image_type: &str, bytes: B, content_type: &str,
    ) -> Result<Response>
    where
        reqwest::Body: From<B>,
    {
        let path = format!("Items/{id}/Images/{image_type}");
        self.post_raw(&path, bytes, content_type)
            .await?
            .error_for_status()
            .map_err(|e| e.into())
    }

    pub async fn post_image_url(
        &self, id: &str, image_type: &str, image_index: u8, url: &str,
    ) -> Result<Response> {
        let path = format!("Items/{id}/Images/{image_index}/{image_type}");
        let body = json!({ "Url": url });
        self.post(&path, &[], body).await
    }

    pub async fn delete_image(
        &self, id: &str, image_type: &str, image_index: Option<u8>,
    ) -> Result<Response> {
        let mut path = format!("Items/{id}/Images/{image_type}");
        if let Some(image_index) = image_index {
            path.push_str(&format!("/{image_index}"));
        }
        path.push_str("/Delete");
        self.post(&path, &[], json!({})).await
    }

    pub async fn get_artist_albums(&self, id: &str, artist_id: &str) -> Result<List> {
        let s = self.session();
        let path = format!("Users/{}/Items", s.account.user_id);
        let params = [
            ("IncludeItemTypes", "MusicAlbum"),
            ("Recursive", "true"),
            ("ImageTypeLimit", "1"),
            ("Limit", "12"),
            ("SortBy", "ProductionYear,SortName"),
            ("EnableImageTypes", "Primary,Backdrop,Thumb,Banner"),
            ("SortOrder", "Descending"),
            ("Fields", fields::CORE_YEAR),
            ("AlbumArtistIds", artist_id),
            ("ExcludeItemIds", id),
        ];
        self.request(&path, &params).await
    }

    pub async fn get_shows_next_up(&self, series_id: &str) -> Result<List> {
        let s = self.session();
        let path = "Shows/NextUp".to_string();
        let params = [
            ("Fields", fields::CORE),
            ("Limit", "1"),
            ("ImageTypeLimit", "1"),
            ("SeriesId", series_id),
            ("UserId", &s.account.user_id),
        ];
        self.request(&path, &params).await
    }

    pub async fn get_playbackinfo(
        &self, id: &str, sub_stream_index: Option<i64>, media_source_id: Option<String>,
        is_playback: bool,
    ) -> Result<Media> {
        self.get_playbackinfo_inner(
            id,
            sub_stream_index,
            media_source_id.as_deref(),
            None,
            is_playback,
            false,
        )
        .await
    }

    pub async fn get_fallback_playbackinfo(
        &self, id: &str, media_source_id: &str, live_stream_id: Option<&str>,
    ) -> Result<Media> {
        self.get_playbackinfo_inner(id, None, Some(media_source_id), live_stream_id, true, true)
            .await
    }

    async fn get_playbackinfo_inner(
        &self, id: &str, sub_stream_index: Option<i64>, media_source_id: Option<&str>,
        live_stream_id: Option<&str>, is_playback: bool, disable_direct_play: bool,
    ) -> Result<Media> {
        let s = self.session();
        let path = format!("Items/{id}/PlaybackInfo");
        let subtitle_stream_index = sub_stream_index.map(|s| s.to_string()).unwrap_or_default();
        let is_playback = is_playback.to_string();
        let mut params = vec![
            ("StartTimeTicks", "0"),
            ("UserId", &s.account.user_id),
            ("AutoOpenLiveStream", "true"),
            ("IsPlayback", &is_playback),
            ("MediaSourceId", media_source_id.unwrap_or_default()),
            ("SubtitleStreamIndex", &subtitle_stream_index),
        ];
        if let Some(live_stream_id) = live_stream_id {
            params.push(("LiveStreamId", live_stream_id));
        }
        if disable_direct_play {
            params.push(("EnableDirectPlay", "false"));
        }
        let profile: Value = serde_json::from_str(PROFILE).expect("Failed to parse profile");
        self.post_json(&path, &params, profile).await
    }

    pub async fn get_skippable_segments(&self, id: &str) -> Result<MediaSegmentList> {
        // Capability, not server type: Emby has no MediaSegments endpoint, but a
        // fork may, and a Jellyfin build may have it disabled. Returning an empty
        // list keeps the player overlay working instead of failing it.
        if !self.caps().segments {
            return Ok(MediaSegmentList::default());
        }
        let path = format!("MediaSegments/{id}");
        let params = [
            ("includeSegmentTypes", "Intro"),
            ("includeSegmentTypes", "Outro"),
        ];
        self.request(&path, &params).await
    }

    pub async fn scan(&self, id: &str) -> Result<Response> {
        let path = format!("Items/{id}/Refresh");
        let params = [
            ("Recursive", "true"),
            ("ImageRefreshMode", "Default"),
            ("MetadataRefreshMode", "Default"),
            ("ReplaceAllImages", "false"),
            ("ReplaceAllMetadata", "false"),
        ];
        self.post(&path, &params, json!({})).await
    }

    pub async fn fullscan(
        &self, id: &str, replace_images: &str, replace_metadata: &str,
    ) -> Result<Response> {
        let path = format!("Items/{id}/Refresh");
        let params = [
            ("Recursive", "true"),
            ("ImageRefreshMode", "FullRefresh"),
            ("MetadataRefreshMode", "FullRefresh"),
            ("ReplaceAllImages", replace_images),
            ("ReplaceAllMetadata", replace_metadata),
        ];
        self.post(&path, &params, json!({})).await
    }

    pub async fn remote_search(&self, type_: &str, info: &RemoteSearchInfo) -> Result<Value> {
        let path = format!("Items/RemoteSearch/{type_}");
        let body = json!(info);
        self.post_json(&path, &[], body).await
    }

    pub async fn apply_remote_search(
        &self, id: &str, value: Value, replace_all_images: bool,
    ) -> Result<Response> {
        let path = format!("Items/RemoteSearch/Apply/{id}");
        let params: [(&str, &str); 1] = [("ReplaceAllImages", &replace_all_images.to_string())];
        self.post(&path, &params, json! {value}).await
    }

    pub async fn get_user_avatar_source(&self) -> Result<Option<PictureSource>> {
        let user = self.get_current_user().await?;
        Ok(user
            .primary_image_tag
            .map(|tag| PictureSource::User { id: user.id, tag }))
    }

    pub async fn get_external_id_info(&self, id: &str) -> Result<Vec<ExternalIdInfo>> {
        let path = format!("Items/{id}/ExternalIdInfos");
        let params = [("IsSupportedAsIdentifier", "true")];
        self.request(&path, &params).await
    }

    pub async fn get_library(&self) -> Result<List> {
        let s = self.session();
        let path = format!("Users/{}/Views", s.account.user_id);
        self.request(&path, &[]).await
    }

    pub async fn get_latest(&self, id: &str) -> Result<Vec<SimpleListItem>> {
        let s = self.session();
        let path = format!("Users/{}/Items/Latest", s.account.user_id);
        let params = [
            ("Limit", "16"),
            ("Fields", fields::CORE_YEAR_RATING),
            ("ParentId", id),
            ("ImageTypeLimit", "1"),
            ("EnableImageTypes", "Primary,Backdrop,Thumb,Banner"),
        ];
        self.request(&path, &params).await
    }

    pub fn resolve_url(&self, path: &str) -> String {
        let s = self.session();
        let (url, _) = s.url_headers.as_ref().expect("Client not initialized");
        url.join(url_relative_to_base(url.path(), path))
            .unwrap()
            .to_string()
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn get_list(
        &self, id: &str, start: u32, include_item_types: &str, list_type: ListType,
        sort_order: &str, sortby: &str, filters_list: &FiltersList,
    ) -> Result<List> {
        let s = self.session();
        let user_id = &s.account.user_id;
        let path = match list_type {
            ListType::All => format!("Users/{user_id}/Items"),
            ListType::Resume => format!("Users/{user_id}/Items/Resume"),
            ListType::Genres => "Genres".to_string(),
            _ => format!("Users/{user_id}/Items"),
        };
        let include_item_type = match list_type {
            ListType::Tags => "Tag",
            ListType::BoxSet => "BoxSet",
            _ => include_item_types,
        };
        let start_string = start.to_string();
        let mut params = match list_type {
            ListType::All | ListType::Liked | ListType::Tags | ListType::BoxSet => {
                vec![
                    ("Limit", "50"),
                    ("Fields", fields::LIST_OVERVIEW),
                    ("ParentId", id),
                    ("ImageTypeLimit", "1"),
                    ("StartIndex", &start_string),
                    ("Recursive", "true"),
                    ("IncludeItemTypes", include_item_type),
                    ("SortBy", sortby),
                    ("SortOrder", sort_order),
                    ("EnableImageTypes", "Primary,Backdrop,Thumb,Banner"),
                    if list_type == ListType::Liked {
                        ("Filters", "IsFavorite")
                    } else {
                        ("", "")
                    },
                ]
            }
            ListType::Resume => {
                vec![
                    ("Fields", fields::RESUME),
                    ("ParentId", id),
                    ("EnableImageTypes", "Primary,Backdrop,Thumb,Banner"),
                    ("ImageTypeLimit", "1"),
                    (
                        "IncludeItemTypes",
                        match include_item_type {
                            "Series" => "Episode",
                            _ => include_item_type,
                        },
                    ),
                ]
            }
            ListType::Genres => vec![
                ("Fields", fields::CORE),
                ("IncludeItemTypes", include_item_type),
                ("StartIndex", &start_string),
                ("ImageTypeLimit", "1"),
                ("EnableImageTypes", "Primary,Backdrop,Thumb,Banner"),
                ("Limit", "50"),
                ("userId", user_id),
                ("Recursive", "true"),
                ("ParentId", id),
            ],
            _ => vec![],
        };
        let kv = filters_list.to_kv();
        kv.iter().for_each(|(k, v)| {
            params.push((k.as_str(), v.as_str()));
        });
        self.request(&path, &params).await
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn get_inlist(
        &self, id: Option<String>, start: u32, listtype: &str, parentid: &str, sort_order: &str,
        sortby: &str, filters_list: &FiltersList,
    ) -> Result<List> {
        let s = self.session();
        let path = format!("Users/{}/Items", s.account.user_id);
        let start_string = start.to_string();
        let mut params = vec![
            ("Limit", "50"),
            ("Fields", fields::LIST_OVERVIEW),
            ("ImageTypeLimit", "1"),
            ("StartIndex", &start_string),
            ("Recursive", "true"),
            ("IncludeItemTypes", "Movie,Series,MusicAlbum"),
            ("SortBy", sortby),
            ("SortOrder", sort_order),
            ("EnableImageTypes", "Primary,Backdrop,Thumb,Banner"),
            if listtype == "Genres" || listtype == "Genre" {
                ("GenreIds", parentid)
            } else if listtype == "Studios" {
                ("StudioIds", parentid)
            } else {
                ("TagIds", parentid)
            },
        ];
        if let Some(id) = id.as_deref() {
            params.push(("ParentId", id));
        }
        let kv = filters_list.to_kv();
        kv.iter().for_each(|(k, v)| {
            params.push((k.as_str(), v.as_str()));
        });
        self.request(&path, &params).await
    }

    pub async fn like(&self, id: &str) -> Result<()> {
        let s = self.session();
        let path = format!("Users/{}/FavoriteItems/{}", s.account.user_id, id);
        self.post(&path, &[], json!({})).await?;
        Ok(())
    }

    pub async fn unlike(&self, id: &str) -> Result<()> {
        let s = self.session();
        match self.server_type() {
            ServerType::Emby => {
                let path = format!("Users/{}/FavoriteItems/{}/Delete", s.account.user_id, id);
                self.post(&path, &[], json!({})).await?;
            }
            ServerType::Jellyfin => {
                let path = format!("Users/{}/FavoriteItems/{}", s.account.user_id, id);
                self.delete(&path, &[]).await?;
            }
        }
        Ok(())
    }

    pub async fn set_as_played<T: Into<String>>(
        &self, id: &str, series_id: Option<T>,
    ) -> Result<()> {
        let s = self.session();
        let path = format!("Users/{}/PlayedItems/{}", s.account.user_id, id);
        self.post(&path, &[], json!({})).await?;
        if let Some(series_id) = series_id {
            self.invalidate_next_up_date(series_id.into());
        }
        Ok(())
    }

    pub async fn set_as_unplayed<T: Into<String>>(
        &self, id: &str, series_id: Option<T>,
    ) -> Result<()> {
        let s = self.session();
        match self.server_type() {
            ServerType::Emby => {
                let path = format!("Users/{}/PlayedItems/{}/Delete", s.account.user_id, id);
                self.post(&path, &[], json!({})).await?;
            }
            ServerType::Jellyfin => {
                let path = format!("Users/{}/PlayedItems/{}", s.account.user_id, id);
                self.delete(&path, &[]).await?;
            }
        }
        if let Some(series_id) = series_id {
            self.invalidate_next_up_date(series_id.into());
        }
        Ok(())
    }

    pub async fn position_back(&self, back: &Back, backtype: BackType) -> Result<()> {
        let kind = match backtype {
            BackType::Start => ReportKind::Start,
            BackType::Stop => ReportKind::Stop,
            BackType::Back => ReportKind::Progress,
        };
        // A session is identified by item + media source + play session, so
        // switching episodes (or restarting one) resets the throttle.
        let key = format!(
            "{}|{}|{}",
            back.id,
            back.mediasourceid,
            back.playsessionid.as_deref().unwrap_or_default()
        );

        // Holding the lock across the request serialises reports: a `Stop` can
        // never overtake an in-flight `Progress`.
        let mut gate = self.report_gate.lock().await;
        if !gate.should_send(kind, &key, back.tick) {
            tracing::trace!("{kind:?} report for {key} at {} deduplicated", back.tick);
            return Ok(());
        }

        let result = self.send_position_back(back, backtype).await;
        gate.record(kind, &key, back.tick, result.is_ok());
        result
    }

    async fn send_position_back(&self, back: &Back, backtype: BackType) -> Result<()> {
        let path = match backtype {
            BackType::Start => "Sessions/Playing",
            BackType::Stop => "Sessions/Playing/Stopped",
            BackType::Back => "Sessions/Playing/Progress",
        };
        let params = [("reqformat", "json")];
        let body = json!({
            "VolumeLevel":100,
            "NowPlayingQueue":[],
            "IsMuted":false,
            "IsPaused":false,
            "MaxStreamingBitrate":2147483647,
            "RepeatMode":"RepeatNone",
            "PlaybackStartTimeTicks":back.start_tick,
            "SubtitleOffset":0,
            "PlaybackRate":1,
            "PositionTicks":back.tick,
            "PlayMethod":back.playmethod,
            "PlaySessionId":back.playsessionid,
            "LiveStreamId":back.livestreamid,
            "MediaSourceId":back.mediasourceid,
            "PlaylistIndex":0,
            "PlaylistLength":1,
            "CanSeek":true,
            "ItemId":back.id,
            "Shuffle":false
        });
        self.post(path, &params, body).await?;
        if matches!(backtype, BackType::Stop)
            && let Some(series_id) = back.series_id.as_deref()
        {
            self.invalidate_next_up_date(series_id.to_owned());
        }
        Ok(())
    }

    pub async fn get_similar(&self, id: &str) -> Result<List> {
        let s = self.session();
        let path = format!("Items/{id}/Similar");
        let params = [
            ("Fields", fields::LIST),
            ("UserId", &s.account.user_id),
            ("ImageTypeLimit", "1"),
            ("Limit", "12"),
        ];
        self.request(&path, &params).await
    }

    pub async fn get_actor_item_list(&self, id: &str, types: &str) -> Result<List> {
        let s = self.session();
        let path = format!("Users/{}/Items", s.account.user_id);
        let params = [
            (
                "Fields",
                "PrimaryImageAspectRatio,ProductionYear,CommunityRating",
            ),
            ("PersonIds", id),
            ("Recursive", "true"),
            ("CollapseBoxSetItems", "false"),
            ("SortBy", "SortName"),
            ("SortOrder", "Ascending"),
            ("IncludeItemTypes", types),
            ("ImageTypeLimit", "1"),
            ("Limit", "12"),
        ];
        self.request(&path, &params).await
    }

    pub async fn get_person_large_list(
        &self, id: &str, types: &str, sort_by: &str, sort_order: &str, start_index: u32,
        filters_list: &FiltersList,
    ) -> Result<List> {
        let s = self.session();
        let start_string = start_index.to_string();
        let path = format!("Users/{}/Items", s.account.user_id);
        let mut params = vec![
            ("Fields", fields::OVERVIEW_RATING),
            ("PersonIds", id),
            ("Recursive", "true"),
            ("CollapseBoxSetItems", "false"),
            ("SortBy", sort_by),
            ("SortOrder", sort_order),
            ("IncludeItemTypes", types),
            ("StartIndex", &start_string),
            ("ImageTypeLimit", "1"),
            ("Limit", "50"),
        ];
        let kv = filters_list.to_kv();
        kv.iter().for_each(|(k, v)| {
            params.push((k.as_str(), v.as_str()));
        });
        self.request(&path, &params).await
    }

    pub async fn get_continue_play_list(&self, parent_id: &str) -> Result<List> {
        let s = self.session();
        let params = [
            ("Fields", fields::CORE_OVERVIEW),
            ("Limit", "40"),
            ("ImageTypeLimit", "1"),
            ("SeriesId", parent_id),
            ("UserId", &s.account.user_id),
        ];
        self.request("Shows/NextUp", &params).await
    }

    pub async fn get_season_list(&self, parent_id: &str) -> Result<List> {
        let s = self.session();
        let path = format!("Shows/{parent_id}/Seasons");
        let params = [
            (
                "Fields",
                "BasicSyncInfo,CanDelete,PremiereDate,PrimaryImageAspectRatio,Overview",
            ),
            ("UserId", &s.account.user_id),
            ("ImageTypeLimit", "1"),
        ];
        self.request(&path, &params).await
    }

    pub async fn get_search_recommend(&self) -> Result<List> {
        let s = self.session();
        let path = format!("Users/{}/Items", s.account.user_id);
        let params = [
            ("Limit", "20"),
            ("Fields", "Overview"),
            ("EnableTotalRecordCount", "false"),
            ("ImageTypeLimit", "1"),
            ("EnableImageTypes", "Backdrop"),
            ("Recursive", "true"),
            ("IncludeItemTypes", "Movie,Series"),
            ("SortBy", "IsFavoriteOrLiked,Random"),
        ];
        self.request(&path, &params).await
    }

    pub async fn get_favourite(
        &self, types: &str, start: u32, limit: u32, sort_by: &str, sort_order: &str,
        filters_list: &FiltersList,
    ) -> Result<List> {
        let s = self.session();
        let user_id = &s.account.user_id;
        let path = if types == "People" {
            "Persons".to_string()
        } else {
            format!("Users/{user_id}/Items")
        };
        let limit_string = limit.to_string();
        let start_string = start.to_string();
        let mut params = vec![
            ("Fields", fields::CORE_YEAR_RATING),
            ("Filters", "IsFavorite"),
            ("Recursive", "true"),
            ("CollapseBoxSetItems", "false"),
            ("SortBy", sort_by),
            ("SortOrder", sort_order),
            ("IncludeItemTypes", types),
            ("Limit", &limit_string),
            ("StartIndex", &start_string),
        ];
        if types == "People" {
            params.push(("UserId", user_id));
        }
        let kv = filters_list.to_kv();
        kv.iter().for_each(|(k, v)| {
            params.push((k.as_str(), v.as_str()));
        });
        self.request(&path, &params).await
    }

    pub async fn get_included(&self, id: &str) -> Result<List> {
        if self.is_jellyfin() {
            // TODO: Jellyfin does not support filtering by ListItemIds, return an empty list temporarily.
            return Ok(List::default());
        }
        let s = self.session();
        let path = format!("Users/{}/Items", s.account.user_id);
        let params = [
            ("Fields", fields::CORE_RATING),
            ("Limit", "12"),
            ("ListItemIds", id),
            ("Recursive", "true"),
            ("IncludeItemTypes", "Playlist,BoxSet"),
            ("SortBy", "SortName"),
        ];
        self.request(&path, &params).await
    }

    pub async fn get_includedby(&self, parent_id: &str) -> Result<List> {
        let s = self.session();
        let path = format!("Users/{}/Items", s.account.user_id);
        let params = [
            ("Fields", fields::LIST),
            ("ImageTypeLimit", "1"),
            ("ParentId", parent_id),
            ("SortBy", "DisplayOrder"),
            ("SortOrder", "Ascending"),
            ("EnableTotalRecordCount", "false"),
        ];
        self.request(&path, &params).await
    }

    pub async fn get_folder_include(
        &self, parent_id: &str, sort_by: &str, sort_order: &str, start_index: u32,
        filters_list: &FiltersList,
    ) -> Result<List> {
        let s = self.session();
        let path = format!("Users/{}/Items", s.account.user_id);
        let start_index_string = start_index.to_string();
        let sort_by = format!("IsFolder,{sort_by}");
        let mut params = vec![
            ("Fields", fields::LIST),
            ("StartIndex", &start_index_string),
            ("ImageTypeLimit", "1"),
            ("Limit", "50"),
            ("ParentId", parent_id),
            ("SortBy", &sort_by),
            ("SortOrder", sort_order),
            ("EnableTotalRecordCount", "true"),
        ];
        let kv = filters_list.to_kv();
        kv.iter().for_each(|(k, v)| {
            params.push((k.as_str(), v.as_str()));
        });
        self.request(&path, &params).await
    }

    pub async fn change_password(&self, new_password: &str) -> Result<()> {
        let s = self.session();
        let path = format!("Users/{}/Password", s.account.user_id);
        let old_password = s.account.password.as_str();
        let body = json!({
            "CurrentPw": old_password,
            "NewPw": new_password
        });
        self.post(&path, &[], body).await?;
        Ok(())
    }

    pub async fn hide_from_resume<T: Into<String>>(
        &self, id: &str, series_id: Option<T>,
    ) -> Result<()> {
        let s = self.session();
        let path = format!("Users/{}/Items/{}/HideFromResume", s.account.user_id, id);
        let params = [("Hide", "true")];
        self.post(&path, &params, json!({})).await?;
        if let Some(series_id) = series_id {
            self.invalidate_next_up_date(series_id.into());
        }
        Ok(())
    }

    pub async fn get_songs(&self, parent_id: &str) -> Result<List> {
        let s = self.session();
        let path = format!("Users/{}/Items", s.account.user_id);
        let params = [
            (
                "Fields",
                "BasicSyncInfo,CanDelete,PrimaryImageAspectRatio,SyncStatus",
            ),
            ("ImageTypeLimit", "1"),
            ("ParentId", parent_id),
            ("EnableTotalRecordCount", "false"),
        ];
        self.request(&path, &params).await
    }

    pub async fn get_song_streaming_uri(&self, id: &str) -> String {
        let s = self.session();
        let (url, _) = s.url_headers.as_ref().expect("Client not initialized");
        let path = format!("Audio/{}/universal", id);
        let mut url = url.join(&path).unwrap();
        let mut query_pairs = url.query_pairs_mut();
        query_pairs
            .append_pair("UserId", &s.account.user_id)
            .append_pair("DeviceId", &DEVICE_ID)
            .append_pair("MaxStreamingBitrate", "4000000")
            .append_pair("Container", "opus,mp3|mp3,mp2,mp3|mp2,m4a|aac,mp4|aac,flac,webma,webm,wav|PCM_S16LE,wav|PCM_S24LE,ogg")
            .append_pair("TranscodingContainer", "aac")
            .append_pair("TranscodingProtocol", "hls")
            .append_pair("AudioCodec", "aac")
            .append_pair("PlaySessionId", "1715006733496")
            .append_pair("StartTimeTicks", "0")
            .append_pair("EnableRedirection", "true")
            .append_pair("EnableRemoteMedia", "false");
        match self.server_type() {
            ServerType::Emby => {
                query_pairs.append_pair("api_key", &s.account.access_token);
            }
            ServerType::Jellyfin => {
                query_pairs.append_pair("ApiKey", &s.account.access_token);
            }
        }
        drop(query_pairs);
        url.to_string()
    }

    pub async fn get_additional(&self, id: &str) -> Result<List> {
        let s = self.session();
        let path = format!("Videos/{id}/AdditionalParts");
        let params: [(&str, &str); 1] = [("UserId", &s.account.user_id)];
        self.request(&path, &params).await
    }

    pub async fn get_channels(&self) -> Result<List> {
        let s = self.session();
        let params = [
            ("IsAiring", "true"),
            ("userId", &s.account.user_id),
            ("ImageTypeLimit", "1"),
            ("Limit", "12"),
            ("Fields", "ProgramPrimaryImageAspectRatio"),
            ("SortBy", "DefaultChannelOrder"),
            ("SortOrder", "Ascending"),
        ];
        self.request("LiveTv/Channels", &params).await
    }

    pub async fn get_channels_list(&self, start_index: u32) -> Result<List> {
        let s = self.session();
        let params = [
            ("IsAiring", "true"),
            ("userId", &s.account.user_id),
            ("ImageTypeLimit", "1"),
            ("Limit", "50"),
            ("Fields", "ProgramPrimaryImageAspectRatio"),
            ("SortBy", "DefaultChannelOrder"),
            ("SortOrder", "Ascending"),
            ("StartIndex", &start_index.to_string()),
        ];
        self.request("LiveTv/Channels", &params).await
    }

    pub async fn get_server_info(&self) -> Result<ServerInfo> {
        self.request("System/Info", &[]).await
    }

    pub async fn get_server_info_public(
        &self, server: &str, port: &str, server_type: ServerType,
    ) -> Result<PublicServerInfo> {
        let raw = build_url(server, port)?.to_string();
        Ok(compat::resolve_base(&self.client, &raw, server_type)
            .await?
            .info)
    }

    pub async fn shut_down(&self) -> Result<Response> {
        self.post("System/Shutdown", &[], json!({})).await
    }

    pub async fn restart(&self) -> Result<Response> {
        self.post("System/Restart", &[], json!({})).await
    }

    pub async fn get_activity_log(&self, has_user_id: bool) -> Result<ActivityLogs> {
        let params = [
            ("Limit", "15"),
            ("StartIndex", "0"),
            ("hasUserId", &has_user_id.to_string()),
        ];
        self.request("System/ActivityLog/Entries", &params).await
    }

    pub async fn get_scheduled_tasks(&self) -> Result<Vec<ScheduledTask>> {
        self.request("ScheduledTasks", &[]).await
    }

    pub async fn run_scheduled_task(&self, id: String) -> Result<()> {
        let path = format!("ScheduledTasks/Running/{}", id);
        self.post(&path, &[], json!({})).await?;
        Ok(())
    }

    pub async fn get_image_path(
        &self, id: &str, image_type: &str, image_index: Option<u32>,
    ) -> String {
        let s = self.session();
        let path = format!("Items/{id}/Images/{image_type}/");
        let (url, _) = s.url_headers.as_ref().expect("Client not initialized");
        let url = url.join(&path).unwrap();
        match image_index {
            Some(index) => url.join(&index.to_string()).unwrap().to_string(),
            None => url.to_string(),
        }
    }

    pub async fn get_remote_image_list(
        &self, id: &str, start_index: u32, include_all_languages: bool, type_: &str,
        provider_name: &str,
    ) -> Result<ImageSearchResult> {
        let path = format!("Items/{id}/RemoteImages");
        let start_string = start_index.to_string();
        let params = [
            ("Limit", "50"),
            ("StartIndex", &start_string),
            ("Type", type_),
            ("IncludeAllLanguages", &include_all_languages.to_string()),
            ("ProviderName", provider_name),
        ];
        self.request(&path, &params).await
    }

    pub async fn delete_info(&self, id: &str) -> Result<DeleteInfo> {
        let path = format!("Items/{id}/DeleteInfo");
        self.request(&path, &[]).await
    }

    pub async fn delete_item(&self, ids: &str) -> Result<Response> {
        let params = [("Ids", ids)];
        self.post("Items/Delete", &params, json!({})).await
    }

    pub async fn download_remote_images(
        &self, id: &str, type_: &str, provider_name: &str, image_url: &str,
    ) -> Result<()> {
        let path = format!("Items/{id}/RemoteImages/Download");
        let params = [
            ("Type", type_),
            ("ProviderName", provider_name),
            ("ImageUrl", image_url),
        ];
        self.post(&path, &params, json!({})).await?;
        Ok(())
    }

    pub async fn get_show_missing(
        &self, id: &str, include_specials: bool, upcoming: bool,
    ) -> Result<MissingEpisodesList> {
        let s = self.session();
        let params = [
            ("Fields", "Overview"),
            ("UserId", &s.account.user_id),
            ("ParentId", id),
            ("IncludeSpecials", &include_specials.to_string()),
            ("IncludeUnaired", &upcoming.to_string()),
        ];
        self.request("Shows/Missing", &params).await
    }

    pub async fn reset_metadata(&self, ids: &str) -> Result<Response> {
        self.post("items/metadata/reset", &[], json!({"Ids": ids}))
            .await
    }

    pub async fn filters(&self, type_: &str) -> Result<FilterList> {
        let s = self.session();
        let params = [
            ("SortBy", "SortName"),
            ("SortOrder", "Ascending"),
            ("Recursive", "true"),
            ("EnableImages", "false"),
            ("EnableUserData", "false"),
            (
                "IncludeItemTypes",
                "Movie,Series,Episode,BoxSet,Person,MusicAlbum,Audio,Video",
            ),
            ("userId", &s.account.user_id),
        ];
        self.request(type_, &params).await
    }
}

/// Returns `path` relative to the server base path, so it can be joined onto
/// the session URL. Some servers (and proxies in front of them, e.g.
/// emos/Meridian) hand out paths that already contain the base path: with a
/// base of `https://host/emby/`, `"/emby/emya/subtitle?..."` must not become
/// `/emby/emby/emya/subtitle?...`, which answers 404.
/// Append one line if it is not already listed.
fn push_line(lines: &mut Vec<compat::Line>, url: &Url) {
    let text = url.to_string();
    if lines.iter().any(|line| line.url == text) {
        return;
    }
    lines.push(compat::Line {
        label: compat::line_label(url),
        url: text,
    });
}

fn url_relative_to_base<'a>(base_path: &str, path: &'a str) -> &'a str {
    let base_path = base_path.trim_matches('/');
    let trimmed = path.trim_start_matches('/');

    match trimmed.strip_prefix(base_path) {
        Some(rest) if !base_path.is_empty() && (rest.is_empty() || rest.starts_with('/')) => {
            rest.trim_start_matches('/')
        }
        _ => trimmed,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client::{
        account::ServerType,
        error::UserFacingError,
    };

    #[test]
    fn relative_url_does_not_duplicate_the_base_path() {
        // emos/Emya signed subtitle URLs already carry the /emby base path
        assert_eq!(
            url_relative_to_base("/emby/", "/emby/emya/subtitle?server=emos"),
            "emya/subtitle?server=emos"
        );
        assert_eq!(
            url_relative_to_base("/emby/", "emby/emya/subtitle"),
            "emya/subtitle"
        );
        // regular server paths must keep the base path
        assert_eq!(
            url_relative_to_base("/emby/", "/emya/video?server=emos"),
            "emya/video?server=emos"
        );
        assert_eq!(
            url_relative_to_base("/", "/Videos/1/stream.mkv"),
            "Videos/1/stream.mkv"
        );
        // absolute URLs are left alone
        assert_eq!(
            url_relative_to_base("/emby/", "https://cdn.example/x.ass"),
            "https://cdn.example/x.ass"
        );
    }

    #[tokio::test]
    async fn search() {
        let result = JELLYFIN_CLIENT
            .login(
                "https://example.com",
                "443",
                ServerType::Jellyfin,
                "test",
                "test",
            )
            .await;
        match result {
            Ok(response) => {
                println!("{}", response.access_token);
                let account = Account {
                    servername: "test".to_string(),
                    server: "https://example.com".to_string(),
                    username: "inaha".to_string(),
                    password: String::new(),
                    port: "443".to_string(),
                    user_id: response.user.id,
                    access_token: response.access_token,
                    server_type: Some(ServerType::Jellyfin),
                    profile: None,
                };
                let _ = JELLYFIN_CLIENT.init(&account).await;
            }
            Err(e) => {
                eprintln!("{}", e.to_user_facing());
            }
        }
        let filters_list = FiltersList::default();
        let result = JELLYFIN_CLIENT.search("你的名字", &["Movie"], "0", &filters_list);
        match result.await {
            Ok(items) => {
                for item in items.items {
                    println!("{}", item.name);
                }
            }
            Err(e) => {
                eprintln!("{}", e.to_user_facing());
            }
        }
    }

    #[test]
    fn every_dialect_sends_the_emby_token() {
        // A login succeeds without any credential, so the dialect that "works"
        // at login proves nothing. Every dialect must carry `X-Emby-Token`,
        // otherwise Emby answers 401 to everything afterwards.
        for style in [AuthStyle::Emby, AuthStyle::MediaBrowser, AuthStyle::Auto] {
            let headers =
                build_headers_with(Some("user"), Some("token"), style, "device").expect("headers");
            assert_eq!(
                headers.get("X-Emby-Token").and_then(|v| v.to_str().ok()),
                Some("token"),
                "{style:?} dropped X-Emby-Token"
            );
        }
    }

    #[test]
    fn auto_style_sends_both_scheme_headers() {
        let headers =
            build_headers_with(Some("user"), Some("token"), AuthStyle::Auto, "device").expect("h");

        let authorization = headers
            .get("Authorization")
            .and_then(|v| v.to_str().ok())
            .expect("Authorization header");
        assert!(authorization.starts_with("MediaBrowser "));
        assert!(authorization.contains("Token=token"));

        let emby = headers
            .get("X-Emby-authorization")
            .and_then(|v| v.to_str().ok())
            .expect("X-Emby-authorization header");
        assert!(emby.starts_with("Emby "));
    }

    #[test]
    fn unauthenticated_requests_have_no_token_header() {
        let headers = build_headers_with(None, None, AuthStyle::Auto, "device").expect("h");
        assert!(headers.get("X-Emby-Token").is_none());
    }

    #[test]
    fn report_gate_dedupes_progress_and_stop() {
        let mut gate = ReportGate::default();
        let key = "item|source|session";

        assert!(gate.should_send(ReportKind::Start, key, 0));
        gate.record(ReportKind::Start, key, 0, true);

        // Same position: the burst of reports after a seek must not go out.
        assert!(!gate.should_send(ReportKind::Progress, key, 0));

        // Real playback moved on.
        assert!(gate.should_send(ReportKind::Progress, key, PROGRESS_MIN_DELTA_TICKS));
        gate.record(ReportKind::Progress, key, PROGRESS_MIN_DELTA_TICKS, true);

        // Seeking backwards is significant too.
        assert!(gate.should_send(ReportKind::Progress, key, 0));

        // A different session always reports.
        assert!(gate.should_send(ReportKind::Progress, "other", 0));

        // `Stop` goes through exactly once (page exit, then window close).
        assert!(gate.should_send(ReportKind::Stop, key, 0));
        gate.record(ReportKind::Stop, key, 0, true);
        assert!(!gate.should_send(ReportKind::Stop, key, 0));

        // …and does not block the next playback of the same item.
        assert!(gate.should_send(ReportKind::Start, key, 0));
    }

    #[test]
    fn report_gate_backs_off_after_a_failure() {
        let mut gate = ReportGate::default();
        let key = "k";

        gate.record(ReportKind::Start, key, 0, true);
        gate.record(ReportKind::Progress, key, 100_000_000, false);

        // The server rejected the last report: do not retry on every tick.
        assert!(!gate.should_send(ReportKind::Progress, key, 200_000_000));

        // `Stop` is never suppressed by a failure or by the throttle.
        assert!(gate.should_send(ReportKind::Stop, key, 200_000_000));
    }

    #[test]
    fn parse_url() {
        let uri = "127.0.0.1";
        let url = if Url::parse(uri).is_err() {
            format!("http://{uri}")
        } else {
            uri.to_string()
        };

        assert_eq!(url, "http://127.0.0.1");
    }

    #[tokio::test]
    async fn test_upload_image() {
        let result = JELLYFIN_CLIENT
            .login(
                "http://127.0.0.1",
                "8096",
                ServerType::Jellyfin,
                "inaha",
                "",
            )
            .await;
        match result {
            Ok(response) => {
                println!("{}", response.access_token);
                let account = Account {
                    servername: "test".to_string(),
                    server: "http://127.0.0.1".to_string(),
                    username: "inaha".to_string(),
                    password: String::new(),
                    port: "8096".to_string(),
                    user_id: response.user.id,
                    access_token: response.access_token,
                    server_type: Some(ServerType::Jellyfin),
                    profile: None,
                };
                let _ = JELLYFIN_CLIENT.init(&account).await;
            }
            Err(e) => {
                eprintln!("{}", e.to_user_facing());
            }
        }

        let image = std::fs::read("/home/inaha/Works/tsukimi/target/debug/test.jpg").unwrap();
        use base64::{
            Engine as _,
            engine::general_purpose::STANDARD,
        };
        let image = STANDARD.encode(&image);
        match JELLYFIN_CLIENT
            .post_image("293", "Thumb", image, "image/jpeg")
            .await
        {
            Ok(_) => {
                println!("success");
            }
            Err(e) => {
                eprintln!("{}", e.to_user_facing());
            }
        }
    }
}

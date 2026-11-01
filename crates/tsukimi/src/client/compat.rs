//! Server compatibility layer.
//!
//! Everything that makes Tsukimi survive *non-stock* Emby/Jellyfin servers lives
//! here: base URL normalization, flavour/capability detection and authorization
//! style negotiation.
//!
//! Design rules (kept deliberately small so upstream rebases stay cheap):
//!
//! * This module never holds state — callers pass the session data in and store
//!   the resulting [`ServerProfile`] on the [`super::Account`].
//! * Every capability is *probed*, never inferred from a version string.
//! * Nothing here may make a working setup worse: all probes are best effort and
//!   fall back to the previous behaviour.

use std::time::Duration;

use anyhow::{
    Context,
    Result,
    anyhow,
};
use reqwest::{
    Client,
    StatusCode,
    header::HeaderMap,
};
use serde::{
    Deserialize,
    Serialize,
};
use url::Url;

use super::{
    account::ServerType,
    structs::PublicServerInfo,
};

/// Probes must never make account switching feel slow: they are an
/// optimisation, and a server that does not answer an *optional* endpoint must
/// not cost more than this.
pub const PROBE_TIMEOUT: Duration = Duration::from_secs(2);

/// Which server implementation we are talking to.
///
/// `Emby`/`Jellyfin` are the two upstreams; `FnOs` and `Ugreen` are NAS vendors
/// that ship an Emby-compatible API; `Custom` is everything else (relay panels,
/// private builds, proxy layers).
#[derive(Serialize, Deserialize, Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum Flavor {
    #[default]
    Emby,
    Jellyfin,
    FnOs,
    Ugreen,
    Custom,
}

impl Flavor {
    pub fn is_emby_family(self) -> bool {
        matches!(self, Self::Emby | Self::FnOs | Self::Ugreen)
    }
}

/// Authorization header dialect.
///
/// Older Emby builds only understand `X-Emby-authorization: Emby ...`, Jellyfin
/// only understands `Authorization: MediaBrowser ...`, and several forks accept
/// one or the other. `Auto` tries `MediaBrowser` first and remembers whatever
/// worked (see the 401 retry in [`crate::client::jellyfin_client`]).
#[derive(Serialize, Deserialize, Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum AuthStyle {
    MediaBrowser,
    Emby,
    #[default]
    Auto,
}

impl AuthStyle {
    /// Styles to try, in order. `Auto` starts with the Jellyfin spelling because
    /// it is the superset understood by both upstreams.
    pub fn attempts(self) -> Vec<AuthStyle> {
        match self {
            Self::Auto => vec![Self::MediaBrowser, Self::Emby],
            other => vec![other],
        }
    }

    pub fn alternate(self) -> Option<Self> {
        match self {
            Self::MediaBrowser => Some(Self::Emby),
            Self::Emby => Some(Self::MediaBrowser),
            Self::Auto => None,
        }
    }
}

/// What the server can actually do. Probed once, then cached in the account.
#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq)]
pub struct Caps {
    /// `false` means "defaults for this flavour, not verified yet".
    pub probed: bool,
    /// `System/Ext/ServerDomains` answered → we can offer alternate lines.
    pub server_domains: bool,
    /// `LiveTv/Channels` answered → show the live TV module.
    pub live_tv: bool,
    /// `MediaSegments/{id}` answered → intro/outro skipping.
    pub segments: bool,
    /// Collect / played / delete are accepted (downgraded on 403/404).
    pub item_mutation: bool,
    /// Person / studio libraries exist.
    pub person_library: bool,
}

impl Default for Caps {
    fn default() -> Self {
        Self {
            probed: false,
            server_domains: false,
            live_tv: false,
            // Jellyfin has MediaSegments, Emby does not (it has chapters).
            segments: false,
            // Optimistic: most servers accept mutations.
            item_mutation: true,
            person_library: true,
        }
    }
}

impl Caps {
    /// Conservative defaults used before the first probe, derived from flavour.
    pub fn with_flavor_defaults(flavor: Flavor) -> Self {
        Self {
            segments: matches!(flavor, Flavor::Jellyfin),
            ..Default::default()
        }
    }
}

/// Everything discovered about one server. Stored on [`super::Account`].
#[derive(Serialize, Deserialize, Clone, Debug, Default, PartialEq, Eq)]
pub struct ServerProfile {
    /// Resolved API root, always with a trailing slash (`https://host/emby/`).
    pub base: String,
    pub flavor: Flavor,
    pub auth: AuthStyle,
    pub caps: Caps,
    /// Alternate API roots for this server (`System/Ext/ServerDomains` plus the
    /// addresses the user configured). Empty means "single line".
    #[serde(default)]
    pub lines: Vec<Line>,
    /// Root the user pinned manually; `None` means "pick by latency".
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub preferred: Option<String>,
}

/// One reachable API root for a server.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct Line {
    /// Absolute URL, always with a trailing slash.
    pub url: String,
    /// Human readable origin for the UI (`host:port` or `Local`/`Remote`).
    pub label: String,
}

impl ServerProfile {
    pub fn base_url(&self) -> Option<Url> {
        if self.base.is_empty() {
            None
        } else {
            Url::parse(&self.base).ok()
        }
    }
}

/// Result of a successful base URL probe.
#[derive(Clone, Debug)]
pub struct ResolvedBase {
    pub base: Url,
    pub info: PublicServerInfo,
}

/// Candidate API roots for a user supplied address.
///
/// Handles the four cases that actually occur in the wild:
/// * `http://host:8096`              → Emby lives under `/emby`
/// * `http://host:8096/emby`         → already correct
/// * `http://host:8096/emby/`        → trailing slash
/// * `http://host/emby` (Jellyfin behind a proxy) → keep as-is
pub fn candidates(raw: &str, hint: ServerType) -> Vec<Url> {
    let Ok(base) = Url::parse(raw) else {
        return Vec::new();
    };
    let base = normalize_path(base);
    let ends_with_emby = base.path().trim_end_matches('/').ends_with("emby");

    let root = if ends_with_emby {
        strip_emby(&base)
    } else {
        base.clone()
    };
    let emby = if ends_with_emby {
        Some(base.clone())
    } else {
        Some(push_segment(&root, "emby"))
    };

    let mut list: Vec<Url> = Vec::new();
    let mut push = |url: Option<Url>| {
        if let Some(url) = url
            && !list.contains(&url)
        {
            list.push(url);
        }
    };

    match hint {
        ServerType::Emby => {
            push(emby.clone());
            push(Some(root.clone()));
        }
        ServerType::Jellyfin => {
            push(Some(root.clone()));
            push(emby.clone());
        }
    }
    // Always keep the address the user typed, verbatim path included.
    push(Some(base));
    list
}

fn normalize_path(mut url: Url) -> Url {
    url.set_query(None);
    url.set_fragment(None);
    let path = url.path().to_string();
    if !path.ends_with('/') {
        url.set_path(&format!("{path}/"));
    }
    url
}

fn push_segment(base: &Url, segment: &str) -> Url {
    let mut url = base.clone();
    url.set_path(&format!("{}/{segment}/", url.path().trim_end_matches('/')));
    url
}

fn strip_emby(base: &Url) -> Url {
    let mut url = base.clone();
    let path = url
        .path()
        .trim_end_matches('/')
        .trim_end_matches("emby")
        .trim_end_matches('/')
        .to_string();
    let stripped = if path.is_empty() {
        "/".to_string()
    } else {
        format!("{path}/")
    };
    url.set_path(&stripped);
    url
}

/// Probe every candidate until one answers like an Emby/Jellyfin server.
pub async fn resolve_base(client: &Client, raw: &str, hint: ServerType) -> Result<ResolvedBase> {
    let candidates = candidates(raw, hint);
    if candidates.is_empty() {
        return Err(anyhow!("not a valid server address: {raw}"));
    }

    // Race the candidates instead of trying them one after another: a stale
    // first candidate used to cost a full timeout before the right one was even
    // attempted, which is exactly what made the first open of a server feel slow.
    let probes = candidates.into_iter().map(|candidate| {
        Box::pin(async move {
            // `base` may differ from `candidate`: reverse proxies frequently
            // redirect `/System/Info/Public` to their canonical mount point.
            probe(client, &candidate)
                .await
                .map(|(base, info)| ResolvedBase { base, info })
                .inspect_err(|e| tracing::debug!("probe {candidate} failed: {e}"))
        })
    });

    match futures_util::future::select_ok(probes).await {
        Ok((resolved, _slower)) => Ok(resolved),
        Err(e) => Err(e.context("no reachable Emby/Jellyfin endpoint")),
    }
}

/// `web/manifest.json` first (no auth needed, served by every Emby/Jellyfin web
/// client), then `System/Info/Public` for the product name.
async fn probe(client: &Client, base: &Url) -> Result<(Url, PublicServerInfo)> {
    let manifest = base.join("web/manifest.json")?;
    let has_manifest = match client.get(manifest).timeout(PROBE_TIMEOUT).send().await {
        Ok(res) => res.status().is_success(),
        Err(_) => false,
    };

    let info_url = base.join("System/Info/Public")?;
    let res = client
        .get(info_url)
        .timeout(PROBE_TIMEOUT)
        .send()
        .await
        .with_context(|| format!("probe {base}"))?;

    if !res.status().is_success() {
        return Err(anyhow!(
            "{base} answered {} (manifest: {})",
            res.status(),
            has_manifest
        ));
    }

    // Follow redirects: the API root is whatever URL actually answered.
    let resolved = root_of(res.url());

    let info = res
        .json::<PublicServerInfo>()
        .await
        .context("parse System/Info/Public")?;

    Ok((resolved, info))
}

/// Derive the API root from a `System/Info/Public` URL, tolerating the redirect
/// target a reverse proxy hands us.
fn root_of(url: &Url) -> Url {
    let mut root = url.clone();
    let path = root.path().to_string();
    let trimmed = path.trim_end_matches('/');
    let suffix = "System/Info/Public";
    let stripped = match trimmed.strip_suffix(suffix) {
        Some(prefix) => prefix.to_string(),
        None => return normalize_path(url.clone()),
    };
    let base = if stripped.is_empty() {
        "/".to_string()
    } else {
        format!("{}/", stripped.trim_end_matches('/'))
    };
    root.set_path(&base);
    root.set_query(None);
    root.set_fragment(None);
    root
}

/// Map the reported product name to a flavour. Never trusted for behaviour —
/// only used to pick conservative defaults before [`probe_caps`] runs.
pub fn detect_flavor(info: &PublicServerInfo) -> Flavor {
    let product = info
        .product_name
        .clone()
        .unwrap_or_default()
        .to_ascii_lowercase();

    if product.contains("jellyfin") {
        Flavor::Jellyfin
    } else if product.contains("fnos") || product.contains("feiniu") {
        Flavor::FnOs
    } else if product.contains("ugreen") {
        Flavor::Ugreen
    } else if product.contains("emby") || product.is_empty() {
        Flavor::Emby
    } else {
        Flavor::Custom
    }
}

/// Best-effort capability probe. Failures mean "assume unsupported", never
/// "fail the account".
pub async fn probe_caps(
    client: &Client, base: &Url, headers: &HeaderMap, user_id: Option<&str>, flavor: Flavor,
) -> Caps {
    let mut caps = Caps::with_flavor_defaults(flavor);

    let live_tv = match user_id {
        Some(uid) => format!("LiveTv/Channels?UserId={uid}&Limit=1"),
        None => "LiveTv/Channels?Limit=1".to_string(),
    };

    // Both probes have the same worst case, so run them together instead of
    // paying the timeout twice.
    let (server_domains, live_tv) = tokio::join!(
        probe_endpoint(client, base, headers, "System/Ext/ServerDomains"),
        probe_endpoint(client, base, headers, &live_tv),
    );
    caps.server_domains = server_domains;
    caps.live_tv = live_tv;

    caps.probed = true;
    caps
}

/// `true` when the endpoint answers 2xx. 401/403/404 are legitimate answers for
/// optional endpoints and simply mean "not available for this account".
async fn probe_endpoint(client: &Client, base: &Url, headers: &HeaderMap, path: &str) -> bool {
    let Ok(url) = base.join(path) else {
        return false;
    };
    match client
        .get(url)
        .headers(headers.clone())
        .timeout(PROBE_TIMEOUT)
        .send()
        .await
    {
        Ok(res) => res.status().is_success(),
        Err(_) => false,
    }
}

/// `Fields` sets used by the client.
///
/// The server returns *everything* it has unless `Fields` is sent, so every list
/// request names what it needs. These used to be inlined at a dozen call sites and
/// had drifted apart; keeping them here makes the drift visible.
///
/// The canonical tiers (see the adapter notes) are `FULL` for detail pages,
/// `CANONICAL_LIST` for grids and `IDENTITY` for lookups. The historical sets are
/// kept verbatim so switching an existing call site changes nothing on screen.
pub mod fields {
    /// Grid/cover rendering only.
    pub const CORE: &str = "BasicSyncInfo,CanDelete,PrimaryImageAspectRatio";

    pub const CORE_YEAR: &str = "BasicSyncInfo,CanDelete,PrimaryImageAspectRatio,ProductionYear";

    pub const CORE_RATING: &str = "BasicSyncInfo,CanDelete,PrimaryImageAspectRatio,CommunityRating";

    pub const CORE_YEAR_RATING: &str =
        "BasicSyncInfo,CanDelete,PrimaryImageAspectRatio,ProductionYear,CommunityRating";

    pub const CORE_OVERVIEW: &str = "BasicSyncInfo,CanDelete,PrimaryImageAspectRatio,Overview";

    /// Item list (search, browse, in-list).
    pub const LIST: &str = "BasicSyncInfo,CanDelete,PrimaryImageAspectRatio,ProductionYear,Status,EndDate,CommunityRating";

    /// Item list that also needs the overview text.
    pub const LIST_OVERVIEW: &str = "Overview,BasicSyncInfo,CanDelete,PrimaryImageAspectRatio,ProductionYear,Status,EndDate,CommunityRating";

    /// Continue-watching / resume rows.
    pub const RESUME: &str =
        "Overview,BasicSyncInfo,CanDelete,PrimaryImageAspectRatio,ProductionYear";

    /// Resume rows that also ask for the community rating.
    pub const RESUME_RATING: &str =
        "Overview,BasicSyncInfo,CanDelete,PrimaryImageAspectRatio,ProductionYear,CommunityRating";

    /// Artist / album rows: overview plus rating, no sync fields.
    pub const OVERVIEW_RATING: &str =
        "Overview,PrimaryImageAspectRatio,ProductionYear,CommunityRating";

    /// Offline / sync-status views.
    pub const SYNC: &str =
        "Overview,PrimaryImageAspectRatio,PremiereDate,ProductionYear,SyncStatus";

    /// Canonical list tier, for new call sites.
    pub const CANONICAL_LIST: &str = "ProviderIds,Overview,Genres,ParentId,SortName,DateCreated,CanDelete,UserData,UserDataLastPlayedDate,ProductionYear,CollectionType,PrimaryImageAspectRatio,CommunityRating,OfficialRating,ChildCount";

    /// Canonical detail tier, for new call sites.
    pub const CANONICAL_FULL: &str = "BasicSyncInfo,ProviderIds,MediaSources,MediaStreams,Chapters,Overview,Genres,People,ParentId,SortName,DateCreated,CanDelete,CanDownload,UserData,UserDataLastPlayedDate,ProductionYear,EndDate,CollectionType,PrimaryImageAspectRatio,CommunityRating,Container,OfficialRating,ChildCount,Studios,AlternateMediaSources";

    /// Identity-only lookups.
    pub const IDENTITY: &str = "UserData,UserDataLastPlayedDate,ParentId";
}

/// Query parameter groups that are safe to drop when a server rejects a request.
///
/// Order matters: the first stage only drops response enrichment (the result set
/// is identical), the second drops optional grouping/filtering flags. Neither
/// stage can change *which* items a request addresses.
pub const DEGRADE_STAGES: &[&[&str]] = &[
    &[
        "Fields",
        "ImageTypeLimit",
        "EnableImageTypes",
        "EnableImages",
    ],
    &[
        "Filters",
        "GroupItemsIntoCollections",
        "CollapseBoxSetItems",
        "GroupProgramsBySeries",
        "enableResumable",
        "excludeContinueWatching",
        "AnyProviderIdEquals",
        "contentDirectoryID",
        "IncludeExternalContent",
    ],
];

/// Remove one degradation stage from a parameter list.
pub fn strip<'a>(params: &[(&'a str, &'a str)], stage: &[&str]) -> Vec<(&'a str, &'a str)> {
    params
        .iter()
        .copied()
        .filter(|(key, _)| !stage.contains(key))
        .collect()
}

/// Parse the (Emby specific) `System/Ext/ServerDomains` response into lines.
///
/// The endpoint is not part of the public Jellyfin API and its shape has changed
/// over time, so parsing is deliberately lenient: accept a bare array of strings,
/// an array of objects with `Url`/`Address`/`Name`, or an object wrapping either
/// under `ServerDomains`/`Items`.
pub fn parse_server_domains(base: &Url, value: &serde_json::Value) -> Vec<Line> {
    fn collect(value: &serde_json::Value, out: &mut Vec<serde_json::Value>) {
        match value {
            serde_json::Value::Array(items) => out.extend(items.iter().cloned()),
            serde_json::Value::Object(map) => {
                for key in ["ServerDomains", "Items", "Domains"] {
                    if let Some(inner) = map.get(key) {
                        collect(inner, out);
                        return;
                    }
                }
                out.push(value.clone());
            }
            _ => {}
        }
    }

    let mut entries = Vec::new();
    collect(value, &mut entries);

    let mut lines: Vec<Line> = Vec::new();
    let mut push = |url: Url, label: String| {
        let url = normalize_path(url).to_string();
        if !lines.iter().any(|line| line.url == url) {
            lines.push(Line { url, label });
        }
    };

    for entry in entries {
        let (address, name) = match &entry {
            serde_json::Value::String(address) => (address.clone(), None),
            serde_json::Value::Object(map) => {
                let address = ["Url", "Address", "Domain", "Value"]
                    .iter()
                    .find_map(|key| map.get(*key).and_then(|v| v.as_str()))
                    .unwrap_or_default()
                    .to_string();
                let name = map.get("Name").and_then(|v| v.as_str()).map(str::to_string);
                (address, name)
            }
            _ => continue,
        };

        let Some(url) = resolve_line(base, &address) else {
            continue;
        };
        let label = name.unwrap_or_else(|| line_label(&url));
        push(url, label);
    }

    lines
}

/// Resolve one address coming from `System/Ext/ServerDomains`.
///
/// Those entries are frequently bare `host:port` strings, sometimes absolute URLs
/// with the client's mount path, and sometimes relative paths.
fn resolve_line(base: &Url, address: &str) -> Option<Url> {
    let address = address.trim();
    if address.is_empty() {
        return None;
    }

    if let Ok(url) = Url::parse(address) {
        return Some(url);
    }

    // `host:port` without a scheme: rebuild it with the scheme of the server we
    // are talking to (and the same mount path, which the caller cannot know).
    let host_port = address.trim_start_matches('/');
    if !host_port.contains(' ') {
        let candidate = format!("{}://{host_port}{}", base.scheme(), base.path());
        if let Ok(url) = Url::parse(&candidate) {
            return Some(url);
        }
    }

    base.join(address).ok()
}

/// `host:port` for the UI, skipping the default port.
pub fn line_label(url: &Url) -> String {
    match (url.host_str(), url.port()) {
        (Some(host), Some(port)) => format!("{host}:{port}"),
        (Some(host), None) => host.to_string(),
        _ => url.to_string(),
    }
}

/// Measure the round trip time of one API root, in milliseconds.
///
/// Uses the cheapest unauthenticated endpoint (`System/Info/Public`) so it can be
/// run before login and from the UI without creating sessions.
pub async fn measure(client: &Client, url: &str) -> Option<u64> {
    let base = Url::parse(url).ok()?;
    let info = base.join("System/Info/Public").ok()?;
    let started = std::time::Instant::now();
    let res = client.get(info).timeout(PROBE_TIMEOUT).send().await.ok()?;
    let elapsed = started.elapsed();
    res.status()
        .is_success()
        .then_some(elapsed.as_millis() as u64)
}

/// Classify a failed response so callers can degrade instead of aborting.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ApiFailure {
    /// Token expired / wrong auth style → try the other style, then re-login.
    Unauthorized,
    /// Endpoint or parameter not supported by this server → degrade silently.
    Unsupported,
    /// Item gone.
    NotFound,
    /// Server-side problem → retry may help.
    Server,
    Other,
}

impl ApiFailure {
    pub fn of(status: StatusCode) -> Self {
        match status {
            StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN => Self::Unauthorized,
            StatusCode::NOT_FOUND | StatusCode::METHOD_NOT_ALLOWED => Self::NotFound,
            StatusCode::NOT_IMPLEMENTED | StatusCode::BAD_REQUEST => Self::Unsupported,
            s if s.is_server_error() => Self::Server,
            _ => Self::Other,
        }
    }

    /// Whether a 404/501 should be treated as "this server does not have it".
    pub fn is_unsupported(self) -> bool {
        matches!(self, Self::Unsupported | Self::NotFound)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn paths(urls: &[Url]) -> Vec<String> {
        urls.iter().map(|u| u.path().to_string()).collect()
    }

    #[test]
    fn emby_root_gets_emby_segment_first() {
        let list = candidates("http://nas:8096", ServerType::Emby);
        assert_eq!(paths(&list), vec!["/emby/", "/"]);
    }

    #[test]
    fn jellyfin_root_does_not_get_a_segment() {
        let list = candidates("http://nas:8096", ServerType::Jellyfin);
        assert_eq!(paths(&list), vec!["/", "/emby/"]);
    }

    #[test]
    fn existing_emby_path_is_kept_and_root_is_the_fallback() {
        let list = candidates("http://nas:8096/emby", ServerType::Emby);
        assert_eq!(paths(&list), vec!["/emby/", "/"]);
    }

    #[test]
    fn custom_prefix_is_preserved() {
        // A relay that exposes Emby under its own prefix must keep that prefix
        // and may also fall back to the parent path.
        let list = candidates("https://relay.example/panel/emby", ServerType::Emby);
        assert_eq!(paths(&list), vec!["/panel/emby/", "/panel/"]);
    }

    #[test]
    fn query_and_fragment_are_dropped() {
        let list = candidates("http://nas:8096/?x=1#frag", ServerType::Emby);
        assert_eq!(list[0].query(), None);
        assert_eq!(list[0].fragment(), None);
    }

    #[test]
    fn invalid_address_yields_no_candidates() {
        assert!(candidates("not a url", ServerType::Emby).is_empty());
    }

    #[test]
    fn flavor_detection() {
        let detect = |product: &str| {
            detect_flavor(&PublicServerInfo {
                product_name: Some(product.into()),
                ..Default::default()
            })
        };

        assert_eq!(detect("Jellyfin Server"), Flavor::Jellyfin);
        assert_eq!(detect("Emby Server"), Flavor::Emby);
        assert_eq!(detect("fnOS Media"), Flavor::FnOs);
        assert_eq!(detect("UGREEN NAS"), Flavor::Ugreen);
        assert_eq!(detect("Emya"), Flavor::Custom);
        // Servers that report nothing are treated as plain Emby.
        assert_eq!(detect(""), Flavor::Emby);
    }

    #[test]
    fn root_is_derived_from_the_redirected_info_url() {
        let url = Url::parse("https://relay.example/emby/System/Info/Public?x=1").unwrap();
        assert_eq!(root_of(&url).as_str(), "https://relay.example/emby/");

        let url = Url::parse("https://relay.example/System/Info/Public").unwrap();
        assert_eq!(root_of(&url).as_str(), "https://relay.example/");
    }

    #[test]
    fn strip_only_removes_the_named_stage() {
        let params = [
            ("Fields", fields::LIST),
            ("Recursive", "true"),
            ("Filters", "IsFavorite"),
        ];

        let without_enrichment = strip(&params, DEGRADE_STAGES[0]);
        assert_eq!(
            without_enrichment,
            vec![("Recursive", "true"), ("Filters", "IsFavorite")]
        );

        let without_filters = strip(&params, DEGRADE_STAGES[1]);
        assert_eq!(
            without_filters,
            vec![("Fields", fields::LIST), ("Recursive", "true")]
        );
    }

    #[test]
    fn server_domains_parse_common_shapes() {
        let base = Url::parse("https://nas.local:8096/emby/").unwrap();

        let value = serde_json::json!({
            "ServerDomains": [
                { "Name": "Remote", "Url": "https://example.com/emby/" },
                "192.168.1.10:8096",
                "/emby/"
            ]
        });
        let lines = parse_server_domains(&base, &value);
        let urls: Vec<&str> = lines.iter().map(|line| line.url.as_str()).collect();

        // Absolute URLs are kept as-is; a bare `host:port` inherits the scheme
        // and mount path of the server we are talking to.
        assert!(urls.contains(&"https://example.com/emby/"));
        assert!(urls.contains(&"https://192.168.1.10:8096/emby/"));
        assert_eq!(lines[0].label, "Remote");
    }

    #[test]
    fn server_domains_ignore_unparsable_entries() {
        let base = Url::parse("https://nas.local:8096/").unwrap();
        let value = serde_json::json!([null, 42, {}]);
        assert!(parse_server_domains(&base, &value).is_empty());
    }

    #[test]
    fn auto_auth_tries_media_browser_first() {
        assert_eq!(
            AuthStyle::Auto.attempts(),
            vec![AuthStyle::MediaBrowser, AuthStyle::Emby]
        );
        assert_eq!(AuthStyle::MediaBrowser.alternate(), Some(AuthStyle::Emby));
    }
}

//! URL selection shared by the built-in player and external playback.
use url::Url;

use super::{
    jellyfin_client::JELLYFIN_CLIENT,
    structs::MediaSource,
};

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum MediaSourceFallback {
    DirectPlay(String),
    PlaybackInfo,
}

pub(crate) fn media_source_url(
    source: &MediaSource, item_id: &str, play_session_id: Option<&str>,
) -> Option<(String, Option<MediaSourceFallback>)> {
    let direct_url = source.container.as_deref().and_then(|container| {
        JELLYFIN_CLIENT
            .get_item_direct_play_url(
                container,
                source.item_id.as_deref().unwrap_or(item_id),
                &source.id,
                play_session_id,
            )
            .ok()
    });
    let (url, fallback) = select_url(source, direct_url)?;
    // Signed DirectStreamUrl values can be relative to the active API root.
    Some((JELLYFIN_CLIENT.resolve_url(&url), fallback))
}

fn select_url(
    source: &MediaSource, direct_url: Option<String>,
) -> Option<(String, Option<MediaSourceFallback>)> {
    if let Some(path) = source.path.as_deref()
        && Url::parse(path).is_ok()
    {
        let fallback = if source.live_stream_id.is_some() {
            Some(MediaSourceFallback::PlaybackInfo)
        } else {
            direct_url.map(MediaSourceFallback::DirectPlay)
        };
        return Some((path.to_owned(), fallback));
    }

    // Emos/Emya needs its server-signed URL, whereas plain Jellyfin/Emby
    // DirectStreamUrl values without credentials need our authenticated builder.
    if let Some(url) = source.direct_stream_url.as_deref().map(str::trim)
        && let Some((_, query)) = url.split_once('?')
        && url::form_urlencoded::parse(query.split('#').next().unwrap_or_default().as_bytes()).any(
            |(key, value)| {
                !value.is_empty()
                    && ["api_key", "apikey", "token", "sign"]
                        .iter()
                        .any(|name| key.eq_ignore_ascii_case(name))
            },
        )
    {
        return Some((
            url.to_owned(),
            direct_url.map(MediaSourceFallback::DirectPlay),
        ));
    }

    direct_url.map(|url| (url, None))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn source(value: serde_json::Value) -> MediaSource {
        let mut base = serde_json::json!({"Id": "source-1", "MediaStreams": []});
        base.as_object_mut()
            .unwrap()
            .extend(value.as_object().unwrap().clone());
        serde_json::from_value(base).unwrap()
    }

    #[test]
    fn remote_path_keeps_direct_play_fallback() {
        let source = source(serde_json::json!({"Path": "https://cdn.example/video.mkv"}));
        assert_eq!(
            select_url(
                &source,
                Some("https://server.example/stream?api_key=test".into())
            ),
            Some((
                "https://cdn.example/video.mkv".into(),
                Some(MediaSourceFallback::DirectPlay(
                    "https://server.example/stream?api_key=test".into()
                ))
            ))
        );
    }

    #[test]
    fn live_stream_renegotiates_instead_of_using_direct_play_fallback() {
        let source = source(serde_json::json!({
            "Path": "https://cdn.example/live.m3u8", "LiveStreamId": "live-1"
        }));
        assert_eq!(
            select_url(&source, Some("direct".into())).unwrap().1,
            Some(MediaSourceFallback::PlaybackInfo)
        );
    }

    #[test]
    fn signed_server_url_is_preserved_including_relative_path() {
        for url in [
            "/emby/emya/video?token=secret&sign=a%2Bb",
            "https://cdn.example/stream?ApiKey=secret",
        ] {
            let source = source(serde_json::json!({"DirectStreamUrl": url}));
            assert_eq!(select_url(&source, Some("direct".into())).unwrap().0, url);
        }
    }

    #[test]
    fn unsigned_or_empty_credentials_use_authenticated_builder() {
        for url in [
            "/stream?Static=true",
            "/stream?token=",
            "/token=secret/stream",
        ] {
            let source = source(serde_json::json!({"DirectStreamUrl": url}));
            assert_eq!(
                select_url(&source, Some("direct".into())),
                Some(("direct".into(), None))
            );
        }
    }

    #[test]
    fn local_server_path_is_not_exported() {
        let source = source(serde_json::json!({"Path": "/media/movie.mkv"}));
        assert_eq!(
            select_url(&source, Some("direct".into())),
            Some(("direct".into(), None))
        );
        assert_eq!(select_url(&source, None), None);
    }
}

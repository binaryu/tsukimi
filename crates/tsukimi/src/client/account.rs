use anyhow::{
    Context,
    Result,
    anyhow,
};
use serde::{
    Deserialize,
    Serialize,
};
use url::Url;

use crate::{
    client::compat::ServerProfile,
    ui::provider::descriptor::VecSerialize,
};

#[derive(Serialize, Deserialize, Clone, Copy, PartialEq, Eq, Default)]
pub enum ServerType {
    #[default]
    Emby = 0,
    Jellyfin = 1,
}

impl ServerType {
    pub fn index(self) -> u32 {
        self as u32
    }

    pub fn from_index(index: u32) -> Self {
        match index {
            1 => Self::Jellyfin,
            _ => Self::Emby,
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Default)]
pub struct Account {
    pub servername: String,
    pub server: String,
    pub username: String,
    pub password: String,
    pub port: String,
    pub user_id: String,
    pub access_token: String,
    pub server_type: Option<ServerType>,
    /// Compatibility profile discovered by probing the server (base URL,
    /// flavour, authorization style, capabilities). Accounts created before this
    /// field existed have `None` and are probed on first use.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub profile: Option<ServerProfile>,
}

/// Identity of an account is its connection data **including the token**.
///
/// The compatibility profile is derived state and must not make two accounts
/// compare unequal, but the access token must: `edit_account` treats
/// "already present" as "nothing to do", so a re-login that changed only the
/// token would be silently dropped and every request would keep failing with 401
/// using the stale credential.
impl PartialEq for Account {
    fn eq(&self, other: &Self) -> bool {
        self.servername == other.servername
            && self.server == other.server
            && self.username == other.username
            && self.port == other.port
            && self.user_id == other.user_id
            && self.access_token == other.access_token
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn account(token: &str) -> Account {
        Account {
            password: String::new(),
            servername: "nas".into(),
            server: "https://nas.example".into(),
            username: "user".into(),
            port: "8096".into(),
            user_id: "uid".into(),
            access_token: token.into(),
            server_type: Some(ServerType::Emby),
            profile: None,
        }
    }

    #[test]
    fn a_new_token_makes_the_account_different() {
        // Otherwise a re-login is dropped and the client keeps using the old token.
        assert!(account("old") != account("new"));
    }

    #[test]
    fn the_derived_profile_does_not_affect_identity() {
        let mut other = account("token");
        other.profile = Some(Default::default());
        assert!(account("token") == other);
    }
}

impl Account {
    pub fn url(&self) -> Result<Url> {
        build_url(&self.server, &self.port)
    }
}

pub(super) fn build_url(url_str: &str, port: &str) -> Result<Url> {
    let mut url = Url::parse(url_str)?;
    let port = port.parse::<u16>().context("Invalid server port")?;
    url.set_port(Some(port))
        .map_err(|_| anyhow!("Failed to set port"))?;
    Ok(url)
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "type")]
pub struct Accounts {
    pub accounts: Vec<Account>,
}

impl VecSerialize<Account> for Vec<Account> {
    fn to_string(&self) -> String {
        serde_json::to_string(&self).expect("Failed to serialize Vec<Descriptor>")
    }
}

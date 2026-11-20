//! The `ipc` family's own settings API: the one its web page uses.
//!
//! Generic ONVIF cameras sold under many names (`A_ONVIF_CAMERA` and the
//! like, gSOAP on port 80) keep their accounts outside ONVIF: a user made with
//! ONVIF `CreateUsers` cannot log in to RTSP or the web page. Their page reads
//! and writes settings as whole XML documents, `POST /get<Thing>Config` and
//! `/set<Thing>Config`, each wrapped in a SOAP envelope whose header carries
//! the login: user and password, each DES-ECB encrypted under the fixed key
//! `WebLogin` (zero padded) and written as hex. The content type must be
//! exactly `application/x-www-form-urlencoded`: anything else and gSOAP takes
//! the request for ONVIF and answers `ActionNotSupported`. A wrong login is a
//! 200 whose body is `passwordError`.
//!
//! Plain HTTP on the camera's LAN, as the camera offers nothing else: the
//! gateway keeps that LAN to itself.

use std::time::Duration;

use des::cipher::{BlockCipherEncrypt, KeyInit};
use des::Des;
use quick_xml::events::Event;

/// A camera account. The camera keeps passwords readable; they are never
/// copied out of here.
#[derive(Debug, Clone, PartialEq)]
pub struct Account {
    pub username: String,
    password: String,
    /// `Administrator`, `Operator` or `User`.
    pub group: String,
    /// `Enable` or `Disable`.
    pub status: String,
}

/// The three levels, as the camera names them, by the names commands use.
pub const GROUPS: [(&str, &str); 3] = [
    ("admin", "Administrator"),
    ("operator", "Operator"),
    ("viewer", "User"),
];

#[derive(Debug)]
pub enum Error {
    /// The camera did not answer, or not in time.
    Unreachable(String),
    /// The login was refused.
    Login,
    /// It answered something else.
    Refused(String),
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::Unreachable(e) => write!(f, "the camera did not answer: {e}"),
            Error::Login => f.write_str("the camera refused the gateway's login"),
            Error::Refused(e) => write!(f, "the camera refused: {e}"),
        }
    }
}

/// `s` as the page sends it: DES-ECB under `WebLogin`, zero padded, in hex.
pub fn login_hex(s: &str) -> String {
    let des = Des::new_from_slice(b"WebLogin").expect("an 8-byte key");
    let mut bytes = s.as_bytes().to_vec();
    bytes.resize(bytes.len().div_ceil(8).max(1) * 8, 0);
    for block in bytes.as_chunks_mut::<8>().0 {
        des.encrypt_block(block.into());
    }
    hex::encode(bytes)
}

fn attr(s: &str) -> String {
    quick_xml::escape::escape(s).into_owned()
}

/// One camera: where it is and the login the gateway uses.
pub struct Camera {
    base: String,
    user: String,
    password: String,
    http: reqwest::Client,
}

impl Camera {
    pub fn new(address: &str, user: &str, password: &str) -> Self {
        Camera {
            base: format!("http://{address}"),
            user: user.into(),
            password: password.into(),
            http: reqwest::Client::builder()
                .timeout(Duration::from_secs(10))
                .build()
                .expect("an HTTP client"),
        }
    }

    pub fn login_user(&self) -> &str {
        &self.user
    }

    async fn call(&self, endpoint: &str, body: &str) -> Result<String, Error> {
        let envelope = format!(
            "<?xml version=\"1.0\"?><soap:Envelope xmlns:soap=\"http://www.w3.org/2001/12/soap-envelope\">\
             <soap:Header>\t<userid>{}</userid>\t<passwd>{}</passwd></soap:Header>\
             <soap:Body>{body}</soap:Body></soap:Envelope>",
            login_hex(&self.user),
            login_hex(&self.password),
        );
        let r = self
            .http
            .post(format!("{}{endpoint}", self.base))
            .header("Content-Type", "application/x-www-form-urlencoded")
            .header("X-Requested-With", "XMLHttpRequest")
            .body(envelope)
            .send()
            .await
            .map_err(|e| Error::Unreachable(e.without_url().to_string()))?;
        let status = r.status();
        let text = r
            .text()
            .await
            .map_err(|e| Error::Unreachable(e.without_url().to_string()))?;
        if text.trim() == "passwordError" || status == reqwest::StatusCode::UNAUTHORIZED {
            return Err(Error::Login);
        }
        if !status.is_success() || text.contains("ActionNotSupported") || text.contains("Fault>") {
            return Err(Error::Refused(format!("{endpoint} answered {status}")));
        }
        Ok(text)
    }

    pub async fn users(&self) -> Result<Vec<Account>, Error> {
        parse_users(&self.call("/getUserConfig", "").await?)
    }

    pub async fn add_user(&self, username: &str, password: &str, group: &str) -> Result<(), Error> {
        let a = Account {
            username: username.into(),
            password: password.into(),
            group: group.into(),
            status: "Enable".into(),
        };
        self.call("/setAddUserConfig", &document(&a))
            .await
            .map(drop)
    }

    /// Writes an account back whole, as the page does: a new password, group
    /// or status for an existing user.
    pub async fn edit_user(&self, a: &Account) -> Result<(), Error> {
        self.call("/setEditUserConfig", &document(a))
            .await
            .map(drop)
    }

    pub async fn remove_user(&self, a: &Account) -> Result<(), Error> {
        self.call("/setDelUserConfig", &document(a)).await.map(drop)
    }
}

impl Account {
    pub fn with_password(mut self, password: &str) -> Self {
        self.password = password.into();
        self
    }

    pub fn with_group(mut self, group: &str) -> Self {
        self.group = group.into();
        self
    }
}

fn document(a: &Account) -> String {
    format!(
        "<UserConfig><Account Username=\"{}\" Password=\"{}\" Group=\"{}\" Status=\"{}\" /></UserConfig>",
        attr(&a.username),
        attr(&a.password),
        attr(&a.group),
        attr(&a.status),
    )
}

/// The `<Account>` elements of a `UserConfig` answer.
fn parse_users(xml: &str) -> Result<Vec<Account>, Error> {
    let mut reader = quick_xml::Reader::from_str(xml);
    let mut out = Vec::new();
    loop {
        match reader.read_event() {
            Ok(Event::Empty(e) | Event::Start(e)) if e.name().as_ref() == "Account" => {
                let mut a = Account {
                    username: String::new(),
                    password: String::new(),
                    group: String::new(),
                    status: String::new(),
                };
                for at in e.attributes().flatten() {
                    let v = at
                        .normalized_value(quick_xml::XmlVersion::Implicit1_0)
                        .map_err(|e| Error::Refused(format!("unreadable user list: {e}")))?
                        .into_owned();
                    match at.key.as_ref() {
                        "Username" => a.username = v,
                        "Password" => a.password = v,
                        "Group" => a.group = v,
                        "Status" => a.status = v,
                        _ => {}
                    }
                }
                out.push(a);
            }
            Ok(Event::Eof) => break,
            Err(e) => return Err(Error::Refused(format!("unreadable user list: {e}"))),
            _ => {}
        }
    }
    if out.is_empty() {
        return Err(Error::Refused("the user list is empty".into()));
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn login_matches_the_cameras_own_des() {
        // From the camera's jsCore/des.js: stringToHex(des("WebLogin", s, 1, 0)).
        assert_eq!(login_hex("a"), "b4b4cfb9437ba29e");
        assert_eq!(login_hex("abcdefgh"), "4b24819b8505b9b5");
        assert_eq!(login_hex("abcdefghi"), "4b24819b8505b9b5e8b7e01c8a97a456");
    }

    #[test]
    fn reads_the_user_list_and_escapes_what_it_writes() {
        let xml = "<UserConfig>\n<Account Username=\"admin\" Password=\"x&amp;y\"\nGroup=\"Administrator\" Status=\"Enable\" />\n\
                   <Account Username=\"site\" Password=\"p\" Group=\"User\" Status=\"Enable\" /></UserConfig>";
        let users = parse_users(xml).unwrap();
        assert_eq!(users.len(), 2);
        assert_eq!(
            (users[0].username.as_str(), users[0].password.as_str()),
            ("admin", "x&y")
        );
        assert_eq!(users[1].group, "User");
        let doc = document(&users[0]);
        assert!(doc.contains("Password=\"x&amp;y\""), "{doc}");
        assert!(parse_users("passwordError").is_err());
    }
}

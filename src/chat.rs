//! # Chat module

use std::convert::TryFrom;
use std::time::{Duration, SystemTime};

use async_std::path::{Path, PathBuf};
use deltachat_derive::*;
use itertools::Itertools;
use num_traits::FromPrimitive;
use serde::{Deserialize, Serialize};

use crate::blob::{BlobError, BlobObject};
use crate::chatlist::*;
use crate::config::*;
use crate::constants::*;
use crate::contact::*;
use crate::context::Context;
use crate::dc_tools::*;
use crate::error::{bail, ensure, format_err, Error};
use crate::events::Event;
use crate::job::{self, Action};
use crate::message::{self, InvalidMsgId, Message, MessageState, MsgId};
use crate::mimeparser::SystemMessage;
use crate::param::*;
use crate::sql;
use crate::stock::StockMessage;

/// Chat ID, including reserved IDs.
///
/// Some chat IDs are reserved to identify special chat types.  This
/// type can represent both the special as well as normal chats.
#[derive(
    Debug,
    Copy,
    Clone,
    Default,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    Hash,
    PartialOrd,
    Ord,
    FromPrimitive,
    ToPrimitive,
    Sqlx,
)]
pub struct ChatId(u32);

impl ChatId {
    /// Create a new [ChatId].
    pub fn new(id: u32) -> ChatId {
        ChatId(id)
    }

    /// A ChatID which indicates an error.
    ///
    /// This is transitional and should not be used in new code.  Do
    /// not represent errors in a ChatId.
    pub fn is_error(self) -> bool {
        self.0 == 0
    }

    /// An unset ChatId
    ///
    /// Like [ChatId::is_error], from which it is indistinguishable, this is
    /// transitional and should not be used in new code.
    pub fn is_unset(self) -> bool {
        self.0 == 0
    }

    /// Whether the chat ID signifies a special chat.
    ///
    /// This kind of chat ID can not be used for real chats.
    pub fn is_special(self) -> bool {
        match self.0 {
            0..=DC_CHAT_ID_LAST_SPECIAL => true,
            _ => false,
        }
    }

    /// Chat ID which represents the deaddrop chat.
    ///
    /// This is a virtual chat showing all messages belonging to chats
    /// flagged with [Blocked::Deaddrop].  Usually the UI will show
    /// these messages as contact requests.
    pub fn is_deaddrop(self) -> bool {
        self.0 == DC_CHAT_ID_DEADDROP
    }

    /// Chat ID for messages which need to be deleted.
    ///
    /// Messages which should be deleted get this chat ID and are
    /// deleted later.  Deleted messages need to stay around as long
    /// as they are not deleted on the server so that their rfc724_mid
    /// remains known and downloading them again can be avoided.
    pub fn is_trash(self) -> bool {
        self.0 == DC_CHAT_ID_TRASH
    }

    // DC_CHAT_ID_MSGS_IN_CREATION seems unused?

    /// Virtual chat showing all starred messages.
    pub fn is_starred(self) -> bool {
        self.0 == DC_CHAT_ID_STARRED
    }

    /// Chat ID signifying there are **any** number of archived chats.
    ///
    /// This chat ID can be returned in a [Chatlist] and signals to
    /// the UI to include a link to the archived chats.
    pub fn is_archived_link(self) -> bool {
        self.0 == DC_CHAT_ID_ARCHIVED_LINK
    }

    /// Virtual chat ID signalling there are **only** archived chats.
    ///
    /// This can be included in the chatlist if the
    /// [DC_GCL_ADD_ALLDONE_HINT] flag is used to build the
    /// [Chatlist].
    pub fn is_alldone_hint(self) -> bool {
        self.0 == DC_CHAT_ID_ALLDONE_HINT
    }

    pub async fn set_selfavatar_timestamp(
        self,
        context: &Context,
        timestamp: i64,
    ) -> Result<(), Error> {
        context
            .sql
            .execute(
                r#"
UPDATE contacts
  SET selfavatar_sent=?
  WHERE id IN(
    SELECT contact_id FROM chats_contacts WHERE chat_id=?
  );
"#,
                paramsx![timestamp, self],
            )
            .await?;
        Ok(())
    }

    pub async fn set_blocked(self, context: &Context, new_blocked: Blocked) -> bool {
        if self.is_special() {
            warn!(context, "ignoring setting of Block-status for {}", self);
            return false;
        }
        context
            .sql
            .execute(
                "UPDATE chats SET blocked=? WHERE id=?;",
                paramsx![new_blocked, self],
            )
            .await
            .is_ok()
    }

    pub async fn unblock(self, context: &Context) {
        self.set_blocked(context, Blocked::Not).await;
    }

    /// Archives or unarchives a chat.
    pub async fn set_visibility(
        self,
        context: &Context,
        visibility: ChatVisibility,
    ) -> Result<(), Error> {
        ensure!(
            !self.is_special(),
            "bad chat_id, can not be special chat: {}",
            self
        );

        if visibility == ChatVisibility::Archived {
            context
                .sql
                .execute(
                    "UPDATE msgs SET state=? WHERE chat_id=? AND state=?;",
                    paramsx![MessageState::InNoticed, self, MessageState::InFresh],
                )
                .await?;
        }

        context
            .sql
            .execute(
                "UPDATE chats SET archived=? WHERE id=?;",
                paramsx![visibility, self],
            )
            .await?;

        context.emit_event(Event::MsgsChanged {
            msg_id: MsgId::new(0),
            chat_id: ChatId::new(0),
        });

        Ok(())
    }

    // note that unarchive() is not the same as set_visibility(Normal) -
    // eg. unarchive() does not modify pinned chats and does not send events.
    pub async fn unarchive(self, context: &Context) -> Result<(), Error> {
        context
            .sql
            .execute(
                "UPDATE chats SET archived=0 WHERE id=? and archived=1",
                paramsx![self],
            )
            .await?;
        Ok(())
    }

    /// Deletes a chat.
    pub async fn delete(self, context: &Context) -> Result<(), Error> {
        ensure!(
            !self.is_special(),
            "bad chat_id, can not be a special chat: {}",
            self
        );
        /* Up to 2017-11-02 deleting a group also implied leaving it, see above why we have changed this. */

        let _chat = Chat::load_from_db(context, self).await?;
        context
            .sql
            .execute(
                "DELETE FROM msgs_mdns WHERE msg_id IN (SELECT id FROM msgs WHERE chat_id=?);",
                paramsx![self],
            )
            .await?;

        context
            .sql
            .execute("DELETE FROM msgs WHERE chat_id=?;", paramsx![self])
            .await?;

        context
            .sql
            .execute(
                "DELETE FROM chats_contacts WHERE chat_id=?;",
                paramsx![self],
            )
            .await?;

        context
            .sql
            .execute("DELETE FROM chats WHERE id=?;", paramsx![self])
            .await?;

        context.emit_event(Event::MsgsChanged {
            msg_id: MsgId::new(0),
            chat_id: ChatId::new(0),
        });

        job::kill_action(context, Action::Housekeeping).await;
        let j = job::Job::new(Action::Housekeeping, 0, Params::new(), 10);
        job::add(context, j).await;

        Ok(())
    }

    /// Sets draft message.
    ///
    /// Passing `None` as message just deletes the draft
    pub async fn set_draft(self, context: &Context, msg: Option<&mut Message>) {
        if self.is_special() {
            return;
        }

        let changed = match msg {
            None => self.maybe_delete_draft(context).await,
            Some(msg) => self.set_draft_raw(context, msg).await,
        };

        if changed {
            context.emit_event(Event::MsgsChanged {
                chat_id: self,
                msg_id: MsgId::new(0),
            });
        }
    }

    // similar to as dc_set_draft() but does not emit an event
    async fn set_draft_raw(self, context: &Context, msg: &mut Message) -> bool {
        let deleted = self.maybe_delete_draft(context).await;
        let set = self.do_set_draft(context, msg).await.is_ok();

        // Can't inline. Both functions above must be called, no shortcut!
        deleted || set
    }

    async fn get_draft_msg_id(self, context: &Context) -> Option<MsgId> {
        context
            .sql
            .query_value(
                "SELECT id FROM msgs WHERE chat_id=? AND state=?;",
                paramsx![self, MessageState::OutDraft],
            )
            .await
            .ok()
    }

    pub async fn get_draft(self, context: &Context) -> Result<Option<Message>, Error> {
        if self.is_special() {
            return Ok(None);
        }
        match self.get_draft_msg_id(context).await {
            Some(draft_msg_id) => {
                let msg = Message::load_from_db(context, draft_msg_id).await?;
                Ok(Some(msg))
            }
            None => Ok(None),
        }
    }

    /// Delete draft message in specified chat, if there is one.
    ///
    /// Returns `true`, if message was deleted, `false` otherwise.
    async fn maybe_delete_draft(self, context: &Context) -> bool {
        match self.get_draft_msg_id(context).await {
            Some(msg_id) => msg_id.delete_from_db(context).await.is_ok(),
            None => false,
        }
    }

    /// Set provided message as draft message for specified chat.
    ///
    /// Return true on success, false on database error.
    async fn do_set_draft(self, context: &Context, msg: &mut Message) -> Result<(), Error> {
        match msg.viewtype {
            Viewtype::Unknown => bail!("Can not set draft of unknown type."),
            Viewtype::Text => match msg.text.as_ref() {
                Some(text) => {
                    if text.is_empty() {
                        bail!("No text in draft");
                    }
                }
                None => bail!("No text in draft"),
            },
            _ => {
                let blob = msg
                    .param
                    .get_blob(Param::File, context, !msg.is_increation())
                    .await?
                    .ok_or_else(|| format_err!("No file stored in params"))?;
                msg.param.set(Param::File, blob.as_name());
            }
        }
        context
            .sql
            .execute(
                "INSERT INTO msgs (chat_id, from_id, timestamp, type, state, txt, param, hidden)
         VALUES (?,?,?, ?,?,?,?,?);",
                paramsx![
                    self,
                    DC_CONTACT_ID_SELF as i32,
                    time(),
                    msg.viewtype,
                    MessageState::OutDraft,
                    msg.text.as_deref().unwrap_or(""),
                    msg.param.to_string(),
                    1i32
                ],
            )
            .await?;
        Ok(())
    }

    /// Returns number of messages in a chat.
    pub async fn get_msg_cnt(self, context: &Context) -> usize {
        let v: i32 = context
            .sql
            .query_value("SELECT COUNT(*) FROM msgs WHERE chat_id=?;", paramsx![self])
            .await
            .unwrap_or_default();
        v as usize
    }

    pub async fn get_fresh_msg_cnt(self, context: &Context) -> usize {
        let v: i32 = context
            .sql
            .query_value(
                "SELECT COUNT(*)
                FROM msgs
                WHERE state=10
                AND hidden=0
                AND chat_id=?;",
                paramsx![self],
            )
            .await
            .unwrap_or_default();
        v as usize
    }

    pub(crate) async fn get_param(self, context: &Context) -> Result<Params, Error> {
        let res: Option<String> = context
            .sql
            .query_value_optional("SELECT param FROM chats WHERE id=?", paramsx![self])
            .await?;
        Ok(res
            .map(|s| s.parse().unwrap_or_default())
            .unwrap_or_default())
    }

    // Returns true if chat is a saved messages chat.
    pub async fn is_self_talk(self, context: &Context) -> Result<bool, Error> {
        Ok(self.get_param(context).await?.exists(Param::Selftalk))
    }

    /// Returns true if chat is a device chat.
    pub async fn is_device_talk(self, context: &Context) -> Result<bool, Error> {
        Ok(self.get_param(context).await?.exists(Param::Devicetalk))
    }

    async fn parent_query<T>(self, context: &Context, fields: &str) -> sql::Result<Option<T>>
    where
        T: for<'a> sqlx::row::FromRow<'a, sqlx::sqlite::SqliteRow<'a>> + Unpin,
    {
        let sql = &context.sql;
        let query = format!(
            "SELECT {} \
             FROM msgs WHERE chat_id=? AND state NOT IN (?, ?, ?, ?) AND NOT hidden \
             ORDER BY timestamp DESC, id DESC \
             LIMIT 1;",
            fields
        );
        sql.query_row_optional(
            query,
            paramsx![
                self,
                MessageState::OutPreparing,
                MessageState::OutDraft,
                MessageState::OutPending,
                MessageState::OutFailed
            ],
        )
        .await
    }

    async fn get_parent_mime_headers(self, context: &Context) -> Option<(String, String, String)> {
        self.parent_query(context, "rfc724_mid, mime_in_reply_to, mime_references")
            .await
            .ok()
            .flatten()
    }

    async fn parent_is_encrypted(self, context: &Context) -> Result<bool, Error> {
        let packed: Option<(String,)> = self.parent_query(context, "param").await?;

        if let Some(ref packed) = packed {
            let param = packed.0.parse::<Params>()?;
            Ok(param.exists(Param::GuaranteeE2ee))
        } else {
            // No messages
            Ok(false)
        }
    }

    /// Bad evil escape hatch.
    ///
    /// Avoid using this, eventually types should be cleaned up enough
    /// that it is no longer necessary.
    pub fn to_u32(self) -> u32 {
        self.0
    }
}

impl std::fmt::Display for ChatId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.is_deaddrop() {
            write!(f, "Chat#Deadrop")
        } else if self.is_trash() {
            write!(f, "Chat#Trash")
        } else if self.is_starred() {
            write!(f, "Chat#Starred")
        } else if self.is_archived_link() {
            write!(f, "Chat#ArchivedLink")
        } else if self.is_alldone_hint() {
            write!(f, "Chat#AlldoneHint")
        } else if self.is_special() {
            write!(f, "Chat#Special{}", self.0)
        } else {
            write!(f, "Chat#{}", self.0)
        }
    }
}

/// Allow converting [ChatId] to an SQLite type.
///
/// This allows you to directly store [ChatId] into the database as
/// well as query for a [ChatId].
impl rusqlite::types::ToSql for ChatId {
    fn to_sql(&self) -> rusqlite::Result<rusqlite::types::ToSqlOutput> {
        let val = rusqlite::types::Value::Integer(self.0 as i64);
        let out = rusqlite::types::ToSqlOutput::Owned(val);
        Ok(out)
    }
}

/// Allow converting an SQLite integer directly into [ChatId].
impl rusqlite::types::FromSql for ChatId {
    fn column_result(value: rusqlite::types::ValueRef) -> rusqlite::types::FromSqlResult<Self> {
        i64::column_result(value).and_then(|val| {
            if 0 <= val && val <= std::u32::MAX as i64 {
                Ok(ChatId::new(val as u32))
            } else {
                Err(rusqlite::types::FromSqlError::OutOfRange(val))
            }
        })
    }
}

/// An object representing a single chat in memory.
/// Chat objects are created using eg. `Chat::load_from_db`
/// and are not updated on database changes;
/// if you want an update, you have to recreate the object.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Chat {
    pub id: ChatId,
    pub typ: Chattype,
    pub name: String,
    pub visibility: ChatVisibility,
    pub grpid: String,
    blocked: Blocked,
    pub param: Params,
    is_sending_locations: bool,
    pub mute_duration: MuteDuration,
}

impl<'a> sqlx::FromRow<'a, sqlx::sqlite::SqliteRow<'a>> for Chat {
    fn from_row(row: &sqlx::sqlite::SqliteRow<'a>) -> Result<Self, sqlx::Error> {
        use sqlx::Row;

        let c = Chat {
            id: row.try_get("id").unwrap_or_default(),
            typ: row.try_get("type")?,
            name: row.try_get::<String, _>("name")?,
            grpid: row.try_get::<String, _>("grpid")?,
            param: row
                .try_get::<String, _>("param")?
                .parse()
                .unwrap_or_default(),
            visibility: row.try_get("archived")?,
            blocked: row.try_get::<Option<_>, _>("blocked")?.unwrap_or_default(),
            is_sending_locations: row.try_get("locations_send_until")?,
            mute_duration: row.try_get("muted_until")?,
        };

        Ok(c)
    }
}

impl Chat {
    /// Loads chat from the database by its ID.
    pub async fn load_from_db(context: &Context, chat_id: ChatId) -> Result<Self, Error> {
        let res: Result<Chat, _> = context
            .sql
            .query_row(
                r#"
SELECT c.id, c.type, c.name, c.grpid, c.param, c.archived,
       c.blocked, c.locations_send_until, c.muted_until
  FROM chats c
  WHERE c.id=?;
"#,
                paramsx![chat_id],
            )
            .await;

        match res {
            Err(err @ crate::sql::Error::Sql(rusqlite::Error::QueryReturnedNoRows)) => {
                Err(err.into())
            }
            Err(err) => {
                error!(
                    context,
                    "chat: failed to load from db {}: {:?}", chat_id, err
                );
                Err(err.into())
            }
            Ok(mut chat) => {
                if chat.id.is_deaddrop() {
                    chat.name = context.stock_str(StockMessage::DeadDrop).await.into();
                } else if chat.id.is_archived_link() {
                    let tempname = context.stock_str(StockMessage::ArchivedChats).await;
                    let cnt = dc_get_archived_cnt(context).await;
                    chat.name = format!("{} ({})", tempname, cnt);
                } else if chat.id.is_starred() {
                    chat.name = context.stock_str(StockMessage::StarredMsgs).await.into();
                } else {
                    if chat.typ == Chattype::Single {
                        let contacts = get_chat_contacts(context, chat.id).await;
                        let mut chat_name = "Err [Name not found]".to_owned();
                        if let Some(contact_id) = contacts.first() {
                            if let Ok(contact) = Contact::get_by_id(context, *contact_id).await {
                                chat_name = contact.get_display_name().to_owned();
                            }
                        }
                        chat.name = chat_name;
                    }
                    if chat.param.exists(Param::Selftalk) {
                        chat.name = context.stock_str(StockMessage::SavedMessages).await.into();
                    } else if chat.param.exists(Param::Devicetalk) {
                        chat.name = context.stock_str(StockMessage::DeviceMessages).await.into();
                    }
                }
                Ok(chat)
            }
        }
    }

    pub fn is_self_talk(&self) -> bool {
        self.param.exists(Param::Selftalk)
    }

    /// Returns true if chat is a device chat.
    pub fn is_device_talk(&self) -> bool {
        self.param.exists(Param::Devicetalk)
    }

    /// Returns true if user can send messages to this chat.
    pub fn can_send(&self) -> bool {
        !self.id.is_special() && !self.is_device_talk()
    }

    pub async fn update_param(&mut self, context: &Context) -> Result<(), Error> {
        context
            .sql
            .execute(
                "UPDATE chats SET param=? WHERE id=?",
                paramsx![self.param.to_string(), self.id],
            )
            .await?;
        Ok(())
    }

    /// Returns chat ID.
    pub fn get_id(&self) -> ChatId {
        self.id
    }

    /// Returns chat type.
    pub fn get_type(&self) -> Chattype {
        self.typ
    }

    /// Returns chat name.
    pub fn get_name(&self) -> &str {
        &self.name
    }

    pub async fn get_profile_image(&self, context: &Context) -> Option<PathBuf> {
        if let Some(image_rel) = self.param.get(Param::ProfileImage) {
            if !image_rel.is_empty() {
                return Some(dc_get_abs_path(context, image_rel));
            }
        } else if self.typ == Chattype::Single {
            let contacts = get_chat_contacts(context, self.id).await;
            if let Some(contact_id) = contacts.first() {
                if let Ok(contact) = Contact::get_by_id(context, *contact_id).await {
                    return contact.get_profile_image(context).await;
                }
            }
        }

        None
    }

    pub async fn get_gossiped_timestamp(&self, context: &Context) -> i64 {
        get_gossiped_timestamp(context, self.id).await
    }

    pub async fn get_color(&self, context: &Context) -> u32 {
        let mut color = 0;

        if self.typ == Chattype::Single {
            let contacts = get_chat_contacts(context, self.id).await;
            if let Some(contact_id) = contacts.first() {
                if let Ok(contact) = Contact::get_by_id(context, *contact_id).await {
                    color = contact.get_color();
                }
            }
        } else {
            color = dc_str_to_color(&self.name);
        }

        color
    }

    /// Returns a struct describing the current state of the chat.
    ///
    /// This is somewhat experimental, even more so than the rest of
    /// deltachat, and the data returned is still subject to change.
    pub async fn get_info(&self, context: &Context) -> Result<ChatInfo, Error> {
        let draft = match self.id.get_draft(context).await? {
            Some(message) => message.text.unwrap_or_else(String::new),
            _ => String::new(),
        };
        Ok(ChatInfo {
            id: self.id,
            type_: self.typ as u32,
            name: self.name.clone(),
            archived: self.visibility == ChatVisibility::Archived,
            param: self.param.to_string(),
            gossiped_timestamp: self.get_gossiped_timestamp(context).await,
            is_sending_locations: self.is_sending_locations,
            color: self.get_color(context).await,
            profile_image: self
                .get_profile_image(context)
                .await
                .map(Into::into)
                .unwrap_or_else(std::path::PathBuf::new),
            draft,
            is_muted: self.is_muted(),
        })
    }

    pub fn get_visibility(&self) -> ChatVisibility {
        self.visibility
    }

    pub fn is_unpromoted(&self) -> bool {
        self.param.get_int(Param::Unpromoted).unwrap_or_default() == 1
    }

    pub fn is_promoted(&self) -> bool {
        !self.is_unpromoted()
    }

    /// Returns true if chat is a verified group chat.
    pub fn is_verified(&self) -> bool {
        self.typ == Chattype::VerifiedGroup
    }

    /// Returns true if location streaming is enabled in the chat.
    pub fn is_sending_locations(&self) -> bool {
        self.is_sending_locations
    }

    pub fn is_muted(&self) -> bool {
        match self.mute_duration {
            MuteDuration::NotMuted => false,
            MuteDuration::Forever => true,
            MuteDuration::Until(when) => when > SystemTime::now(),
        }
    }

    async fn prepare_msg_raw(
        &mut self,
        context: &Context,
        msg: &mut Message,
        timestamp: i64,
    ) -> Result<MsgId, Error> {
        let mut new_references = "".into();
        let mut new_in_reply_to = "".into();
        let mut msg_id = 0;
        let mut to_id = 0;
        let mut location_id = 0;

        if !(self.typ == Chattype::Single
            || self.typ == Chattype::Group
            || self.typ == Chattype::VerifiedGroup)
        {
            error!(context, "Cannot send to chat type #{}.", self.typ,);
            bail!("Cannot set to chat type #{}", self.typ);
        }

        if (self.typ == Chattype::Group || self.typ == Chattype::VerifiedGroup)
            && !is_contact_in_chat(context, self.id, DC_CONTACT_ID_SELF).await
        {
            emit_event!(
                context,
                Event::ErrorSelfNotInGroup("Cannot send message; self not in group.".into())
            );
            bail!("Cannot set message; self not in group.");
        }

        if let Some(from) = context.get_config(Config::ConfiguredAddr).await {
            let new_rfc724_mid = {
                let grpid = match self.typ {
                    Chattype::Group | Chattype::VerifiedGroup => Some(self.grpid.as_str()),
                    _ => None,
                };
                dc_create_outgoing_rfc724_mid(grpid, &from)
            };

            if self.typ == Chattype::Single {
                if let Ok(id) = context
                    .sql
                    .query_value(
                        "SELECT contact_id FROM chats_contacts WHERE chat_id=?;",
                        paramsx![self.id],
                    )
                    .await
                {
                    to_id = id;
                } else {
                    error!(
                        context,
                        "Cannot send message, contact for {} not found.", self.id,
                    );
                    bail!("Cannot set message, contact for {} not found.", self.id);
                }
            } else if (self.typ == Chattype::Group || self.typ == Chattype::VerifiedGroup)
                && self.param.get_int(Param::Unpromoted).unwrap_or_default() == 1
            {
                msg.param.set_int(Param::AttachGroupImage, 1);
                self.param.remove(Param::Unpromoted);
                self.update_param(context).await?;
            }

            /* check if we want to encrypt this message.  If yes and circumstances change
            so that E2EE is no longer available at a later point (reset, changed settings),
            we might not send the message out at all */
            if msg.param.get_int(Param::ForcePlaintext).unwrap_or_default() == 0 {
                let mut can_encrypt = true;
                let mut all_mutual = context.get_config_bool(Config::E2eeEnabled).await;

                // take care that this statement returns NULL rows
                // if there is no peerstates for a chat member!
                // for DC_PARAM_SELFTALK this statement does not return any row
                let res = context
                    .sql
                    .query_map(
                        "SELECT ps.prefer_encrypted, c.addr \
                     FROM chats_contacts cc  \
                     LEFT JOIN contacts c ON cc.contact_id=c.id  \
                     LEFT JOIN acpeerstates ps ON c.addr=ps.addr  \
                     WHERE cc.chat_id=?  AND cc.contact_id>9;",
                        paramsv![self.id],
                        |row| {
                            let addr: String = row.get(1)?;

                            if let Some(prefer_encrypted) = row.get::<_, Option<i32>>(0)? {
                                // the peerstate exist, so we have either public_key or gossip_key
                                // and can encrypt potentially
                                if prefer_encrypted != 1 {
                                    info!(
                                        context,
                                        "[autocrypt] peerstate for {} is {}",
                                        addr,
                                        if prefer_encrypted == 0 {
                                            "NOPREFERENCE"
                                        } else {
                                            "RESET"
                                        },
                                    );
                                    all_mutual = false;
                                }
                            } else {
                                info!(context, "[autocrypt] no peerstate for {}", addr,);
                                can_encrypt = false;
                                all_mutual = false;
                            }
                            Ok(())
                        },
                        |rows| rows.collect::<Result<Vec<_>, _>>().map_err(Into::into),
                    )
                    .await;
                match res {
                    Ok(_) => {}
                    Err(err) => {
                        warn!(context, "chat: failed to load peerstates: {:?}", err);
                    }
                }

                if can_encrypt && (all_mutual || self.id.parent_is_encrypted(context).await?) {
                    msg.param.set_int(Param::GuaranteeE2ee, 1);
                }
            }
            // reset encrypt error state eg. for forwarding
            msg.param.remove(Param::ErroneousE2ee);

            // set "In-Reply-To:" to identify the message to which the composed message is a reply;
            // set "References:" to identify the "thread" of the conversation;
            // both according to RFC 5322 3.6.4, page 25
            //
            // as self-talks are mainly used to transfer data between devices,
            // we do not set In-Reply-To/References in this case.
            if !self.is_self_talk() {
                if let Some((parent_rfc724_mid, parent_in_reply_to, parent_references)) =
                    self.id.get_parent_mime_headers(context).await
                {
                    if !parent_rfc724_mid.is_empty() {
                        new_in_reply_to = parent_rfc724_mid.clone();
                    }

                    // the whole list of messages referenced may be huge;
                    // only use the oldest and and the parent message
                    let parent_references = if let Some(n) = parent_references.find(' ') {
                        &parent_references[0..n]
                    } else {
                        &parent_references
                    };

                    if !parent_references.is_empty() && !parent_rfc724_mid.is_empty() {
                        // angle brackets are added by the mimefactory later
                        new_references = format!("{} {}", parent_references, parent_rfc724_mid);
                    } else if !parent_references.is_empty() {
                        new_references = parent_references.to_string();
                    } else if !parent_in_reply_to.is_empty() && !parent_rfc724_mid.is_empty() {
                        new_references = format!("{} {}", parent_in_reply_to, parent_rfc724_mid);
                    } else if !parent_in_reply_to.is_empty() {
                        new_references = parent_in_reply_to;
                    }
                }
            }

            // add independent location to database

            if msg.param.exists(Param::SetLatitude)
                && context
                    .sql
                    .execute(
                        "INSERT INTO locations \
                     (timestamp,from_id,chat_id, latitude,longitude,independent)\
                     VALUES (?,?,?, ?,?,1);", // 1=DC_CONTACT_ID_SELF
                        paramsx![
                            timestamp,
                            DC_CONTACT_ID_SELF as i32,
                            self.id,
                            msg.param.get_float(Param::SetLatitude).unwrap_or_default(),
                            msg.param.get_float(Param::SetLongitude).unwrap_or_default()
                        ],
                    )
                    .await
                    .is_ok()
            {
                location_id = context
                    .sql
                    .get_rowid2(
                        "locations",
                        "timestamp",
                        timestamp,
                        "from_id",
                        DC_CONTACT_ID_SELF as i32,
                    )
                    .await?;
            }

            // add message to the database

            if context.sql.execute(
                        "INSERT INTO msgs (rfc724_mid, chat_id, from_id, to_id, timestamp, type, state, txt, param, hidden, mime_in_reply_to, mime_references, location_id) VALUES (?,?,?,?,?, ?,?,?,?,?, ?,?,?);",
                        paramsx![
                            &new_rfc724_mid,
                            self.id,
                            DC_CONTACT_ID_SELF as i32,
                            to_id as i32,
                            timestamp,
                            msg.viewtype,
                            msg.state,
                            msg.text.as_ref(),
                            msg.param.to_string(),
                            msg.hidden,
                            new_in_reply_to,
                            new_references,
                            location_id as i32
                        ]
                    ).await.is_ok() {
                        msg_id = context.sql.get_rowid(
                            "msgs",
                            "rfc724_mid",
                            new_rfc724_mid,
                        ).await?;
                    } else {
                        error!(
                            context,
                            "Cannot send message, cannot insert to database ({}).",
                            self.id,
                        );
                    }
        } else {
            error!(context, "Cannot send message, not configured.",);
        }

        Ok(MsgId::new(msg_id))
    }
}

#[derive(
    Debug, Copy, Eq, PartialEq, Clone, Serialize, Deserialize, FromPrimitive, ToPrimitive, Sqlx,
)]
#[repr(u8)]
pub enum ChatVisibility {
    Normal = 0,
    Archived = 1,
    Pinned = 2,
}

impl Default for ChatVisibility {
    fn default() -> Self {
        ChatVisibility::Normal
    }
}

impl rusqlite::types::ToSql for ChatVisibility {
    fn to_sql(&self) -> rusqlite::Result<rusqlite::types::ToSqlOutput> {
        let visibility = match &self {
            ChatVisibility::Normal => 0,
            ChatVisibility::Archived => 1,
            ChatVisibility::Pinned => 2,
        };
        let val = rusqlite::types::Value::Integer(visibility);
        let out = rusqlite::types::ToSqlOutput::Owned(val);
        Ok(out)
    }
}

impl rusqlite::types::FromSql for ChatVisibility {
    fn column_result(value: rusqlite::types::ValueRef) -> rusqlite::types::FromSqlResult<Self> {
        i64::column_result(value).and_then(|val| {
            match val {
                2 => Ok(ChatVisibility::Pinned),
                1 => Ok(ChatVisibility::Archived),
                0 => Ok(ChatVisibility::Normal),
                // fallback to to Normal for unknown values, may happen eg. on imports created by a newer version.
                _ => Ok(ChatVisibility::Normal),
            }
        })
    }
}

/// The current state of a chat.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct ChatInfo {
    /// The chat ID.
    pub id: ChatId,

    /// The type of chat as a `u32` representation of [Chattype].
    ///
    /// On the C API this number is one of the
    /// `DC_CHAT_TYPE_UNDEFINED`, `DC_CHAT_TYPE_SINGLE`,
    /// `DC_CHAT_TYPE_GROUP` or `DC_CHAT_TYPE_VERIFIED_GROUP`
    /// constants.
    #[serde(rename = "type")]
    pub type_: u32,

    /// The name of the chat.
    pub name: String,

    /// Whether the chat is archived.
    pub archived: bool,

    /// The "params" of the chat.
    ///
    /// This is the string-serialised version of [Params] currently.
    pub param: String,

    /// Last time this client sent autocrypt gossip headers to this chat.
    pub gossiped_timestamp: i64,

    /// Whether this chat is currently sending location-stream messages.
    pub is_sending_locations: bool,

    /// Colour this chat should be represented in by the UI.
    ///
    /// Yes, spelling colour is hard.
    pub color: u32,

    /// The path to the profile image.
    ///
    /// If there is no profile image set this will be an empty string
    /// currently.
    pub profile_image: std::path::PathBuf,

    /// The draft message text.
    ///
    /// If the chat has not draft this is an empty string.
    ///
    /// TODO: This doesn't seem rich enough, it can not handle drafts
    ///       which contain non-text parts.  Perhaps it should be a
    ///       simple `has_draft` bool instead.
    pub draft: String,

    /// Whether the chat is muted
    ///
    /// The exact time its muted can be found out via the `chat.mute_duration` property
    pub is_muted: bool,
    // ToDo:
    // - [ ] deaddrop,
    // - [ ] summary,
    // - [ ] lastUpdated,
    // - [ ] freshMessageCounter,
    // - [ ] email
}

/// Create a chat from a message ID.
///
/// Typically you'd do this for a message ID found in the
/// [DC_CHAT_ID_DEADDROP] which turns the chat the message belongs to
/// into a normal chat.  The chat can be a 1:1 chat or a group chat
/// and all messages belonging to the chat will be moved from the
/// deaddrop to the normal chat.
///
/// In reality the messages already belong to this chat as receive_imf
/// always creates chat IDs appropriately, so this function really
/// only unblocks the chat and "scales up" the origin of the contact
/// the message is from.
///
/// If prompting the user before calling this function, they should be
/// asked whether they want to chat with the **contact** the message
/// is from and **not** the group name since this can be really weird
/// and confusing when taken from subject of implicit groups.
///
/// # Returns
///
/// The "created" chat ID is returned.
pub async fn create_by_msg_id(context: &Context, msg_id: MsgId) -> Result<ChatId, Error> {
    let msg = Message::load_from_db(context, msg_id).await?;
    let chat = Chat::load_from_db(context, msg.chat_id).await?;
    ensure!(
        !chat.id.is_special(),
        "Message can not belong to a special chat"
    );
    if chat.blocked != Blocked::Not {
        chat.id.unblock(context).await;

        // Sending with 0s as data since multiple messages may have changed.
        context.emit_event(Event::MsgsChanged {
            chat_id: ChatId::new(0),
            msg_id: MsgId::new(0),
        });
    }
    Contact::scaleup_origin_by_id(context, msg.from_id, Origin::CreateChat).await;
    Ok(chat.id)
}

/// Create a normal chat with a single user.  To create group chats,
/// see dc_create_group_chat().
///
/// If a chat already exists, this ID is returned, otherwise a new chat is created;
/// this new chat may already contain messages, eg. from the deaddrop, to get the
/// chat messages, use dc_get_chat_msgs().
pub async fn create_by_contact_id(context: &Context, contact_id: u32) -> Result<ChatId, Error> {
    let chat_id = match lookup_by_contact_id(context, contact_id).await {
        Ok((chat_id, chat_blocked)) => {
            if chat_blocked != Blocked::Not {
                // unblock chat (typically move it from the deaddrop to view
                chat_id.unblock(context).await;
            }
            chat_id
        }
        Err(err) => {
            if !Contact::real_exists_by_id(context, contact_id).await
                && contact_id != DC_CONTACT_ID_SELF
            {
                warn!(
                    context,
                    "Cannot create chat, contact {} does not exist.", contact_id,
                );
                return Err(err);
            } else {
                let (chat_id, _) =
                    create_or_lookup_by_contact_id(context, contact_id, Blocked::Not).await?;
                Contact::scaleup_origin_by_id(context, contact_id, Origin::CreateChat).await;
                chat_id
            }
        }
    };

    context.emit_event(Event::MsgsChanged {
        chat_id: ChatId::new(0),
        msg_id: MsgId::new(0),
    });

    Ok(chat_id)
}

pub(crate) async fn update_saved_messages_icon(context: &Context) -> Result<(), Error> {
    // if there is no saved-messages chat, there is nothing to update. this is no error.
    if let Ok((chat_id, _)) = lookup_by_contact_id(context, DC_CONTACT_ID_SELF).await {
        let icon = include_bytes!("../assets/icon-saved-messages.png");
        let blob = BlobObject::create(context, "icon-saved-messages.png".to_string(), icon).await?;
        let icon = blob.as_name().to_string();

        let mut chat = Chat::load_from_db(context, chat_id).await?;
        chat.param.set(Param::ProfileImage, icon);
        chat.update_param(context).await?;
    }
    Ok(())
}

pub(crate) async fn update_device_icon(context: &Context) -> Result<(), Error> {
    // if there is no device-chat, there is nothing to update. this is no error.
    if let Ok((chat_id, _)) = lookup_by_contact_id(context, DC_CONTACT_ID_DEVICE).await {
        let icon = include_bytes!("../assets/icon-device.png");
        let blob = BlobObject::create(context, "icon-device.png".to_string(), icon).await?;
        let icon = blob.as_name().to_string();

        let mut chat = Chat::load_from_db(context, chat_id).await?;
        chat.param.set(Param::ProfileImage, &icon);
        chat.update_param(context).await?;

        let mut contact = Contact::load_from_db(context, DC_CONTACT_ID_DEVICE).await?;
        contact.param.set(Param::ProfileImage, icon);
        contact.update_param(context).await?;
    }
    Ok(())
}

async fn update_special_chat_name(
    context: &Context,
    contact_id: u32,
    stock_id: StockMessage,
) -> Result<(), Error> {
    if let Ok((chat_id, _)) = lookup_by_contact_id(context, contact_id).await {
        let name: String = context.stock_str(stock_id).await.into();
        // the `!= name` condition avoids unneeded writes
        context
            .sql
            .execute(
                "UPDATE chats SET name=? WHERE id=? AND name!=?;",
                paramsx![&name, chat_id, &name],
            )
            .await?;
    }
    Ok(())
}

pub(crate) async fn update_special_chat_names(context: &Context) -> Result<(), Error> {
    update_special_chat_name(context, DC_CONTACT_ID_DEVICE, StockMessage::DeviceMessages).await?;
    update_special_chat_name(context, DC_CONTACT_ID_SELF, StockMessage::SavedMessages).await?;
    Ok(())
}

pub(crate) async fn create_or_lookup_by_contact_id(
    context: &Context,
    contact_id: u32,
    create_blocked: Blocked,
) -> Result<(ChatId, Blocked), Error> {
    ensure!(context.sql.is_open().await, "Database not available");
    ensure!(contact_id > 0, "Invalid contact id requested");

    if let Ok((chat_id, chat_blocked)) = lookup_by_contact_id(context, contact_id).await {
        // Already exists, no need to create.
        return Ok((chat_id, chat_blocked));
    }

    let contact = Contact::load_from_db(context, contact_id).await?;
    let chat_name = contact.get_display_name().to_string();

    let mut tx = context.sql.begin().await?;
    sqlx::query(
        "INSERT INTO chats (type, name, param, blocked, created_timestamp) VALUES(?, ?, ?, ?, ?)",
    )
    .bind_all(paramsx![
        Chattype::Single,
        chat_name,
        match contact_id {
            DC_CONTACT_ID_SELF => "K=1".to_string(), // K = Param::Selftalk
            DC_CONTACT_ID_DEVICE => "D=1".to_string(), // D = Param::Devicetalk
            _ => "".to_string(),
        },
        create_blocked as i32,
        time(),
    ])
    .execute(&mut tx)
    .await?;

    sqlx::query(
        "INSERT INTO chats_contacts (chat_id, contact_id) VALUES ( (SELECT last_insert_rowid() ), ?)",
    )
    .bind(contact_id as i64)
    .execute(&mut tx)
    .await?;
    tx.commit().await?;

    if contact_id == DC_CONTACT_ID_SELF {
        update_saved_messages_icon(context).await?;
    } else if contact_id == DC_CONTACT_ID_DEVICE {
        update_device_icon(context).await?;
    }

    lookup_by_contact_id(context, contact_id).await
}

pub(crate) async fn lookup_by_contact_id(
    context: &Context,
    contact_id: u32,
) -> Result<(ChatId, Blocked), Error> {
    ensure!(context.sql.is_open().await, "Database not available");

    context
        .sql
        .query_row(
            "SELECT c.id, c.blocked
               FROM chats c
              INNER JOIN chats_contacts j
                      ON c.id=j.chat_id
              WHERE c.type=100
                AND c.id>9
                AND j.contact_id=?;",
            paramsx![contact_id as i32],
        )
        .await
        .map(|(id, blocked): (ChatId, Option<Blocked>)| (id, blocked.unwrap_or_default()))
        .map_err(Into::into)
}

pub async fn get_by_contact_id(context: &Context, contact_id: u32) -> Result<ChatId, Error> {
    let (chat_id, blocked) = lookup_by_contact_id(context, contact_id).await?;
    ensure_eq!(blocked, Blocked::Not, "Requested contact is blocked");

    Ok(chat_id)
}

pub async fn prepare_msg(
    context: &Context,
    chat_id: ChatId,
    msg: &mut Message,
) -> Result<MsgId, Error> {
    ensure!(
        !chat_id.is_special(),
        "Cannot prepare message for special chat"
    );

    msg.state = MessageState::OutPreparing;
    let msg_id = prepare_msg_common(context, chat_id, msg).await?;
    context.emit_event(Event::MsgsChanged {
        chat_id: msg.chat_id,
        msg_id: msg.id,
    });

    Ok(msg_id)
}

pub(crate) fn msgtype_has_file(msgtype: Viewtype) -> bool {
    match msgtype {
        Viewtype::Unknown => false,
        Viewtype::Text => false,
        Viewtype::Image => true,
        Viewtype::Gif => true,
        Viewtype::Sticker => true,
        Viewtype::Audio => true,
        Viewtype::Voice => true,
        Viewtype::Video => true,
        Viewtype::File => true,
    }
}

async fn prepare_msg_blob(context: &Context, msg: &mut Message) -> Result<(), Error> {
    if msg.viewtype == Viewtype::Text {
        // the caller should check if the message text is empty
    } else if msgtype_has_file(msg.viewtype) {
        let blob = msg
            .param
            .get_blob(Param::File, context, !msg.is_increation())
            .await?
            .ok_or_else(|| {
                format_err!("Attachment missing for message of type #{}", msg.viewtype)
            })?;
        msg.param.set(Param::File, blob.as_name());

        if msg.viewtype == Viewtype::File || msg.viewtype == Viewtype::Image {
            // Correct the type, take care not to correct already very special
            // formats as GIF or VOICE.
            //
            // Typical conversions:
            // - from FILE to AUDIO/VIDEO/IMAGE
            // - from FILE/IMAGE to GIF */
            if let Some((better_type, better_mime)) =
                message::guess_msgtype_from_suffix(&blob.to_abs_path())
            {
                msg.viewtype = better_type;
                msg.param.set(Param::MimeType, better_mime);
            }
        } else if !msg.param.exists(Param::MimeType) {
            if let Some((_, mime)) = message::guess_msgtype_from_suffix(&blob.to_abs_path()) {
                msg.param.set(Param::MimeType, mime);
            }
        }
        info!(
            context,
            "Attaching \"{}\" for message type #{}.",
            blob.to_abs_path().display(),
            msg.viewtype
        );
    } else {
        bail!("Cannot send messages of type #{}.", msg.viewtype);
    }
    Ok(())
}

async fn prepare_msg_common(
    context: &Context,
    chat_id: ChatId,
    msg: &mut Message,
) -> Result<MsgId, Error> {
    msg.id = MsgId::new_unset();
    prepare_msg_blob(context, msg).await?;
    chat_id.unarchive(context).await?;

    let mut chat = Chat::load_from_db(context, chat_id).await?;
    ensure!(chat.can_send(), "cannot send to {}", chat_id);

    // The OutPreparing state is set by dc_prepare_msg() before it
    // calls this function and the message is left in the OutPreparing
    // state.  Otherwise we got called by send_msg() and we change the
    // state to OutPending.
    if msg.state != MessageState::OutPreparing {
        msg.state = MessageState::OutPending;
    }

    msg.id = chat
        .prepare_msg_raw(context, msg, dc_create_smeared_timestamp(context).await)
        .await?;
    msg.chat_id = chat_id;

    Ok(msg.id)
}

/// Returns whether a contact is in a chat or not.
pub async fn is_contact_in_chat(context: &Context, chat_id: ChatId, contact_id: u32) -> bool {
    // this function works for group and for normal chats, however, it is more useful
    // for group chats.
    // DC_CONTACT_ID_SELF may be used to check, if the user itself is in a group
    // chat (DC_CONTACT_ID_SELF is not added to normal chats)

    context
        .sql
        .exists(
            "SELECT contact_id FROM chats_contacts WHERE chat_id=? AND contact_id=?;",
            paramsx![chat_id, contact_id as i32],
        )
        .await
        .unwrap_or_default()
}

/// Send a message defined by a dc_msg_t object to a chat.
///
/// Sends the event #DC_EVENT_MSGS_CHANGED on succcess.
/// However, this does not imply, the message really reached the recipient -
/// sending may be delayed eg. due to network problems. However, from your
/// view, you're done with the message. Sooner or later it will find its way.
// TODO: Do not allow ChatId to be 0, if prepare_msg had been called
//   the caller can get it from msg.chat_id.  Forwards would need to
//   be fixed for this somehow too.
pub async fn send_msg(
    context: &Context,
    chat_id: ChatId,
    msg: &mut Message,
) -> Result<MsgId, Error> {
    if chat_id.is_unset() {
        let forwards = msg.param.get(Param::PrepForwards);
        if let Some(forwards) = forwards {
            for forward in forwards.split(' ') {
                if let Ok(msg_id) = forward
                    .parse::<u32>()
                    .map_err(|_| InvalidMsgId)
                    .map(MsgId::new)
                {
                    if let Ok(mut msg) = Message::load_from_db(context, msg_id).await {
                        send_msg_inner(context, chat_id, &mut msg).await?;
                    };
                }
            }
            msg.param.remove(Param::PrepForwards);
            msg.save_param_to_disk(context).await;
        }
        return send_msg_inner(context, chat_id, msg).await;
    }

    send_msg_inner(context, chat_id, msg).await
}

/// Tries to send a message synchronously.
///
/// Directly  opens an smtp
/// connection and sends the message, bypassing the job system. If this fails, it writes a send job to
/// the database.
pub async fn send_msg_sync(
    context: &Context,
    chat_id: ChatId,
    msg: &mut Message,
) -> Result<MsgId, Error> {
    if context.is_io_running().await {
        return send_msg(context, chat_id, msg).await;
    }

    if let Some(mut job) = prepare_send_msg(context, chat_id, msg).await? {
        let mut smtp = crate::smtp::Smtp::new();

        let status = job.send_msg_to_smtp(context, &mut smtp).await;

        match status {
            job::Status::Finished(Ok(_)) => {
                context.emit_event(Event::MsgsChanged {
                    chat_id: msg.chat_id,
                    msg_id: msg.id,
                });

                Ok(msg.id)
            }
            _ => {
                job.save(context).await?;
                Err(format_err!(
                    "failed to send message, queued for later sending"
                ))
            }
        }
    } else {
        // Nothing to do
        Ok(msg.id)
    }
}

async fn send_msg_inner(
    context: &Context,
    chat_id: ChatId,
    msg: &mut Message,
) -> Result<MsgId, Error> {
    if let Some(send_job) = prepare_send_msg(context, chat_id, msg).await? {
        job::add(context, send_job).await;

        context.emit_event(Event::MsgsChanged {
            chat_id: msg.chat_id,
            msg_id: msg.id,
        });

        if msg.param.exists(Param::SetLatitude) {
            context.emit_event(Event::LocationChanged(Some(DC_CONTACT_ID_SELF)));
        }
    }

    Ok(msg.id)
}

async fn prepare_send_msg(
    context: &Context,
    chat_id: ChatId,
    msg: &mut Message,
) -> Result<Option<crate::job::Job>, Error> {
    // dc_prepare_msg() leaves the message state to OutPreparing, we
    // only have to change the state to OutPending in this case.
    // Otherwise we still have to prepare the message, which will set
    // the state to OutPending.
    if msg.state != MessageState::OutPreparing {
        // automatically prepare normal messages
        prepare_msg_common(context, chat_id, msg).await?;
    } else {
        // update message state of separately prepared messages
        ensure!(
            chat_id.is_unset() || chat_id == msg.chat_id,
            "Inconsistent chat ID"
        );
        message::update_msg_state(context, msg.id, MessageState::OutPending).await;
    }
    let job = job::send_msg_job(context, msg.id).await?;

    Ok(job)
}

pub async fn send_text_msg(
    context: &Context,
    chat_id: ChatId,
    text_to_send: String,
) -> Result<MsgId, Error> {
    ensure!(
        !chat_id.is_special(),
        "bad chat_id, can not be a special chat: {}",
        chat_id
    );

    let mut msg = Message::new(Viewtype::Text);
    msg.text = Some(text_to_send);
    send_msg(context, chat_id, &mut msg).await
}

pub async fn get_chat_msgs(
    context: &Context,
    chat_id: ChatId,
    flags: u32,
    marker1before: Option<MsgId>,
) -> Vec<MsgId> {
    match delete_device_expired_messages(context).await {
        Err(err) => warn!(context, "Failed to delete expired messages: {}", err),
        Ok(messages_deleted) => {
            if messages_deleted {
                context.emit_event(Event::MsgsChanged {
                    msg_id: MsgId::new(0),
                    chat_id: ChatId::new(0),
                })
            }
        }
    }

    let process_row =
        |row: &rusqlite::Row| Ok((row.get::<_, MsgId>("id")?, row.get::<_, i64>("timestamp")?));
    let process_rows = |rows: rusqlite::MappedRows<_>| {
        let mut ret = Vec::new();
        let mut last_day = 0;
        let cnv_to_local = dc_gm2local_offset();
        for row in rows {
            let (curr_id, ts) = row?;
            if let Some(marker_id) = marker1before {
                if curr_id == marker_id {
                    ret.push(MsgId::new(DC_MSG_ID_MARKER1));
                }
            }
            if (flags & DC_GCM_ADDDAYMARKER) != 0 {
                let curr_local_timestamp = ts + cnv_to_local;
                let curr_day = curr_local_timestamp / 86400;
                if curr_day != last_day {
                    ret.push(MsgId::new(DC_MSG_ID_DAYMARKER));
                    last_day = curr_day;
                }
            }
            ret.push(curr_id);
        }
        Ok(ret)
    };
    let success = if chat_id.is_deaddrop() {
        let show_emails = ShowEmails::from_i32(context.get_config_int(Config::ShowEmails).await)
            .unwrap_or_default();
        context
            .sql
            .query_map(
                "SELECT m.id AS id, m.timestamp AS timestamp
               FROM msgs m
               LEFT JOIN chats
                      ON m.chat_id=chats.id
               LEFT JOIN contacts
                      ON m.from_id=contacts.id
              WHERE m.from_id!=1  -- 1=DC_CONTACT_ID_SELF
                AND m.from_id!=2  -- 2=DC_CONTACT_ID_INFO
                AND m.hidden=0
                AND chats.blocked=2
                AND contacts.blocked=0
                AND m.msgrmsg>=?
              ORDER BY m.timestamp,m.id;",
                paramsv![if show_emails == ShowEmails::All { 0 } else { 1 }],
                process_row,
                process_rows,
            )
            .await
    } else if chat_id.is_starred() {
        context
            .sql
            .query_map(
                "SELECT m.id AS id, m.timestamp AS timestamp
               FROM msgs m
               LEFT JOIN contacts ct
                      ON m.from_id=ct.id
              WHERE m.starred=1
                AND m.hidden=0
                AND ct.blocked=0
              ORDER BY m.timestamp,m.id;",
                paramsv![],
                process_row,
                process_rows,
            )
            .await
    } else {
        context
            .sql
            .query_map(
                "SELECT m.id AS id, m.timestamp AS timestamp
               FROM msgs m
              WHERE m.chat_id=?
                AND m.hidden=0
              ORDER BY m.timestamp, m.id;",
                paramsv![chat_id],
                process_row,
                process_rows,
            )
            .await
    };
    match success {
        Ok(ret) => ret,
        Err(e) => {
            error!(context, "Failed to get chat messages: {}", e);
            Vec::new()
        }
    }
}

pub async fn marknoticed_chat(context: &Context, chat_id: ChatId) -> Result<(), Error> {
    if !context
        .sql
        .exists(
            "SELECT id FROM msgs  WHERE chat_id=? AND state=?;",
            paramsx![chat_id, MessageState::InFresh],
        )
        .await?
    {
        return Ok(());
    }

    context
        .sql
        .execute(
            r#"
UPDATE msgs
  SET state=13
  WHERE chat_id=?
    AND state=10;
"#,
            paramsx![chat_id],
        )
        .await?;

    context.emit_event(Event::MsgsChanged {
        chat_id: ChatId::new(0),
        msg_id: MsgId::new(0),
    });

    Ok(())
}

pub async fn marknoticed_all_chats(context: &Context) -> Result<(), Error> {
    if !context
        .sql
        .exists("SELECT id FROM msgs WHERE state=10;", paramsx![])
        .await?
    {
        return Ok(());
    }

    context
        .sql
        .execute("UPDATE msgs SET state=13 WHERE state=10;", paramsx![])
        .await?;

    context.emit_event(Event::MsgsChanged {
        msg_id: MsgId::new(0),
        chat_id: ChatId::new(0),
    });

    Ok(())
}

/// Deletes messages which are expired according to "delete_device_after" setting.
///
/// Returns true if any message is deleted, so event can be emitted. If nothing
/// has been deleted, returns false.
pub async fn delete_device_expired_messages(context: &Context) -> Result<bool, Error> {
    if let Some(delete_device_after) = context.get_config_delete_device_after().await {
        let threshold_timestamp = time() - delete_device_after;

        let self_chat_id = lookup_by_contact_id(context, DC_CONTACT_ID_SELF)
            .await
            .unwrap_or_default()
            .0;
        let device_chat_id = lookup_by_contact_id(context, DC_CONTACT_ID_DEVICE)
            .await
            .unwrap_or_default()
            .0;

        // Delete expired messages
        //
        // Only update the rows that have to be updated, to avoid emitting
        // unnecessary "chat modified" events.
        let rows_modified = context
            .sql
            .execute(
                "UPDATE msgs \
             SET txt = 'DELETED', chat_id = ? \
             WHERE timestamp < ? \
             AND chat_id > ? \
             AND chat_id != ? \
             AND chat_id != ?",
                paramsx![
                    DC_CHAT_ID_TRASH as i32,
                    threshold_timestamp,
                    DC_CHAT_ID_LAST_SPECIAL as i32,
                    self_chat_id,
                    device_chat_id
                ],
            )
            .await?;

        Ok(rows_modified > 0)
    } else {
        Ok(false)
    }
}

pub async fn get_chat_media(
    context: &Context,
    chat_id: ChatId,
    msg_type: Viewtype,
    msg_type2: Viewtype,
    msg_type3: Viewtype,
) -> Vec<MsgId> {
    // TODO This query could/should be converted to `AND type IN (?, ?, ?)`.
    context
        .sql
        .query_map(
            "SELECT id
               FROM msgs
              WHERE chat_id=?
                AND (type=? OR type=? OR type=?)
              ORDER BY timestamp, id;",
            paramsv![
                chat_id,
                msg_type,
                if msg_type2 != Viewtype::Unknown {
                    msg_type2
                } else {
                    msg_type
                },
                if msg_type3 != Viewtype::Unknown {
                    msg_type3
                } else {
                    msg_type
                },
            ],
            |row| row.get::<_, MsgId>(0),
            |ids| {
                let mut ret = Vec::new();
                for id in ids {
                    if let Ok(msg_id) = id {
                        ret.push(msg_id)
                    }
                }
                Ok(ret)
            },
        )
        .await
        .unwrap_or_default()
}

/// Indicates the direction over which to iterate.
#[derive(Debug, Clone, PartialEq, Eq)]
#[repr(i32)]
pub enum Direction {
    Forward = 1,
    Backward = -1,
}

pub async fn get_next_media(
    context: &Context,
    curr_msg_id: MsgId,
    direction: Direction,
    msg_type: Viewtype,
    msg_type2: Viewtype,
    msg_type3: Viewtype,
) -> Option<MsgId> {
    let mut ret: Option<MsgId> = None;

    if let Ok(msg) = Message::load_from_db(context, curr_msg_id).await {
        let list: Vec<MsgId> = get_chat_media(
            context,
            msg.chat_id,
            if msg_type != Viewtype::Unknown {
                msg_type
            } else {
                msg.viewtype
            },
            msg_type2,
            msg_type3,
        )
        .await;
        for (i, msg_id) in list.iter().enumerate() {
            if curr_msg_id == *msg_id {
                match direction {
                    Direction::Forward => {
                        if i + 1 < list.len() {
                            ret = list.get(i + 1).copied();
                        }
                    }
                    Direction::Backward => {
                        if i >= 1 {
                            ret = list.get(i - 1).copied();
                        }
                    }
                }
                break;
            }
        }
    }
    ret
}

pub async fn get_chat_contacts(context: &Context, chat_id: ChatId) -> Vec<u32> {
    /* Normal chats do not include SELF.  Group chats do (as it may happen that one is deleted from a
    groupchat but the chats stays visible, moreover, this makes displaying lists easier) */

    if chat_id.is_deaddrop() {
        return Vec::new();
    }

    // we could also create a list for all contacts in the deaddrop by searching contacts belonging to chats with
    // chats.blocked=2, however, currently this is not needed

    context
        .sql
        .query_map(
            "SELECT cc.contact_id
               FROM chats_contacts cc
               LEFT JOIN contacts c
                      ON c.id=cc.contact_id
              WHERE cc.chat_id=?
              ORDER BY c.id=1, LOWER(c.name||c.addr), c.id;",
            paramsv![chat_id],
            |row| row.get::<_, u32>(0),
            |ids| ids.collect::<Result<Vec<_>, _>>().map_err(Into::into),
        )
        .await
        .unwrap_or_default()
}

pub async fn create_group_chat(
    context: &Context,
    verified: VerifiedStatus,
    chat_name: impl AsRef<str>,
) -> Result<ChatId, Error> {
    ensure!(!chat_name.as_ref().is_empty(), "Invalid chat name");

    let draft_txt = context
        .stock_string_repl_str(StockMessage::NewGroupDraft, &chat_name)
        .await;
    let grpid = dc_create_id();

    context.sql.execute(
        "INSERT INTO chats (type, name, grpid, param, created_timestamp) VALUES(?, ?, ?, \'U=1\', ?);",
        paramsx![
            if verified != VerifiedStatus::Unverified {
                Chattype::VerifiedGroup
            } else {
                Chattype::Group
            },
            chat_name.as_ref(),
            &grpid,
            time()
        ],
    ).await?;

    let row_id = context.sql.get_rowid("chats", "grpid", grpid).await?;
    let chat_id = ChatId::new(row_id);
    if !chat_id.is_error() {
        if add_to_chat_contacts_table(context, chat_id, DC_CONTACT_ID_SELF).await {
            let mut draft_msg = Message::new(Viewtype::Text);
            draft_msg.set_text(Some(draft_txt));
            chat_id.set_draft_raw(context, &mut draft_msg).await;
        }

        context.emit_event(Event::MsgsChanged {
            msg_id: MsgId::new(0),
            chat_id: ChatId::new(0),
        });
    }

    Ok(chat_id)
}

/// add a contact to the chats_contact table
pub(crate) async fn add_to_chat_contacts_table(
    context: &Context,
    chat_id: ChatId,
    contact_id: u32,
) -> bool {
    match context
        .sql
        .execute(
            "INSERT INTO chats_contacts (chat_id, contact_id) VALUES(?, ?)",
            paramsx![chat_id, contact_id as i32],
        )
        .await
    {
        Ok(_) => true,
        Err(err) => {
            error!(
                context,
                "could not add {} to chat {} table: {}", contact_id, chat_id, err
            );

            false
        }
    }
}

/// remove a contact from the chats_contact table
pub(crate) async fn remove_from_chat_contacts_table(
    context: &Context,
    chat_id: ChatId,
    contact_id: u32,
) -> bool {
    match context
        .sql
        .execute(
            "DELETE FROM chats_contacts WHERE chat_id=? AND contact_id=?",
            paramsx![chat_id, contact_id as i32],
        )
        .await
    {
        Ok(_) => true,
        Err(_) => {
            warn!(
                context,
                "could not remove contact {:?} from chat {:?}", contact_id, chat_id
            );

            false
        }
    }
}

/// Adds a contact to the chat.
pub async fn add_contact_to_chat(context: &Context, chat_id: ChatId, contact_id: u32) -> bool {
    match add_contact_to_chat_ex(context, chat_id, contact_id, false).await {
        Ok(res) => res,
        Err(err) => {
            error!(context, "failed to add contact: {}", err);
            false
        }
    }
}

pub(crate) async fn add_contact_to_chat_ex(
    context: &Context,
    chat_id: ChatId,
    contact_id: u32,
    from_handshake: bool,
) -> Result<bool, Error> {
    ensure!(!chat_id.is_special(), "can not add member to special chats");
    let contact = Contact::get_by_id(context, contact_id).await?;
    let mut msg = Message::default();

    reset_gossiped_timestamp(context, chat_id).await?;

    /*this also makes sure, not contacts are added to special or normal chats*/
    let mut chat = Chat::load_from_db(context, chat_id).await?;
    ensure!(
        real_group_exists(context, chat_id).await,
        "{} is not a group where one can add members",
        chat_id
    );
    ensure!(
        Contact::real_exists_by_id(context, contact_id).await || contact_id == DC_CONTACT_ID_SELF,
        "invalid contact_id {} for adding to group",
        contact_id
    );

    if !is_contact_in_chat(context, chat_id, DC_CONTACT_ID_SELF as u32).await {
        /* we should respect this - whatever we send to the group, it gets discarded anyway! */
        emit_event!(
            context,
            Event::ErrorSelfNotInGroup("Cannot add contact to group; self not in group.".into())
        );
        bail!("can not add contact because our account is not part of it");
    }
    if from_handshake && chat.param.get_int(Param::Unpromoted).unwrap_or_default() == 1 {
        chat.param.remove(Param::Unpromoted);
        chat.update_param(context).await?;
    }
    let self_addr = context
        .get_config(Config::ConfiguredAddr)
        .await
        .unwrap_or_default();
    if addr_cmp(contact.get_addr(), &self_addr) {
        // ourself is added using DC_CONTACT_ID_SELF, do not add this address explicitly.
        // if SELF is not in the group, members cannot be added at all.
        warn!(
            context,
            "invalid attempt to add self e-mail address to group"
        );
        return Ok(false);
    }

    if is_contact_in_chat(context, chat_id, contact_id).await {
        if !from_handshake {
            return Ok(true);
        }
    } else {
        // else continue and send status mail
        if chat.typ == Chattype::VerifiedGroup
            && contact.is_verified(context).await != VerifiedStatus::BidirectVerified
        {
            error!(
                context,
                "Only bidirectional verified contacts can be added to verified groups."
            );
            return Ok(false);
        }
        if !add_to_chat_contacts_table(context, chat_id, contact_id).await {
            return Ok(false);
        }
    }
    if chat.param.get_int(Param::Unpromoted).unwrap_or_default() == 0 {
        msg.viewtype = Viewtype::Text;
        msg.text = Some(
            context
                .stock_system_msg(
                    StockMessage::MsgAddMember,
                    contact.get_addr(),
                    "",
                    DC_CONTACT_ID_SELF as u32,
                )
                .await,
        );
        msg.param.set_cmd(SystemMessage::MemberAddedToGroup);
        msg.param.set(Param::Arg, contact.get_addr());
        msg.param.set_int(Param::Arg2, from_handshake.into());
        msg.id = send_msg(context, chat_id, &mut msg).await?;
    }
    context.emit_event(Event::ChatModified(chat_id));
    Ok(true)
}

async fn real_group_exists(context: &Context, chat_id: ChatId) -> bool {
    // check if a group or a verified group exists under the given ID
    if !context.sql.is_open().await || chat_id.is_special() {
        return false;
    }

    context
        .sql
        .exists(
            "SELECT id FROM chats WHERE id=? AND (type=120 OR type=130);",
            paramsx![chat_id],
        )
        .await
        .unwrap_or_default()
}

pub(crate) async fn reset_gossiped_timestamp(
    context: &Context,
    chat_id: ChatId,
) -> Result<(), Error> {
    set_gossiped_timestamp(context, chat_id, 0).await
}

/// Get timestamp of the last gossip sent in the chat.
/// Zero return value means that gossip was never sent.
pub async fn get_gossiped_timestamp(context: &Context, chat_id: ChatId) -> i64 {
    context
        .sql
        .query_value(
            "SELECT gossiped_timestamp FROM chats WHERE id=?;",
            paramsx![chat_id],
        )
        .await
        .unwrap_or_default()
}

pub(crate) async fn set_gossiped_timestamp(
    context: &Context,
    chat_id: ChatId,
    timestamp: i64,
) -> Result<(), Error> {
    ensure!(!chat_id.is_special(), "can not add member to special chats");
    info!(
        context,
        "set gossiped_timestamp for chat #{} to {}.", chat_id, timestamp,
    );

    context
        .sql
        .execute(
            "UPDATE chats SET gossiped_timestamp=? WHERE id=?;",
            paramsx![timestamp, chat_id],
        )
        .await?;

    Ok(())
}

pub(crate) async fn shall_attach_selfavatar(
    context: &Context,
    chat_id: ChatId,
) -> Result<bool, Error> {
    // versions before 12/2019 already allowed to set selfavatar, however, it was never sent to others.
    // to avoid sending out previously set selfavatars unexpectedly we added this additional check.
    // it can be removed after some time.
    if !context.sql.get_raw_config_bool("attach_selfavatar").await {
        return Ok(false);
    }

    let timestamp_some_days_ago = time() - DC_RESEND_USER_AVATAR_DAYS * 24 * 60 * 60;
    let needs_attach = context
        .sql
        .query_map(
            r#"
SELECT c.selfavatar_sent
  FROM chats_contacts cc
  LEFT JOIN contacts c ON c.id=cc.contact_id
  WHERE cc.chat_id=? AND cc.contact_id!=?;
"#,
            paramsv![chat_id, DC_CONTACT_ID_SELF],
            |row| Ok(row.get::<_, i64>(0)),
            |rows| {
                let mut needs_attach = false;
                for row in rows {
                    if let Ok(selfavatar_sent) = row {
                        let selfavatar_sent = selfavatar_sent?;
                        if selfavatar_sent < timestamp_some_days_ago {
                            needs_attach = true;
                        }
                    }
                }
                Ok(needs_attach)
            },
        )
        .await?;
    Ok(needs_attach)
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum MuteDuration {
    NotMuted,
    Forever,
    Until(SystemTime),
}

impl sqlx::encode::Encode<sqlx::sqlite::Sqlite> for MuteDuration {
    fn encode(&self, buf: &mut Vec<sqlx::sqlite::SqliteArgumentValue>) {
        let duration: i64 = match &self {
            MuteDuration::NotMuted => 0,
            MuteDuration::Forever => -1,
            MuteDuration::Until(when) => {
                let duration = when.duration_since(SystemTime::UNIX_EPOCH).unwrap();
                i64::try_from(duration.as_secs()).unwrap()
            }
        };

        duration.encode(buf)
    }
}

impl<'de> sqlx::decode::Decode<'de, sqlx::sqlite::Sqlite> for MuteDuration {
    fn decode(value: sqlx::sqlite::SqliteValue<'de>) -> sqlx::Result<Self> {
        // Negative values other than -1 should not be in the
        // database.  If found they'll be NotMuted.
        let raw: i64 = sqlx::decode::Decode::decode(value)?;
        match raw {
            0 => Ok(MuteDuration::NotMuted),
            -1 => Ok(MuteDuration::Forever),
            n if n > 0 => match SystemTime::UNIX_EPOCH.checked_add(Duration::from_secs(n as u64)) {
                Some(t) => Ok(MuteDuration::Until(t)),
                None => Err(sqlx::Error::Decode(
                    anyhow::anyhow!("mute duration out of range: {}", raw).into(),
                )),
            },
            _ => Ok(MuteDuration::NotMuted),
        }
    }
}

impl sqlx::types::Type<sqlx::sqlite::Sqlite> for MuteDuration {
    fn type_info() -> sqlx::sqlite::SqliteTypeInfo {
        <i64 as sqlx::types::Type<_>>::type_info()
    }
}

impl rusqlite::types::ToSql for MuteDuration {
    fn to_sql(&self) -> rusqlite::Result<rusqlite::types::ToSqlOutput> {
        let duration: i64 = match &self {
            MuteDuration::NotMuted => 0,
            MuteDuration::Forever => -1,
            MuteDuration::Until(when) => {
                let duration = when
                    .duration_since(SystemTime::UNIX_EPOCH)
                    .map_err(|err| rusqlite::Error::ToSqlConversionFailure(Box::new(err)))?;
                i64::try_from(duration.as_secs())
                    .map_err(|err| rusqlite::Error::ToSqlConversionFailure(Box::new(err)))?
            }
        };
        let val = rusqlite::types::Value::Integer(duration);
        let out = rusqlite::types::ToSqlOutput::Owned(val);
        Ok(out)
    }
}

impl rusqlite::types::FromSql for MuteDuration {
    fn column_result(value: rusqlite::types::ValueRef) -> rusqlite::types::FromSqlResult<Self> {
        // Negative values other than -1 should not be in the
        // database.  If found they'll be NotMuted.
        match i64::column_result(value)? {
            0 => Ok(MuteDuration::NotMuted),
            -1 => Ok(MuteDuration::Forever),
            n if n > 0 => match SystemTime::UNIX_EPOCH.checked_add(Duration::from_secs(n as u64)) {
                Some(t) => Ok(MuteDuration::Until(t)),
                None => Err(rusqlite::types::FromSqlError::OutOfRange(n)),
            },
            _ => Ok(MuteDuration::NotMuted),
        }
    }
}

pub async fn set_muted(
    context: &Context,
    chat_id: ChatId,
    duration: MuteDuration,
) -> Result<(), Error> {
    ensure!(!chat_id.is_special(), "Invalid chat ID");
    if context
        .sql
        .execute(
            "UPDATE chats SET muted_until=? WHERE id=?;",
            paramsx![duration, chat_id],
        )
        .await
        .is_ok()
    {
        context.emit_event(Event::ChatModified(chat_id));
    } else {
        bail!("Failed to set mute duration, chat might not exist -");
    }
    Ok(())
}

pub async fn remove_contact_from_chat(
    context: &Context,
    chat_id: ChatId,
    contact_id: u32,
) -> Result<(), Error> {
    ensure!(
        !chat_id.is_special(),
        "bad chat_id, can not be special chat: {}",
        chat_id
    );
    ensure!(
        contact_id > DC_CONTACT_ID_LAST_SPECIAL || contact_id == DC_CONTACT_ID_SELF,
        "Cannot remove special contact"
    );

    let mut msg = Message::default();
    let mut success = false;

    /* we do not check if "contact_id" exists but just delete all records with the id from chats_contacts */
    /* this allows to delete pending references to deleted contacts.  Of course, this should _not_ happen. */
    if let Ok(chat) = Chat::load_from_db(context, chat_id).await {
        if real_group_exists(context, chat_id).await {
            if !is_contact_in_chat(context, chat_id, DC_CONTACT_ID_SELF).await {
                emit_event!(
                    context,
                    Event::ErrorSelfNotInGroup(
                        "Cannot remove contact from chat; self not in group.".into()
                    )
                );
            } else {
                if let Ok(contact) = Contact::get_by_id(context, contact_id).await {
                    if chat.is_promoted() {
                        msg.viewtype = Viewtype::Text;
                        if contact.id == DC_CONTACT_ID_SELF {
                            set_group_explicitly_left(context, chat.grpid).await?;
                            msg.text = Some(
                                context
                                    .stock_system_msg(
                                        StockMessage::MsgGroupLeft,
                                        "",
                                        "",
                                        DC_CONTACT_ID_SELF,
                                    )
                                    .await,
                            );
                        } else {
                            msg.text = Some(
                                context
                                    .stock_system_msg(
                                        StockMessage::MsgDelMember,
                                        contact.get_addr(),
                                        "",
                                        DC_CONTACT_ID_SELF,
                                    )
                                    .await,
                            );
                        }
                        msg.param.set_cmd(SystemMessage::MemberRemovedFromGroup);
                        msg.param.set(Param::Arg, contact.get_addr());
                        msg.id = send_msg(context, chat_id, &mut msg).await?;
                    }
                }
                // we remove the member from the chat after constructing the
                // to-be-send message. If between send_msg() and here the
                // process dies the user will have to re-do the action.  It's
                // better than the other way round: you removed
                // someone from DB but no peer or device gets to know about it and
                // group membership is thus different on different devices.
                // Note also that sending a message needs all recipients
                // in order to correctly determine encryption so if we
                // removed it first, it would complicate the
                // check/encryption logic.
                success = remove_from_chat_contacts_table(context, chat_id, contact_id).await;
                context.emit_event(Event::ChatModified(chat_id));
            }
        }
    }

    if !success {
        bail!("Failed to remove contact");
    }

    Ok(())
}

async fn set_group_explicitly_left(context: &Context, grpid: impl AsRef<str>) -> Result<(), Error> {
    if !is_group_explicitly_left(context, grpid.as_ref()).await? {
        context
            .sql
            .execute(
                "INSERT INTO leftgrps (grpid) VALUES(?);",
                paramsx![grpid.as_ref()],
            )
            .await?;
    }

    Ok(())
}

pub(crate) async fn is_group_explicitly_left(
    context: &Context,
    grpid: impl AsRef<str>,
) -> Result<bool, Error> {
    context
        .sql
        .exists(
            "SELECT id FROM leftgrps WHERE grpid=?;",
            paramsx![grpid.as_ref()],
        )
        .await
        .map_err(Into::into)
}

pub async fn set_chat_name(
    context: &Context,
    chat_id: ChatId,
    new_name: impl AsRef<str>,
) -> Result<(), Error> {
    /* the function only sets the names of group chats; normal chats get their names from the contacts */
    let mut success = false;

    ensure!(!new_name.as_ref().is_empty(), "Invalid name");
    ensure!(!chat_id.is_special(), "Invalid chat ID");

    let chat = Chat::load_from_db(context, chat_id).await?;
    let mut msg = Message::default();

    if real_group_exists(context, chat_id).await {
        if chat.name == new_name.as_ref() {
            success = true;
        } else if !is_contact_in_chat(context, chat_id, DC_CONTACT_ID_SELF).await {
            emit_event!(
                context,
                Event::ErrorSelfNotInGroup("Cannot set chat name; self not in group".into())
            );
        } else {
            /* we should respect this - whatever we send to the group, it gets discarded anyway! */
            if context
                .sql
                .execute(
                    "UPDATE chats SET name=? WHERE id=?;",
                    paramsx![new_name.as_ref(), chat_id],
                )
                .await
                .is_ok()
            {
                if chat.is_promoted() {
                    msg.viewtype = Viewtype::Text;
                    msg.text = Some(
                        context
                            .stock_system_msg(
                                StockMessage::MsgGrpName,
                                &chat.name,
                                new_name.as_ref(),
                                DC_CONTACT_ID_SELF,
                            )
                            .await,
                    );
                    msg.param.set_cmd(SystemMessage::GroupNameChanged);
                    if !chat.name.is_empty() {
                        msg.param.set(Param::Arg, &chat.name);
                    }
                    msg.id = send_msg(context, chat_id, &mut msg).await?;
                    context.emit_event(Event::MsgsChanged {
                        chat_id,
                        msg_id: msg.id,
                    });
                }
                context.emit_event(Event::ChatModified(chat_id));
                success = true;
            }
        }
    }

    if !success {
        bail!("Failed to set name");
    }

    Ok(())
}

/// Set a new profile image for the chat.
///
/// The profile image can only be set when you are a member of the
/// chat.  To remove the profile image pass an empty string for the
/// `new_image` parameter.
pub async fn set_chat_profile_image(
    context: &Context,
    chat_id: ChatId,
    new_image: impl AsRef<str>, // XXX use PathBuf
) -> Result<(), Error> {
    ensure!(!chat_id.is_special(), "Invalid chat ID");
    let mut chat = Chat::load_from_db(context, chat_id).await?;
    ensure!(
        real_group_exists(context, chat_id).await,
        "Failed to set profile image; group does not exist"
    );
    /* we should respect this - whatever we send to the group, it gets discarded anyway! */
    if !is_contact_in_chat(context, chat_id, DC_CONTACT_ID_SELF).await {
        emit_event!(
            context,
            Event::ErrorSelfNotInGroup("Cannot set chat profile image; self not in group.".into())
        );
        bail!("Failed to set profile image");
    }
    let mut msg = Message::new(Viewtype::Text);
    msg.param
        .set_int(Param::Cmd, SystemMessage::GroupImageChanged as i32);
    if new_image.as_ref().is_empty() {
        chat.param.remove(Param::ProfileImage);
        msg.param.remove(Param::Arg);
        msg.text = Some(
            context
                .stock_system_msg(StockMessage::MsgGrpImgDeleted, "", "", DC_CONTACT_ID_SELF)
                .await,
        );
    } else {
        let image_blob = match BlobObject::from_path(context, Path::new(new_image.as_ref())) {
            Ok(blob) => Ok(blob),
            Err(err) => match err {
                BlobError::WrongBlobdir { .. } => {
                    BlobObject::create_and_copy(context, Path::new(new_image.as_ref())).await
                }
                _ => Err(err),
            },
        }?;
        image_blob.recode_to_avatar_size(context)?;
        chat.param.set(Param::ProfileImage, image_blob.as_name());
        msg.param.set(Param::Arg, image_blob.as_name());
        msg.text = Some(
            context
                .stock_system_msg(StockMessage::MsgGrpImgChanged, "", "", DC_CONTACT_ID_SELF)
                .await,
        );
    }
    chat.update_param(context).await?;
    if chat.is_promoted() {
        msg.id = send_msg(context, chat_id, &mut msg).await?;
        emit_event!(
            context,
            Event::MsgsChanged {
                chat_id,
                msg_id: msg.id
            }
        );
    }
    emit_event!(context, Event::ChatModified(chat_id));
    Ok(())
}

pub async fn forward_msgs(
    context: &Context,
    msg_ids: &[MsgId],
    chat_id: ChatId,
) -> Result<(), Error> {
    ensure!(!msg_ids.is_empty(), "empty msgs_ids: nothing to forward");
    ensure!(!chat_id.is_special(), "can not forward to special chat");

    let mut created_chats: Vec<ChatId> = Vec::new();
    let mut created_msgs: Vec<MsgId> = Vec::new();
    let mut curr_timestamp: i64;

    chat_id.unarchive(context).await?;
    if let Ok(mut chat) = Chat::load_from_db(context, chat_id).await {
        ensure!(chat.can_send(), "cannot send to {}", chat_id);
        curr_timestamp = dc_create_smeared_timestamps(context, msg_ids.len()).await;
        let ids = context
            .sql
            .query_map(
                format!(
                    "SELECT id FROM msgs WHERE id IN({}) ORDER BY timestamp,id",
                    msg_ids.iter().map(|_| "?").join(",")
                ),
                msg_ids.iter().map(|v| v as &dyn crate::ToSql).collect(),
                |row| row.get::<_, MsgId>(0),
                |ids| ids.collect::<Result<Vec<_>, _>>().map_err(Into::into),
            )
            .await?;

        for id in ids {
            let src_msg_id: MsgId = id;
            let msg = Message::load_from_db(context, src_msg_id).await;
            if msg.is_err() {
                break;
            }
            let mut msg = msg.unwrap();
            let original_param = msg.param.clone();

            // we tested a sort of broadcast
            // by not marking own forwarded messages as such,
            // however, this turned out to be to confusing and unclear.
            msg.param.set_int(Param::Forwarded, 1);

            msg.param.remove(Param::GuaranteeE2ee);
            msg.param.remove(Param::ForcePlaintext);
            msg.param.remove(Param::Cmd);

            let new_msg_id: MsgId;
            if msg.state == MessageState::OutPreparing {
                let fresh9 = curr_timestamp;
                curr_timestamp += 1;
                new_msg_id = chat.prepare_msg_raw(context, &mut msg, fresh9).await?;
                let save_param = msg.param.clone();
                msg.param = original_param;
                msg.id = src_msg_id;

                if let Some(old_fwd) = msg.param.get(Param::PrepForwards) {
                    let new_fwd = format!("{} {}", old_fwd, new_msg_id.to_u32());
                    msg.param.set(Param::PrepForwards, new_fwd);
                } else {
                    msg.param
                        .set(Param::PrepForwards, new_msg_id.to_u32().to_string());
                }

                msg.save_param_to_disk(context).await;
                msg.param = save_param;
            } else {
                msg.state = MessageState::OutPending;
                let fresh10 = curr_timestamp;
                curr_timestamp += 1;
                new_msg_id = chat.prepare_msg_raw(context, &mut msg, fresh10).await?;
                if let Some(send_job) = job::send_msg_job(context, new_msg_id).await? {
                    job::add(context, send_job).await;
                }
            }
            created_chats.push(chat_id);
            created_msgs.push(new_msg_id);
        }
    }
    for (chat_id, msg_id) in created_chats.iter().zip(created_msgs.iter()) {
        context.emit_event(Event::MsgsChanged {
            chat_id: *chat_id,
            msg_id: *msg_id,
        });
    }
    Ok(())
}

pub(crate) async fn get_chat_contact_cnt(context: &Context, chat_id: ChatId) -> usize {
    let v: i32 = context
        .sql
        .query_value(
            "SELECT COUNT(*) FROM chats_contacts WHERE chat_id=?;",
            paramsx![chat_id],
        )
        .await
        .unwrap_or_default();
    v as usize
}

pub(crate) async fn get_chat_cnt(context: &Context) -> usize {
    if context.sql.is_open().await {
        /* no database, no chats - this is no error (needed eg. for information) */
        let v: i32 = context
            .sql
            .query_value(
                "SELECT COUNT(*) FROM chats WHERE id>9 AND blocked=0;",
                paramsx![],
            )
            .await
            .unwrap_or_default();
        v as usize
    } else {
        0
    }
}

pub(crate) async fn get_chat_id_by_grpid(
    context: &Context,
    grpid: impl AsRef<str>,
) -> Result<(ChatId, bool, Blocked), sql::Error> {
    let (chat_id, blocked, typ): (ChatId, Blocked, Chattype) = context
        .sql
        .query_row(
            "SELECT id, blocked, type FROM chats WHERE grpid=?;",
            paramsx![grpid.as_ref()],
        )
        .await?;

    Ok((chat_id, typ == Chattype::VerifiedGroup, blocked))
}

/// Adds a message to device chat.
///
/// Optional `label` can be provided to ensure that message is added only once.
pub async fn add_device_msg(
    context: &Context,
    label: Option<&str>,
    msg: Option<&mut Message>,
) -> Result<MsgId, Error> {
    ensure!(
        label.is_some() || msg.is_some(),
        "device-messages need label, msg or both"
    );
    let mut chat_id = ChatId::new(0);
    let mut msg_id = MsgId::new_unset();

    if let Some(label) = label {
        if was_device_msg_ever_added(context, label).await? {
            info!(context, "device-message {} already added", label);
            return Ok(msg_id);
        }
    }

    if let Some(msg) = msg {
        chat_id = create_or_lookup_by_contact_id(context, DC_CONTACT_ID_DEVICE, Blocked::Not)
            .await?
            .0;

        let rfc724_mid = dc_create_outgoing_rfc724_mid(None, "@device");
        msg.try_calc_and_set_dimensions(context).await.ok();
        prepare_msg_blob(context, msg).await?;
        chat_id.unarchive(context).await?;

        context
            .sql
            .execute(
                r#"
INSERT INTO msgs (chat_id, from_id, to_id, timestamp, type, state, txt, param, rfc724_mid)
  VALUES (?,?,?, ?,?,?, ?,?,?);
"#,
                paramsx![
                    chat_id,
                    DC_CONTACT_ID_DEVICE as i32,
                    DC_CONTACT_ID_SELF as i32,
                    dc_create_smeared_timestamp(context).await,
                    msg.viewtype,
                    MessageState::InFresh,
                    msg.text.as_ref().cloned().unwrap_or_default(),
                    msg.param.to_string(),
                    &rfc724_mid
                ],
            )
            .await?;

        let row_id = context
            .sql
            .get_rowid("msgs", "rfc724_mid", &rfc724_mid)
            .await?;
        msg_id = MsgId::new(row_id);
    }

    if let Some(label) = label {
        context
            .sql
            .execute(
                "INSERT INTO devmsglabels (label) VALUES (?);",
                paramsx![label],
            )
            .await?;
    }

    if !msg_id.is_unset() {
        context.emit_event(Event::IncomingMsg { chat_id, msg_id });
    }

    Ok(msg_id)
}

pub async fn was_device_msg_ever_added(context: &Context, label: &str) -> Result<bool, Error> {
    ensure!(!label.is_empty(), "empty label");

    if let Ok(count) = context
        .sql
        .execute(
            "SELECT label FROM devmsglabels WHERE label=?",
            paramsx![label],
        )
        .await
    {
        return Ok(count > 0);
    }

    Ok(false)
}

// needed on device-switches during export/import;
// - deletion in `msgs` with `DC_CONTACT_ID_DEVICE` makes sure,
//   no wrong information are shown in the device chat
// - deletion in `devmsglabels` makes sure,
//   deleted messages are resetted and useful messages can be added again
pub(crate) async fn delete_and_reset_all_device_msgs(context: &Context) -> Result<(), Error> {
    context
        .sql
        .execute(
            "DELETE FROM msgs WHERE from_id=?;",
            paramsx![DC_CONTACT_ID_DEVICE as i32],
        )
        .await?;
    context
        .sql
        .execute("DELETE FROM devmsglabels;", paramsx![])
        .await?;
    Ok(())
}

/// Adds an informational message to chat.
///
/// For example, it can be a message showing that a member was added to a group.
pub(crate) async fn add_info_msg(context: &Context, chat_id: ChatId, text: impl AsRef<str>) {
    let rfc724_mid = dc_create_outgoing_rfc724_mid(None, "@device");

    if context.sql.execute(
        "INSERT INTO msgs (chat_id,from_id,to_id, timestamp,type,state, txt,rfc724_mid) VALUES (?,?,?, ?,?,?, ?,?);",
        paramsx![
            chat_id,
            DC_CONTACT_ID_INFO as i32,
            DC_CONTACT_ID_INFO as i32,
            dc_create_smeared_timestamp(context).await,
            Viewtype::Text,
            MessageState::InNoticed,
            text.as_ref(),
            &rfc724_mid
        ]
    ).await.is_err() {
        return;
    }

    let row_id = context
        .sql
        .get_rowid("msgs", "rfc724_mid", &rfc724_mid)
        .await
        .unwrap_or_default();
    context.emit_event(Event::MsgsChanged {
        chat_id,
        msg_id: MsgId::new(row_id),
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::contact::Contact;
    use crate::test_utils::*;

    #[async_std::test]
    async fn test_chat_info() {
        let t = dummy_context().await;
        let bob = Contact::create(&t.ctx, "bob", "bob@example.com")
            .await
            .unwrap();
        let chat_id = create_by_contact_id(&t.ctx, bob).await.unwrap();
        let chat = Chat::load_from_db(&t.ctx, chat_id).await.unwrap();
        let info = chat.get_info(&t.ctx).await.unwrap();

        // Ensure we can serialize this.
        println!("{}", serde_json::to_string_pretty(&info).unwrap());

        let expected = r#"
            {
                "id": 10,
                "type": 100,
                "name": "bob",
                "archived": false,
                "param": "",
                "gossiped_timestamp": 0,
                "is_sending_locations": false,
                "color": 15895624,
                "profile_image": "",
                "draft": "",
                "is_muted": false
            }
        "#;

        // Ensure we can deserialize this.
        let loaded: ChatInfo = serde_json::from_str(expected).unwrap();
        assert_eq!(info, loaded);
    }

    #[async_std::test]
    async fn test_get_draft_no_draft() {
        let t = dummy_context().await;
        let chat_id = create_by_contact_id(&t.ctx, DC_CONTACT_ID_SELF)
            .await
            .unwrap();
        let draft = chat_id.get_draft(&t.ctx).await.unwrap();
        assert!(draft.is_none());
    }

    #[async_std::test]
    async fn test_get_draft_special_chat_id() {
        let t = dummy_context().await;
        let draft = ChatId::new(DC_CHAT_ID_LAST_SPECIAL)
            .get_draft(&t.ctx)
            .await
            .unwrap();
        assert!(draft.is_none());
    }

    #[async_std::test]
    async fn test_get_draft_no_chat() {
        // This is a weird case, maybe this should be an error but we
        // do not get this info from the database currently.
        let t = dummy_context().await;
        let draft = ChatId::new(42).get_draft(&t.ctx).await.unwrap();
        assert!(draft.is_none());
    }

    #[async_std::test]
    async fn test_get_draft() {
        let t = dummy_context().await;
        let chat_id = create_by_contact_id(&t.ctx, DC_CONTACT_ID_SELF)
            .await
            .unwrap();
        let mut msg = Message::new(Viewtype::Text);
        msg.set_text(Some("hello".to_string()));
        chat_id.set_draft(&t.ctx, Some(&mut msg)).await;
        let draft = chat_id.get_draft(&t.ctx).await.unwrap().unwrap();
        let msg_text = msg.get_text();
        let draft_text = draft.get_text();
        assert_eq!(msg_text, draft_text);
    }

    #[async_std::test]
    async fn test_add_contact_to_chat_ex_add_self() {
        // Adding self to a contact should succeed, even though it's pointless.
        let t = test_context().await;
        let chat_id = create_group_chat(&t.ctx, VerifiedStatus::Unverified, "foo")
            .await
            .unwrap();
        let added = add_contact_to_chat_ex(&t.ctx, chat_id, DC_CONTACT_ID_SELF, false)
            .await
            .unwrap();
        assert_eq!(added, false);
    }

    #[async_std::test]
    async fn test_self_talk() {
        let t = dummy_context().await;
        let chat_id = create_by_contact_id(&t.ctx, DC_CONTACT_ID_SELF)
            .await
            .unwrap();
        assert_eq!(DC_CONTACT_ID_SELF, 1);
        assert!(!chat_id.is_special());
        let chat = Chat::load_from_db(&t.ctx, chat_id).await.unwrap();
        assert_eq!(chat.id, chat_id);
        assert!(chat.is_self_talk());
        assert!(chat.visibility == ChatVisibility::Normal);
        assert!(!chat.is_device_talk());
        assert!(chat.can_send());
        assert_eq!(
            chat.name,
            t.ctx.stock_str(StockMessage::SavedMessages).await
        );
        assert!(chat.get_profile_image(&t.ctx).await.is_some());
    }

    #[async_std::test]
    async fn test_deaddrop_chat() {
        let t = dummy_context().await;
        let chat = Chat::load_from_db(&t.ctx, ChatId::new(DC_CHAT_ID_DEADDROP))
            .await
            .unwrap();
        assert_eq!(DC_CHAT_ID_DEADDROP, 1);
        assert!(chat.id.is_deaddrop());
        assert!(!chat.is_self_talk());
        assert!(chat.visibility == ChatVisibility::Normal);
        assert!(!chat.is_device_talk());
        assert!(!chat.can_send());
        assert_eq!(chat.name, t.ctx.stock_str(StockMessage::DeadDrop).await);
    }

    #[async_std::test]
    async fn test_add_device_msg_unlabelled() {
        let t = test_context().await;

        // add two device-messages
        let mut msg1 = Message::new(Viewtype::Text);
        msg1.text = Some("first message".to_string());
        let msg1_id = add_device_msg(&t.ctx, None, Some(&mut msg1)).await;
        assert!(msg1_id.is_ok());

        let mut msg2 = Message::new(Viewtype::Text);
        msg2.text = Some("second message".to_string());
        let msg2_id = add_device_msg(&t.ctx, None, Some(&mut msg2)).await;
        assert!(msg2_id.is_ok());
        assert_ne!(msg1_id.as_ref().unwrap(), msg2_id.as_ref().unwrap());

        // check added messages
        let msg1 = message::Message::load_from_db(&t.ctx, msg1_id.unwrap()).await;
        assert!(msg1.is_ok());
        let msg1 = msg1.unwrap();
        assert_eq!(msg1.text.as_ref().unwrap(), "first message");
        assert_eq!(msg1.from_id, DC_CONTACT_ID_DEVICE);
        assert_eq!(msg1.to_id, DC_CONTACT_ID_SELF);
        assert!(!msg1.is_info());
        assert!(!msg1.is_setupmessage());

        let msg2 = message::Message::load_from_db(&t.ctx, msg2_id.unwrap()).await;
        assert!(msg2.is_ok());
        let msg2 = msg2.unwrap();
        assert_eq!(msg2.text.as_ref().unwrap(), "second message");

        // check device chat
        assert_eq!(msg2.chat_id.get_msg_cnt(&t.ctx).await, 2);
    }

    #[async_std::test]
    async fn test_add_device_msg_labelled() {
        let t = test_context().await;

        // add two device-messages with the same label (second attempt is not added)
        let mut msg1 = Message::new(Viewtype::Text);
        msg1.text = Some("first message".to_string());
        let msg1_id = add_device_msg(&t.ctx, Some("any-label"), Some(&mut msg1)).await;
        assert!(msg1_id.is_ok());
        assert!(!msg1_id.as_ref().unwrap().is_unset());

        let mut msg2 = Message::new(Viewtype::Text);
        msg2.text = Some("second message".to_string());
        let msg2_id = add_device_msg(&t.ctx, Some("any-label"), Some(&mut msg2)).await;
        assert!(msg2_id.is_ok());
        assert!(msg2_id.as_ref().unwrap().is_unset());

        // check added message
        let msg1 = message::Message::load_from_db(&t.ctx, *msg1_id.as_ref().unwrap()).await;
        assert!(msg1.is_ok());
        let msg1 = msg1.unwrap();
        assert_eq!(msg1_id.as_ref().unwrap(), &msg1.id);
        assert_eq!(msg1.text.as_ref().unwrap(), "first message");
        assert_eq!(msg1.from_id, DC_CONTACT_ID_DEVICE);
        assert_eq!(msg1.to_id, DC_CONTACT_ID_SELF);
        assert!(!msg1.is_info());
        assert!(!msg1.is_setupmessage());

        // check device chat
        let chat_id = msg1.chat_id;
        assert_eq!(chat_id.get_msg_cnt(&t.ctx).await, 1);
        assert!(!chat_id.is_special());
        let chat = Chat::load_from_db(&t.ctx, chat_id).await;
        assert!(chat.is_ok());
        let chat = chat.unwrap();
        assert_eq!(chat.get_type(), Chattype::Single);
        assert!(chat.is_device_talk());
        assert!(!chat.is_self_talk());
        assert!(!chat.can_send());
        assert_eq!(
            chat.name,
            t.ctx.stock_str(StockMessage::DeviceMessages).await
        );
        assert!(chat.get_profile_image(&t.ctx).await.is_some());

        // delete device message, make sure it is not added again
        message::delete_msgs(&t.ctx, &[*msg1_id.as_ref().unwrap()]).await;
        let msg1 = message::Message::load_from_db(&t.ctx, *msg1_id.as_ref().unwrap()).await;
        assert!(msg1.is_err() || msg1.unwrap().chat_id.is_trash());
        let msg3_id = add_device_msg(&t.ctx, Some("any-label"), Some(&mut msg2)).await;
        assert!(msg3_id.is_ok());
        assert!(msg2_id.as_ref().unwrap().is_unset());
    }

    #[async_std::test]
    async fn test_add_device_msg_label_only() {
        let t = test_context().await;
        let res = add_device_msg(&t.ctx, Some(""), None).await;
        assert!(res.is_err());
        let res = add_device_msg(&t.ctx, Some("some-label"), None).await;
        assert!(res.is_ok());

        let mut msg = Message::new(Viewtype::Text);
        msg.text = Some("message text".to_string());

        let msg_id = add_device_msg(&t.ctx, Some("some-label"), Some(&mut msg)).await;
        assert!(msg_id.is_ok());
        assert!(msg_id.as_ref().unwrap().is_unset());

        let msg_id = add_device_msg(&t.ctx, Some("unused-label"), Some(&mut msg)).await;
        assert!(msg_id.is_ok());
        assert!(!msg_id.as_ref().unwrap().is_unset());
    }

    #[async_std::test]
    async fn test_was_device_msg_ever_added() {
        let t = test_context().await;
        add_device_msg(&t.ctx, Some("some-label"), None).await.ok();
        assert!(was_device_msg_ever_added(&t.ctx, "some-label")
            .await
            .unwrap());

        let mut msg = Message::new(Viewtype::Text);
        msg.text = Some("message text".to_string());
        add_device_msg(&t.ctx, Some("another-label"), Some(&mut msg))
            .await
            .ok();
        assert!(was_device_msg_ever_added(&t.ctx, "another-label")
            .await
            .unwrap());

        assert!(!was_device_msg_ever_added(&t.ctx, "unused-label")
            .await
            .unwrap());

        assert!(was_device_msg_ever_added(&t.ctx, "").await.is_err());
    }

    #[async_std::test]
    async fn test_delete_device_chat() {
        let t = test_context().await;

        let mut msg = Message::new(Viewtype::Text);
        msg.text = Some("message text".to_string());
        add_device_msg(&t.ctx, Some("some-label"), Some(&mut msg))
            .await
            .ok();
        let chats = Chatlist::try_load(&t.ctx, 0, None, None).await.unwrap();
        assert_eq!(chats.len(), 1);

        // after the device-chat and all messages are deleted, a re-adding should do nothing
        chats.get_chat_id(0).delete(&t.ctx).await.ok();
        add_device_msg(&t.ctx, Some("some-label"), Some(&mut msg))
            .await
            .ok();
        assert_eq!(chatlist_len(&t.ctx, 0).await, 0)
    }

    #[async_std::test]
    async fn test_device_chat_cannot_sent() {
        let t = test_context().await;
        t.ctx.update_device_chats().await.unwrap();
        let (device_chat_id, _) =
            create_or_lookup_by_contact_id(&t.ctx, DC_CONTACT_ID_DEVICE, Blocked::Not)
                .await
                .unwrap();

        let mut msg = Message::new(Viewtype::Text);
        msg.text = Some("message text".to_string());
        assert!(send_msg(&t.ctx, device_chat_id, &mut msg).await.is_err());
        assert!(prepare_msg(&t.ctx, device_chat_id, &mut msg).await.is_err());

        let msg_id = add_device_msg(&t.ctx, None, Some(&mut msg)).await.unwrap();
        assert!(forward_msgs(&t.ctx, &[msg_id], device_chat_id)
            .await
            .is_err());
    }

    #[async_std::test]
    async fn test_delete_and_reset_all_device_msgs() {
        let t = test_context().await;
        let mut msg = Message::new(Viewtype::Text);
        msg.text = Some("message text".to_string());
        let msg_id1 = add_device_msg(&t.ctx, Some("some-label"), Some(&mut msg))
            .await
            .unwrap();

        // adding a device message with the same label won't be executed again ...
        assert!(was_device_msg_ever_added(&t.ctx, "some-label")
            .await
            .unwrap());
        let msg_id2 = add_device_msg(&t.ctx, Some("some-label"), Some(&mut msg))
            .await
            .unwrap();
        assert!(msg_id2.is_unset());

        // ... unless everything is deleted and resetted - as needed eg. on device switch
        delete_and_reset_all_device_msgs(&t.ctx).await.unwrap();
        assert!(!was_device_msg_ever_added(&t.ctx, "some-label")
            .await
            .unwrap());
        let msg_id3 = add_device_msg(&t.ctx, Some("some-label"), Some(&mut msg))
            .await
            .unwrap();
        assert_ne!(msg_id1, msg_id3);
    }

    async fn chatlist_len(ctx: &Context, listflags: usize) -> usize {
        Chatlist::try_load(ctx, listflags, None, None)
            .await
            .unwrap()
            .len()
    }

    #[async_std::test]
    async fn test_archive() {
        // create two chats
        let t = dummy_context().await;
        let mut msg = Message::new(Viewtype::Text);
        msg.text = Some("foo".to_string());
        println!("foo");
        let msg_id = add_device_msg(&t.ctx, None, Some(&mut msg))
            .await
            .expect("failed to add device msg");
        let chat_id1 = message::Message::load_from_db(&t.ctx, msg_id)
            .await
            .expect("failed to load message")
            .chat_id;
        let chat_id2 = create_by_contact_id(&t.ctx, DC_CONTACT_ID_SELF)
            .await
            .unwrap();
        assert!(!chat_id1.is_special());
        assert!(!chat_id2.is_special());
        assert_eq!(get_chat_cnt(&t.ctx).await, 2);
        assert_eq!(chatlist_len(&t.ctx, 0).await, 2);
        assert_eq!(chatlist_len(&t.ctx, DC_GCL_NO_SPECIALS).await, 2);
        assert_eq!(chatlist_len(&t.ctx, DC_GCL_ARCHIVED_ONLY).await, 0);
        assert_eq!(DC_GCL_ARCHIVED_ONLY, 0x01);
        assert_eq!(DC_GCL_NO_SPECIALS, 0x02);

        // archive first chat
        assert!(chat_id1
            .set_visibility(&t.ctx, ChatVisibility::Archived)
            .await
            .is_ok());
        assert!(
            Chat::load_from_db(&t.ctx, chat_id1)
                .await
                .unwrap()
                .get_visibility()
                == ChatVisibility::Archived
        );
        assert!(
            Chat::load_from_db(&t.ctx, chat_id2)
                .await
                .unwrap()
                .get_visibility()
                == ChatVisibility::Normal
        );
        assert_eq!(get_chat_cnt(&t.ctx).await, 2);
        assert_eq!(chatlist_len(&t.ctx, 0).await, 2); // including DC_CHAT_ID_ARCHIVED_LINK now
        assert_eq!(chatlist_len(&t.ctx, DC_GCL_NO_SPECIALS).await, 1);
        assert_eq!(chatlist_len(&t.ctx, DC_GCL_ARCHIVED_ONLY).await, 1);

        // archive second chat
        assert!(chat_id2
            .set_visibility(&t.ctx, ChatVisibility::Archived)
            .await
            .is_ok());
        assert!(
            Chat::load_from_db(&t.ctx, chat_id1)
                .await
                .unwrap()
                .get_visibility()
                == ChatVisibility::Archived
        );
        assert!(
            Chat::load_from_db(&t.ctx, chat_id2)
                .await
                .unwrap()
                .get_visibility()
                == ChatVisibility::Archived
        );
        assert_eq!(get_chat_cnt(&t.ctx).await, 2);
        assert_eq!(chatlist_len(&t.ctx, 0).await, 1); // only DC_CHAT_ID_ARCHIVED_LINK now
        assert_eq!(chatlist_len(&t.ctx, DC_GCL_NO_SPECIALS).await, 0);
        assert_eq!(chatlist_len(&t.ctx, DC_GCL_ARCHIVED_ONLY).await, 2);

        // archive already archived first chat, unarchive second chat two times
        assert!(chat_id1
            .set_visibility(&t.ctx, ChatVisibility::Archived)
            .await
            .is_ok());
        assert!(chat_id2
            .set_visibility(&t.ctx, ChatVisibility::Normal)
            .await
            .is_ok());
        assert!(chat_id2
            .set_visibility(&t.ctx, ChatVisibility::Normal)
            .await
            .is_ok());
        assert!(
            Chat::load_from_db(&t.ctx, chat_id1)
                .await
                .unwrap()
                .get_visibility()
                == ChatVisibility::Archived
        );
        assert!(
            Chat::load_from_db(&t.ctx, chat_id2)
                .await
                .unwrap()
                .get_visibility()
                == ChatVisibility::Normal
        );
        assert_eq!(get_chat_cnt(&t.ctx).await, 2);
        assert_eq!(chatlist_len(&t.ctx, 0).await, 2);
        assert_eq!(chatlist_len(&t.ctx, DC_GCL_NO_SPECIALS).await, 1);
        assert_eq!(chatlist_len(&t.ctx, DC_GCL_ARCHIVED_ONLY).await, 1);
    }

    async fn get_chats_from_chat_list(ctx: &Context, listflags: usize) -> Vec<ChatId> {
        let chatlist = Chatlist::try_load(ctx, listflags, None, None)
            .await
            .unwrap();
        let mut result = Vec::new();
        for chatlist_index in 0..chatlist.len() {
            result.push(chatlist.get_chat_id(chatlist_index))
        }
        result
    }

    #[async_std::test]
    async fn test_pinned() {
        let t = dummy_context().await;

        // create 3 chats, wait 1 second in between to get a reliable order (we order by time)
        let mut msg = Message::new(Viewtype::Text);
        msg.text = Some("foo".to_string());
        let msg_id = add_device_msg(&t.ctx, None, Some(&mut msg)).await.unwrap();
        let chat_id1 = message::Message::load_from_db(&t.ctx, msg_id)
            .await
            .unwrap()
            .chat_id;
        async_std::task::sleep(std::time::Duration::from_millis(1000)).await;
        let chat_id2 = create_by_contact_id(&t.ctx, DC_CONTACT_ID_SELF)
            .await
            .unwrap();
        async_std::task::sleep(std::time::Duration::from_millis(1000)).await;
        let chat_id3 = create_group_chat(&t.ctx, VerifiedStatus::Unverified, "foo")
            .await
            .unwrap();

        let chatlist = get_chats_from_chat_list(&t.ctx, DC_GCL_NO_SPECIALS).await;
        assert_eq!(chatlist, vec![chat_id3, chat_id2, chat_id1]);

        // pin
        assert!(chat_id1
            .set_visibility(&t.ctx, ChatVisibility::Pinned)
            .await
            .is_ok());
        assert_eq!(
            Chat::load_from_db(&t.ctx, chat_id1)
                .await
                .unwrap()
                .get_visibility(),
            ChatVisibility::Pinned
        );

        // check if chat order changed
        let chatlist = get_chats_from_chat_list(&t.ctx, DC_GCL_NO_SPECIALS).await;
        assert_eq!(chatlist, vec![chat_id1, chat_id3, chat_id2]);

        // unpin
        assert!(chat_id1
            .set_visibility(&t.ctx, ChatVisibility::Normal)
            .await
            .is_ok());
        assert_eq!(
            Chat::load_from_db(&t.ctx, chat_id1)
                .await
                .unwrap()
                .get_visibility(),
            ChatVisibility::Normal
        );

        // check if chat order changed back
        let chatlist = get_chats_from_chat_list(&t.ctx, DC_GCL_NO_SPECIALS).await;
        assert_eq!(chatlist, vec![chat_id3, chat_id2, chat_id1]);
    }

    #[async_std::test]
    async fn test_set_chat_name() {
        let t = dummy_context().await;
        let chat_id = create_group_chat(&t.ctx, VerifiedStatus::Unverified, "foo")
            .await
            .unwrap();
        assert_eq!(
            Chat::load_from_db(&t.ctx, chat_id)
                .await
                .unwrap()
                .get_name(),
            "foo"
        );

        set_chat_name(&t.ctx, chat_id, "bar").await.unwrap();
        assert_eq!(
            Chat::load_from_db(&t.ctx, chat_id)
                .await
                .unwrap()
                .get_name(),
            "bar"
        );
    }

    #[async_std::test]
    async fn test_create_same_chat_twice() {
        let context = dummy_context().await;
        let contact1 = Contact::create(&context.ctx, "bob", "bob@mail.de")
            .await
            .unwrap();
        assert_ne!(contact1, 0);

        let chat_id = create_by_contact_id(&context.ctx, contact1).await.unwrap();
        assert!(!chat_id.is_special(), "chat_id too small {}", chat_id);
        let chat = Chat::load_from_db(&context.ctx, chat_id).await.unwrap();

        let chat2_id = create_by_contact_id(&context.ctx, contact1).await.unwrap();
        assert_eq!(chat2_id, chat_id);
        let chat2 = Chat::load_from_db(&context.ctx, chat2_id).await.unwrap();

        assert_eq!(chat2.name, chat.name);
    }

    #[async_std::test]
    async fn test_shall_attach_selfavatar() {
        let t = dummy_context().await;
        let chat_id = create_group_chat(&t.ctx, VerifiedStatus::Unverified, "foo")
            .await
            .unwrap();
        assert!(!shall_attach_selfavatar(&t.ctx, chat_id).await.unwrap());

        let (contact_id, _) =
            Contact::add_or_lookup(&t.ctx, "", "foo@bar.org", Origin::IncomingUnknownTo)
                .await
                .unwrap();
        add_contact_to_chat(&t.ctx, chat_id, contact_id).await;
        assert!(!shall_attach_selfavatar(&t.ctx, chat_id).await.unwrap());
        t.ctx.set_config(Config::Selfavatar, None).await.unwrap(); // setting to None also forces re-sending
        assert!(shall_attach_selfavatar(&t.ctx, chat_id).await.unwrap());

        assert!(chat_id
            .set_selfavatar_timestamp(&t.ctx, time())
            .await
            .is_ok());
        assert!(!shall_attach_selfavatar(&t.ctx, chat_id).await.unwrap());
    }

    #[async_std::test]
    async fn test_set_mute_duration() {
        let t = dummy_context().await;
        let chat_id = create_group_chat(&t.ctx, VerifiedStatus::Unverified, "foo")
            .await
            .unwrap();
        // Initial
        assert_eq!(
            Chat::load_from_db(&t.ctx, chat_id)
                .await
                .unwrap()
                .is_muted(),
            false
        );
        // Forever
        set_muted(&t.ctx, chat_id, MuteDuration::Forever)
            .await
            .unwrap();
        assert_eq!(
            Chat::load_from_db(&t.ctx, chat_id)
                .await
                .unwrap()
                .is_muted(),
            true
        );
        // unMute
        set_muted(&t.ctx, chat_id, MuteDuration::NotMuted)
            .await
            .unwrap();
        assert_eq!(
            Chat::load_from_db(&t.ctx, chat_id)
                .await
                .unwrap()
                .is_muted(),
            false
        );
        // Timed in the future
        set_muted(
            &t.ctx,
            chat_id,
            MuteDuration::Until(SystemTime::now() + Duration::from_secs(3600)),
        )
        .await
        .unwrap();
        assert_eq!(
            Chat::load_from_db(&t.ctx, chat_id)
                .await
                .unwrap()
                .is_muted(),
            true
        );
        // Time in the past
        set_muted(
            &t.ctx,
            chat_id,
            MuteDuration::Until(SystemTime::now() - Duration::from_secs(3600)),
        )
        .await
        .unwrap();
        assert_eq!(
            Chat::load_from_db(&t.ctx, chat_id)
                .await
                .unwrap()
                .is_muted(),
            false
        );
    }

    #[async_std::test]
    async fn test_parent_is_encrypted() {
        let t = dummy_context().await;
        let chat_id = create_group_chat(&t.ctx, VerifiedStatus::Unverified, "foo")
            .await
            .unwrap();
        assert!(!chat_id.parent_is_encrypted(&t.ctx).await.unwrap());

        let mut msg = Message::new(Viewtype::Text);
        msg.set_text(Some("hello".to_string()));
        chat_id.set_draft(&t.ctx, Some(&mut msg)).await;
        assert!(!chat_id.parent_is_encrypted(&t.ctx).await.unwrap());
    }
}

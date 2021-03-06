//! Verified contact protocol implementation as [specified by countermitm project](https://countermitm.readthedocs.io/en/stable/new.html#setup-contact-protocol)

use std::time::Duration;

use percent_encoding::{utf8_percent_encode, AsciiSet, NON_ALPHANUMERIC};

use crate::aheader::EncryptPreference;
use crate::chat::{self, Chat, ChatId};
use crate::config::*;
use crate::constants::*;
use crate::contact::*;
use crate::context::Context;
use crate::e2ee::*;
use crate::error::{bail, Error};
use crate::events::Event;
use crate::headerdef::HeaderDef;
use crate::key::{dc_normalize_fingerprint, DcKey, SignedPublicKey};
use crate::lot::LotState;
use crate::message::Message;
use crate::mimeparser::*;
use crate::param::*;
use crate::peerstate::*;
use crate::qr::check_qr;
use crate::stock::StockMessage;
use crate::token;

pub const NON_ALPHANUMERIC_WITHOUT_DOT: &AsciiSet = &NON_ALPHANUMERIC.remove(b'.');

macro_rules! joiner_progress {
    ($context:tt, $contact_id:expr, $progress:expr) => {
        assert!(
            $progress >= 0 && $progress <= 1000,
            "value in range 0..1000 expected with: 0=error, 1..999=progress, 1000=success"
        );
        $context.emit_event($crate::events::Event::SecurejoinJoinerProgress {
            contact_id: $contact_id,
            progress: $progress,
        });
    };
}

macro_rules! inviter_progress {
    ($context:tt, $contact_id:expr, $progress:expr) => {
        assert!(
            $progress >= 0 && $progress <= 1000,
            "value in range 0..1000 expected with: 0=error, 1..999=progress, 1000=success"
        );
        $context.emit_event($crate::events::Event::SecurejoinInviterProgress {
            contact_id: $contact_id,
            progress: $progress,
        });
    };
}

macro_rules! get_qr_attr {
    ($context:tt, $attr:ident) => {
        $context
            .bob
            .read()
            .await
            .qr_scan
            .as_ref()
            .unwrap()
            .$attr
            .as_ref()
            .unwrap()
    };
}

pub async fn dc_get_securejoin_qr(context: &Context, group_chat_id: ChatId) -> Option<String> {
    /*=======================================================
    ====             Alice - the inviter side            ====
    ====   Step 1 in "Setup verified contact" protocol   ====
    =======================================================*/

    let fingerprint: String;

    ensure_secret_key_exists(context).await.ok();

    // invitenumber will be used to allow starting the handshake,
    // auth will be used to verify the fingerprint
    let invitenumber =
        token::lookup_or_new(context, token::Namespace::InviteNumber, group_chat_id).await;
    let auth = token::lookup_or_new(context, token::Namespace::Auth, group_chat_id).await;
    let self_addr = match context.get_config(Config::ConfiguredAddr).await {
        Some(addr) => addr,
        None => {
            error!(context, "Not configured, cannot generate QR code.",);
            return None;
        }
    };

    let self_name = context
        .get_config(Config::Displayname)
        .await
        .unwrap_or_default();

    fingerprint = match get_self_fingerprint(context).await {
        Some(fp) => fp,
        None => {
            return None;
        }
    };

    let self_addr_urlencoded =
        utf8_percent_encode(&self_addr, NON_ALPHANUMERIC_WITHOUT_DOT).to_string();
    let self_name_urlencoded =
        utf8_percent_encode(&self_name, NON_ALPHANUMERIC_WITHOUT_DOT).to_string();

    let qr = if !group_chat_id.is_unset() {
        // parameters used: a=g=x=i=s=
        if let Ok(chat) = Chat::load_from_db(context, group_chat_id).await {
            let group_name = chat.get_name();
            let group_name_urlencoded =
                utf8_percent_encode(&group_name, NON_ALPHANUMERIC).to_string();

            Some(format!(
                "OPENPGP4FPR:{}#a={}&g={}&x={}&i={}&s={}",
                fingerprint,
                self_addr_urlencoded,
                &group_name_urlencoded,
                &chat.grpid,
                &invitenumber,
                &auth,
            ))
        } else {
            error!(context, "Cannot get QR-code for chat-id {}", group_chat_id,);
            return None;
        }
    } else {
        // parameters used: a=n=i=s=
        Some(format!(
            "OPENPGP4FPR:{}#a={}&n={}&i={}&s={}",
            fingerprint, self_addr_urlencoded, self_name_urlencoded, &invitenumber, &auth,
        ))
    };

    info!(context, "Generated QR code: {}", qr.as_ref().unwrap());

    qr
}

async fn get_self_fingerprint(context: &Context) -> Option<String> {
    match SignedPublicKey::load_self(context).await {
        Ok(key) => Some(key.fingerprint().hex()),
        Err(_) => {
            warn!(context, "get_self_fingerprint(): failed to load key");
            None
        }
    }
}

async fn cleanup(
    context: &Context,
    contact_chat_id: ChatId,
    ongoing_allocated: bool,
    join_vg: bool,
) -> ChatId {
    let mut bob = context.bob.write().await;
    bob.expects = 0;
    let ret_chat_id: ChatId = if bob.status == DC_BOB_SUCCESS {
        if join_vg {
            chat::get_chat_id_by_grpid(
                context,
                bob.qr_scan.as_ref().unwrap().text2.as_ref().unwrap(),
            )
            .await
            .unwrap_or((ChatId::new(0), false, Blocked::Not))
            .0
        } else {
            contact_chat_id
        }
    } else {
        ChatId::new(0)
    };
    bob.qr_scan = None;

    if ongoing_allocated {
        context.free_ongoing().await;
    }
    ret_chat_id
}

/// Take a scanned QR-code and do the setup-contact/join-group handshake.
/// See the ffi-documentation for more details.
pub async fn dc_join_securejoin(context: &Context, qr: &str) -> ChatId {
    if context.alloc_ongoing().await.is_err() {
        return cleanup(&context, ChatId::new(0), false, false).await;
    }

    securejoin(context, qr).await
}

async fn securejoin(context: &Context, qr: &str) -> ChatId {
    /*========================================================
    ====             Bob - the joiner's side             =====
    ====   Step 2 in "Setup verified contact" protocol   =====
    ========================================================*/

    let mut contact_chat_id = ChatId::new(0);
    let mut join_vg: bool = false;

    info!(context, "Requesting secure-join ...",);
    ensure_secret_key_exists(context).await.ok();
    let qr_scan = check_qr(context, &qr).await;
    if qr_scan.state != LotState::QrAskVerifyContact && qr_scan.state != LotState::QrAskVerifyGroup
    {
        error!(context, "Unknown QR code.",);
        return cleanup(&context, contact_chat_id, true, join_vg).await;
    }
    contact_chat_id = match chat::create_by_contact_id(context, qr_scan.id).await {
        Ok(chat_id) => chat_id,
        Err(_) => {
            error!(context, "Unknown contact.");
            return cleanup(&context, contact_chat_id, true, join_vg).await;
        }
    };
    if context.shall_stop_ongoing().await {
        return cleanup(&context, contact_chat_id, true, join_vg).await;
    }
    join_vg = qr_scan.get_state() == LotState::QrAskVerifyGroup;
    {
        let mut bob = context.bob.write().await;
        bob.status = 0;
        bob.qr_scan = Some(qr_scan);
    }
    if fingerprint_equals_sender(
        context,
        context
            .bob
            .read()
            .await
            .qr_scan
            .as_ref()
            .unwrap()
            .fingerprint
            .as_ref()
            .unwrap(),
        contact_chat_id,
    )
    .await
    {
        // the scanned fingerprint matches Alice's key,
        // we can proceed to step 4b) directly and save two mails
        info!(context, "Taking protocol shortcut.");
        context.bob.write().await.expects = DC_VC_CONTACT_CONFIRM;
        joiner_progress!(
            context,
            chat_id_2_contact_id(context, contact_chat_id).await,
            400
        );
        let own_fingerprint = get_self_fingerprint(context).await.unwrap_or_default();

        // Bob -> Alice
        if let Err(err) = send_handshake_msg(
            context,
            contact_chat_id,
            if join_vg {
                "vg-request-with-auth"
            } else {
                "vc-request-with-auth"
            },
            get_qr_attr!(context, auth).to_string(),
            Some(own_fingerprint),
            if join_vg {
                get_qr_attr!(context, text2).to_string()
            } else {
                "".to_string()
            },
        )
        .await
        {
            error!(context, "failed to send handshake message: {}", err);
            return cleanup(&context, contact_chat_id, true, join_vg).await;
        }
    } else {
        context.bob.write().await.expects = DC_VC_AUTH_REQUIRED;

        // Bob -> Alice
        if let Err(err) = send_handshake_msg(
            context,
            contact_chat_id,
            if join_vg { "vg-request" } else { "vc-request" },
            get_qr_attr!(context, invitenumber),
            None,
            "",
        )
        .await
        {
            error!(context, "failed to send handshake message: {}", err);
            return cleanup(&context, contact_chat_id, true, join_vg).await;
        }
    }

    if join_vg {
        // for a group-join, wait until the secure-join is done and the group is created
        while !context.shall_stop_ongoing().await {
            async_std::task::sleep(Duration::from_millis(50)).await;
        }
        cleanup(&context, contact_chat_id, true, join_vg).await
    } else {
        // for a one-to-one-chat, the chat is already known, return the chat-id,
        // the verification runs in background
        context.free_ongoing().await;
        contact_chat_id
    }
}

async fn send_handshake_msg(
    context: &Context,
    contact_chat_id: ChatId,
    step: &str,
    param2: impl AsRef<str>,
    fingerprint: Option<String>,
    grpid: impl AsRef<str>,
) -> Result<(), HandshakeError> {
    let mut msg = Message::default();
    msg.viewtype = Viewtype::Text;
    msg.text = Some(format!("Secure-Join: {}", step));
    msg.hidden = true;
    msg.param.set_cmd(SystemMessage::SecurejoinMessage);
    if step.is_empty() {
        msg.param.remove(Param::Arg);
    } else {
        msg.param.set(Param::Arg, step);
    }
    if !param2.as_ref().is_empty() {
        msg.param.set(Param::Arg2, param2);
    }
    if let Some(fp) = fingerprint {
        msg.param.set(Param::Arg3, fp);
    }
    if !grpid.as_ref().is_empty() {
        msg.param.set(Param::Arg4, grpid.as_ref());
    }
    if step == "vg-request" || step == "vc-request" {
        msg.param.set_int(
            Param::ForcePlaintext,
            ForcePlaintext::AddAutocryptHeader as i32,
        );
    } else {
        msg.param.set_int(Param::GuaranteeE2ee, 1);
    }

    chat::send_msg(context, contact_chat_id, &mut msg)
        .await
        .map_err(HandshakeError::MsgSendFailed)?;

    Ok(())
}

async fn chat_id_2_contact_id(context: &Context, contact_chat_id: ChatId) -> u32 {
    let contacts = chat::get_chat_contacts(context, contact_chat_id).await;
    if contacts.len() == 1 {
        contacts[0]
    } else {
        0
    }
}

async fn fingerprint_equals_sender(
    context: &Context,
    fingerprint: impl AsRef<str>,
    contact_chat_id: ChatId,
) -> bool {
    let contacts = chat::get_chat_contacts(context, contact_chat_id).await;

    if contacts.len() == 1 {
        if let Ok(contact) = Contact::load_from_db(context, contacts[0]).await {
            if let Some(peerstate) = Peerstate::from_addr(context, contact.get_addr()).await {
                let fingerprint_normalized = dc_normalize_fingerprint(fingerprint.as_ref());
                if peerstate.public_key_fingerprint.is_some()
                    && &fingerprint_normalized == peerstate.public_key_fingerprint.as_ref().unwrap()
                {
                    return true;
                }
            }
        }
    }
    false
}
#[derive(Debug, thiserror::Error)]
pub(crate) enum HandshakeError {
    #[error("Can not be called with special contact ID")]
    SpecialContactId,
    #[error("Not a Secure-Join message")]
    NotSecureJoinMsg,
    #[error("Failed to look up or create chat for contact #{contact_id}")]
    NoChat {
        contact_id: u32,
        #[source]
        cause: Error,
    },
    #[error("Chat for group {group} not found")]
    ChatNotFound { group: String },
    #[error("No configured self address found")]
    NoSelfAddr,
    #[error("Failed to send message")]
    MsgSendFailed(#[source] Error),
}

/// What to do with a Secure-Join handshake message after it was handled.
pub(crate) enum HandshakeMessage {
    /// The message has been fully handled and should be removed/delete.
    Done,
    /// The message should be ignored/hidden, but not removed/deleted.
    Ignore,
    /// The message should be further processed by incoming message handling.
    Propagate,
}

/// Handle incoming secure-join handshake.
///
/// This function will update the securejoin state in [Context::bob]
/// and also terminate the ongoing process using
/// [Context::stop_ongoing] as required by the protocol.
///
/// A message which results in [Err] will be hidden from the user but
/// not deleted, it may be a valid message for something else we are
/// not aware off.  E.g. it could be part of a handshake performed by
/// another DC app on the same account.
///
/// When handle_securejoin_handshake() is called,
/// the message is not yet filed in the database;
/// this is done by receive_imf() later on as needed.
pub(crate) async fn handle_securejoin_handshake(
    context: &Context,
    mime_message: &MimeMessage,
    contact_id: u32,
) -> Result<HandshakeMessage, HandshakeError> {
    if contact_id <= DC_CONTACT_ID_LAST_SPECIAL {
        return Err(HandshakeError::SpecialContactId);
    }
    let step = mime_message
        .get(HeaderDef::SecureJoin)
        .ok_or(HandshakeError::NotSecureJoinMsg)?;

    info!(
        context,
        ">>>>>>>>>>>>>>>>>>>>>>>>> secure-join message \'{}\' received", step,
    );

    let contact_chat_id =
        match chat::create_or_lookup_by_contact_id(context, contact_id, Blocked::Not).await {
            Ok((chat_id, blocked)) => {
                if blocked != Blocked::Not {
                    chat_id.unblock(context).await;
                }
                chat_id
            }
            Err(err) => {
                return Err(HandshakeError::NoChat {
                    contact_id,
                    cause: err,
                });
            }
        };

    let join_vg = step.starts_with("vg-");

    match step.as_str() {
        "vg-request" | "vc-request" => {
            /*=======================================================
            ====             Alice - the inviter side            ====
            ====   Step 3 in "Setup verified contact" protocol   ====
            =======================================================*/

            // this message may be unencrypted (Bob, the joiner and the sender, might not have Alice's key yet)
            // it just ensures, we have Bobs key now. If we do _not_ have the key because eg. MitM has removed it,
            // send_message() will fail with the error "End-to-end-encryption unavailable unexpectedly.", so, there is no additional check needed here.
            // verify that the `Secure-Join-Invitenumber:`-header matches invitenumber written to the QR code
            let invitenumber = match mime_message.get(HeaderDef::SecureJoinInvitenumber) {
                Some(n) => n,
                None => {
                    warn!(context, "Secure-join denied (invitenumber missing)");
                    return Ok(HandshakeMessage::Ignore);
                }
            };
            if !token::exists(context, token::Namespace::InviteNumber, &invitenumber).await {
                warn!(context, "Secure-join denied (bad invitenumber).");
                return Ok(HandshakeMessage::Ignore);
            }
            info!(context, "Secure-join requested.",);

            inviter_progress!(context, contact_id, 300);

            // Alice -> Bob
            send_handshake_msg(
                context,
                contact_chat_id,
                &format!("{}-auth-required", &step[..2]),
                "",
                None,
                "",
            )
            .await?;
            Ok(HandshakeMessage::Done)
        }
        "vg-auth-required" | "vc-auth-required" => {
            /*========================================================
            ====             Bob - the joiner's side             =====
            ====   Step 4 in "Setup verified contact" protocol   =====
            ========================================================*/

            // verify that Alice's Autocrypt key and fingerprint matches the QR-code
            let cond = {
                let bob = context.bob.read().await;
                let scan = bob.qr_scan.as_ref();
                scan.is_none()
                    || bob.expects != DC_VC_AUTH_REQUIRED
                    || join_vg && scan.unwrap().state != LotState::QrAskVerifyGroup
            };

            if cond {
                warn!(context, "auth-required message out of sync.");
                // no error, just aborted somehow or a mail from another handshake
                return Ok(HandshakeMessage::Ignore);
            }
            let scanned_fingerprint_of_alice = get_qr_attr!(context, fingerprint).to_string();
            let auth = get_qr_attr!(context, auth).to_string();

            if !encrypted_and_signed(context, mime_message, &scanned_fingerprint_of_alice) {
                could_not_establish_secure_connection(
                    context,
                    contact_chat_id,
                    if mime_message.was_encrypted() {
                        "No valid signature."
                    } else {
                        "Not encrypted."
                    },
                )
                .await;
                context.bob.write().await.status = 0; // secure-join failed
                context.stop_ongoing().await;
                return Ok(HandshakeMessage::Ignore);
            }
            if !fingerprint_equals_sender(context, &scanned_fingerprint_of_alice, contact_chat_id)
                .await
            {
                could_not_establish_secure_connection(
                    context,
                    contact_chat_id,
                    "Fingerprint mismatch on joiner-side.",
                )
                .await;
                context.bob.write().await.status = 0; // secure-join failed
                context.stop_ongoing().await;
                return Ok(HandshakeMessage::Ignore);
            }
            info!(context, "Fingerprint verified.",);
            let own_fingerprint = get_self_fingerprint(context).await.unwrap();
            joiner_progress!(context, contact_id, 400);
            context.bob.write().await.expects = DC_VC_CONTACT_CONFIRM;

            // Bob -> Alice
            send_handshake_msg(
                context,
                contact_chat_id,
                &format!("{}-request-with-auth", &step[..2]),
                auth,
                Some(own_fingerprint),
                if join_vg {
                    get_qr_attr!(context, text2).to_string()
                } else {
                    "".to_string()
                },
            )
            .await?;
            Ok(HandshakeMessage::Done)
        }
        "vg-request-with-auth" | "vc-request-with-auth" => {
            /*==========================================================
            ====              Alice - the inviter side              ====
            ====   Steps 5+6 in "Setup verified contact" protocol   ====
            ====  Step 6 in "Out-of-band verified groups" protocol  ====
            ==========================================================*/

            // verify that Secure-Join-Fingerprint:-header matches the fingerprint of Bob
            let fingerprint = match mime_message.get(HeaderDef::SecureJoinFingerprint) {
                Some(fp) => fp,
                None => {
                    could_not_establish_secure_connection(
                        context,
                        contact_chat_id,
                        "Fingerprint not provided.",
                    )
                    .await;
                    return Ok(HandshakeMessage::Ignore);
                }
            };
            if !encrypted_and_signed(context, mime_message, &fingerprint) {
                could_not_establish_secure_connection(
                    context,
                    contact_chat_id,
                    "Auth not encrypted.",
                )
                .await;
                return Ok(HandshakeMessage::Ignore);
            }
            if !fingerprint_equals_sender(context, &fingerprint, contact_chat_id).await {
                could_not_establish_secure_connection(
                    context,
                    contact_chat_id,
                    "Fingerprint mismatch on inviter-side.",
                )
                .await;
                return Ok(HandshakeMessage::Ignore);
            }
            info!(context, "Fingerprint verified.",);
            // verify that the `Secure-Join-Auth:`-header matches the secret written to the QR code
            let auth_0 = match mime_message.get(HeaderDef::SecureJoinAuth) {
                Some(auth) => auth,
                None => {
                    could_not_establish_secure_connection(
                        context,
                        contact_chat_id,
                        "Auth not provided.",
                    )
                    .await;
                    return Ok(HandshakeMessage::Ignore);
                }
            };
            if !token::exists(context, token::Namespace::Auth, &auth_0).await {
                could_not_establish_secure_connection(context, contact_chat_id, "Auth invalid.")
                    .await;
                return Ok(HandshakeMessage::Ignore);
            }
            if mark_peer_as_verified(context, fingerprint).await.is_err() {
                could_not_establish_secure_connection(
                    context,
                    contact_chat_id,
                    "Fingerprint mismatch on inviter-side.",
                )
                .await;
                return Ok(HandshakeMessage::Ignore);
            }
            Contact::scaleup_origin_by_id(context, contact_id, Origin::SecurejoinInvited).await;
            info!(context, "Auth verified.",);
            secure_connection_established(context, contact_chat_id).await;
            emit_event!(context, Event::ContactsChanged(Some(contact_id)));
            inviter_progress!(context, contact_id, 600);
            if join_vg {
                // the vg-member-added message is special:
                // this is a normal Chat-Group-Member-Added message
                // with an additional Secure-Join header
                let field_grpid = match mime_message.get(HeaderDef::SecureJoinGroup) {
                    Some(s) => s.as_str(),
                    None => {
                        warn!(context, "Missing Secure-Join-Group header");
                        return Ok(HandshakeMessage::Ignore);
                    }
                };
                match chat::get_chat_id_by_grpid(context, field_grpid).await {
                    Ok((group_chat_id, _, _)) => {
                        if let Err(err) =
                            chat::add_contact_to_chat_ex(context, group_chat_id, contact_id, true)
                                .await
                        {
                            error!(context, "failed to add contact: {}", err);
                        }
                    }
                    Err(err) => {
                        error!(context, "Chat {} not found: {}", &field_grpid, err);
                        return Err(HandshakeError::ChatNotFound {
                            group: field_grpid.to_string(),
                        });
                    }
                }
            } else {
                // Alice -> Bob
                send_handshake_msg(
                    context,
                    contact_chat_id,
                    "vc-contact-confirm",
                    "",
                    Some(fingerprint.clone()),
                    "",
                )
                .await?;

                inviter_progress!(context, contact_id, 1000);
            }
            Ok(HandshakeMessage::Ignore) // "Done" would delete the message and break multi-device (the key from Autocrypt-header is needed)
        }
        "vg-member-added" | "vc-contact-confirm" => {
            /*=======================================================
            ====             Bob - the joiner's side             ====
            ====   Step 7 in "Setup verified contact" protocol   ====
            =======================================================*/
            let abort_retval = if join_vg {
                HandshakeMessage::Propagate
            } else {
                HandshakeMessage::Ignore
            };

            if context.bob.read().await.expects != DC_VC_CONTACT_CONFIRM {
                info!(context, "Message belongs to a different handshake.",);
                return Ok(abort_retval);
            }
            let cond = {
                let bob = context.bob.read().await;
                let scan = bob.qr_scan.as_ref();
                scan.is_none() || (join_vg && scan.unwrap().state != LotState::QrAskVerifyGroup)
            };
            if cond {
                warn!(
                    context,
                    "Message out of sync or belongs to a different handshake.",
                );
                return Ok(abort_retval);
            }
            let scanned_fingerprint_of_alice = get_qr_attr!(context, fingerprint).to_string();

            let vg_expect_encrypted = if join_vg {
                let group_id = get_qr_attr!(context, text2).to_string();
                // This is buggy, is_verified_group will always be
                // false since the group is created by receive_imf by
                // the very handshake message we're handling now.  But
                // only after we have returned.  It does not impact
                // the security invariants of secure-join however.
                let (_, is_verified_group, _) = chat::get_chat_id_by_grpid(context, &group_id)
                    .await
                    .unwrap_or((ChatId::new(0), false, Blocked::Not));
                // when joining a non-verified group
                // the vg-member-added message may be unencrypted
                // when not all group members have keys or prefer encryption.
                // So only expect encryption if this is a verified group
                is_verified_group
            } else {
                // setup contact is always encrypted
                true
            };
            if vg_expect_encrypted
                && !encrypted_and_signed(context, mime_message, &scanned_fingerprint_of_alice)
            {
                could_not_establish_secure_connection(
                    context,
                    contact_chat_id,
                    "Contact confirm message not encrypted.",
                )
                .await;
                context.bob.write().await.status = 0;
                return Ok(abort_retval);
            }

            if mark_peer_as_verified(context, &scanned_fingerprint_of_alice)
                .await
                .is_err()
            {
                could_not_establish_secure_connection(
                    context,
                    contact_chat_id,
                    "Fingerprint mismatch on joiner-side.",
                )
                .await;
                return Ok(abort_retval);
            }
            Contact::scaleup_origin_by_id(context, contact_id, Origin::SecurejoinJoined).await;
            emit_event!(context, Event::ContactsChanged(None));
            let cg_member_added = mime_message
                .get(HeaderDef::ChatGroupMemberAdded)
                .map(|s| s.as_str())
                .unwrap_or_else(|| "");
            if join_vg
                && !context
                    .is_self_addr(cg_member_added)
                    .await
                    .map_err(|_| HandshakeError::NoSelfAddr)?
            {
                info!(context, "Message belongs to a different handshake (scaled up contact anyway to allow creation of group).");
                return Ok(abort_retval);
            }
            secure_connection_established(context, contact_chat_id).await;
            context.bob.write().await.expects = 0;

            // Bob -> Alice
            send_handshake_msg(
                context,
                contact_chat_id,
                if join_vg {
                    "vg-member-added-received"
                } else {
                    "vc-contact-confirm-received" // only for observe_securejoin_on_other_device()
                },
                "",
                Some(scanned_fingerprint_of_alice),
                "",
            )
            .await?;

            context.bob.write().await.status = 1;
            context.stop_ongoing().await;
            Ok(if join_vg {
                HandshakeMessage::Propagate
            } else {
                HandshakeMessage::Ignore // "Done" deletes the message and breaks multi-device
            })
        }
        "vg-member-added-received" | "vc-contact-confirm-received" => {
            /*==========================================================
            ====              Alice - the inviter side              ====
            ====  Step 8 in "Out-of-band verified groups" protocol  ====
            ==========================================================*/

            if let Ok(contact) = Contact::get_by_id(context, contact_id).await {
                if contact.is_verified(context).await == VerifiedStatus::Unverified {
                    warn!(context, "{} invalid.", step);
                    return Ok(HandshakeMessage::Ignore);
                }
                if join_vg {
                    inviter_progress!(context, contact_id, 800);
                    inviter_progress!(context, contact_id, 1000);
                    let field_grpid = mime_message
                        .get(HeaderDef::SecureJoinGroup)
                        .map(|s| s.as_str())
                        .unwrap_or_else(|| "");
                    if let Err(err) = chat::get_chat_id_by_grpid(context, &field_grpid).await {
                        warn!(context, "Failed to lookup chat_id from grpid: {}", err);
                        return Err(HandshakeError::ChatNotFound {
                            group: field_grpid.to_string(),
                        });
                    }
                }
                Ok(HandshakeMessage::Ignore) // "Done" deletes the message and breaks multi-device
            } else {
                warn!(context, "{} invalid.", step);
                Ok(HandshakeMessage::Ignore)
            }
        }
        _ => {
            warn!(context, "invalid step: {}", step);
            Ok(HandshakeMessage::Ignore)
        }
    }
}

/// observe_securejoin_on_other_device() must be called when a self-sent securejoin message is seen.
///
/// in a multi-device-setup, there may be other devices that "see" the handshake messages.
/// if the seen messages seen are self-sent messages encrypted+signed correctly with our key,
/// we can make some conclusions of it:
///
/// - if we see the self-sent-message vg-member-added/vc-contact-confirm,
///   we know that we're an inviter-observer.
///   the inviting device has marked a peer as verified on vg-request-with-auth/vc-request-with-auth
///   before sending vg-member-added/vc-contact-confirm - so, if we observe vg-member-added/vc-contact-confirm,
///   we can mark the peer as verified as well.
///
/// - if we see the self-sent-message vg-member-added-received
///   we know that we're an joiner-observer.
///   the joining device has marked the peer as verified on vg-member-added/vc-contact-confirm
///   before sending vg-member-added-received - so, if we observe vg-member-added-received,
///   we can mark the peer as verified as well.
pub(crate) async fn observe_securejoin_on_other_device(
    context: &Context,
    mime_message: &MimeMessage,
    contact_id: u32,
) -> Result<HandshakeMessage, HandshakeError> {
    if contact_id <= DC_CONTACT_ID_LAST_SPECIAL {
        return Err(HandshakeError::SpecialContactId);
    }
    let step = mime_message
        .get(HeaderDef::SecureJoin)
        .ok_or(HandshakeError::NotSecureJoinMsg)?;
    info!(context, "observing secure-join message \'{}\'", step);

    let contact_chat_id =
        match chat::create_or_lookup_by_contact_id(context, contact_id, Blocked::Not).await {
            Ok((chat_id, blocked)) => {
                if blocked != Blocked::Not {
                    chat_id.unblock(context).await;
                }
                chat_id
            }
            Err(err) => {
                return Err(HandshakeError::NoChat {
                    contact_id,
                    cause: err,
                });
            }
        };

    match step.as_str() {
        "vg-member-added"
        | "vc-contact-confirm"
        | "vg-member-added-received"
        | "vc-contact-confirm-received" => {
            if !encrypted_and_signed(
                context,
                mime_message,
                get_self_fingerprint(context).await.unwrap_or_default(),
            ) {
                could_not_establish_secure_connection(
                    context,
                    contact_chat_id,
                    "Message not encrypted correctly.",
                )
                .await;
                return Ok(HandshakeMessage::Ignore);
            }
            let fingerprint = match mime_message.get(HeaderDef::SecureJoinFingerprint) {
                Some(fp) => fp,
                None => {
                    could_not_establish_secure_connection(
                        context,
                        contact_chat_id,
                        "Fingerprint not provided, please update Delta Chat on all your devices.",
                    )
                    .await;
                    return Ok(HandshakeMessage::Ignore);
                }
            };
            if mark_peer_as_verified(context, fingerprint).await.is_err() {
                could_not_establish_secure_connection(
                    context,
                    contact_chat_id,
                    format!("Fingerprint mismatch on observing {}.", step).as_ref(),
                )
                .await;
                return Ok(HandshakeMessage::Ignore);
            }
            Ok(if step.as_str() == "vg-member-added" {
                HandshakeMessage::Propagate
            } else {
                HandshakeMessage::Ignore
            })
        }
        _ => Ok(HandshakeMessage::Ignore),
    }
}

async fn secure_connection_established(context: &Context, contact_chat_id: ChatId) {
    let contact_id: u32 = chat_id_2_contact_id(context, contact_chat_id).await;
    let contact = Contact::get_by_id(context, contact_id).await;

    let addr = if let Ok(ref contact) = contact {
        contact.get_addr()
    } else {
        "?"
    };
    let msg = context
        .stock_string_repl_str(StockMessage::ContactVerified, addr)
        .await;
    chat::add_info_msg(context, contact_chat_id, msg).await;
    emit_event!(context, Event::ChatModified(contact_chat_id));
}

async fn could_not_establish_secure_connection(
    context: &Context,
    contact_chat_id: ChatId,
    details: &str,
) {
    let contact_id = chat_id_2_contact_id(context, contact_chat_id).await;
    let contact = Contact::get_by_id(context, contact_id).await;
    let msg = context
        .stock_string_repl_str(
            StockMessage::ContactNotVerified,
            if let Ok(ref contact) = contact {
                contact.get_addr()
            } else {
                "?"
            },
        )
        .await;

    chat::add_info_msg(context, contact_chat_id, &msg).await;
    error!(context, "{} ({})", &msg, details);
}

async fn mark_peer_as_verified(
    context: &Context,
    fingerprint: impl AsRef<str>,
) -> Result<(), Error> {
    if let Some(ref mut peerstate) =
        Peerstate::from_fingerprint(context, &context.sql, fingerprint.as_ref()).await
    {
        if peerstate.set_verified(
            PeerstateKeyType::PublicKey,
            fingerprint.as_ref(),
            PeerstateVerifiedStatus::BidirectVerified,
        ) {
            peerstate.prefer_encrypt = EncryptPreference::Mutual;
            peerstate.to_save = Some(ToSave::All);
            peerstate
                .save_to_db(&context.sql, false)
                .await
                .unwrap_or_default();
            return Ok(());
        }
    }
    bail!(
        "could not mark peer as verified for fingerprint {}",
        fingerprint.as_ref()
    );
}

/* ******************************************************************************
 * Tools: Misc.
 ******************************************************************************/

fn encrypted_and_signed(
    context: &Context,
    mimeparser: &MimeMessage,
    expected_fingerprint: impl AsRef<str>,
) -> bool {
    if !mimeparser.was_encrypted() {
        warn!(context, "Message not encrypted.",);
        false
    } else if mimeparser.signatures.is_empty() {
        warn!(context, "Message not signed.",);
        false
    } else if expected_fingerprint.as_ref().is_empty() {
        warn!(context, "Fingerprint for comparison missing.",);
        false
    } else if !mimeparser
        .signatures
        .contains(expected_fingerprint.as_ref())
    {
        warn!(
            context,
            "Message does not match expected fingerprint {}.",
            expected_fingerprint.as_ref(),
        );
        false
    } else {
        true
    }
}

pub async fn handle_degrade_event(
    context: &Context,
    peerstate: &Peerstate<'_>,
) -> Result<(), Error> {
    // - we do not issue an warning for DC_DE_ENCRYPTION_PAUSED as this is quite normal
    // - currently, we do not issue an extra warning for DC_DE_VERIFICATION_LOST - this always comes
    //   together with DC_DE_FINGERPRINT_CHANGED which is logged, the idea is not to bother
    //   with things they cannot fix, so the user is just kicked from the verified group
    //   (and he will know this and can fix this)
    if Some(DegradeEvent::FingerprintChanged) == peerstate.degrade_event {
        let contact_id: i32 = match context
            .sql
            .query_get_value(
                context,
                "SELECT id FROM contacts WHERE addr=?;",
                paramsv![peerstate.addr],
            )
            .await
        {
            None => bail!(
                "contact with peerstate.addr {:?} not found",
                &peerstate.addr
            ),
            Some(contact_id) => contact_id,
        };
        if contact_id > 0 {
            let (contact_chat_id, _) =
                chat::create_or_lookup_by_contact_id(context, contact_id as u32, Blocked::Deaddrop)
                    .await
                    .unwrap_or_default();

            let msg = context
                .stock_string_repl_str(StockMessage::ContactSetupChanged, peerstate.addr.clone())
                .await;

            chat::add_info_msg(context, contact_chat_id, msg).await;
            emit_event!(context, Event::ChatModified(contact_chat_id));
        }
    }
    Ok(())
}

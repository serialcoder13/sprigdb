//! Feedback: a thumbs up or down (and a note) on an answer, and what
//! became of each proposal, in `ai_feedback` (module `ai`).
//!
//! - **Whose.** A user rates only answers and proposals of their own
//!   conversations; anything else is "not found".
//! - **What it teaches.** [`prompt`] turns the tenant's latest outcomes
//!   and notes into a short block for every tenant assistant's prompt:
//!   which proposals people accepted or dismissed, and what they said.
//!   Only the proposal's kind and a short label (its `why`, title, term or
//!   subject) are used, never an answer's text, with secrets masked.
//!   Feedback improves prompts and evaluation sets, never model weights.

use std::fmt::Write as _;

use async_trait::async_trait;
use mysql_async::prelude::Queryable as _;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sprig_common::Result;
use sprig_common::ids::UserId;

use crate::audit::{CatalogAudit, MemoryAudit, MsgKind, unavailable};
use crate::role::Role;

/// Notes the prompt carries at most.
pub const PROMPT_NOTES: u32 = 20;
/// The longest label or note one line of the prompt shows, in characters.
const LINE_CHARS: usize = 160;
/// The longest note stored, in characters.
pub const MAX_NOTE: usize = 500;

/// What became of a proposal.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Outcome {
    /// The user accepted it (ran the statement, added the tile, ...).
    Accepted,
    /// The user dismissed it.
    Dismissed,
    /// It was accepted and failed.
    Failed,
}

impl Outcome {
    /// The catalog's and the metrics' name.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Outcome::Accepted => "accepted",
            Outcome::Dismissed => "dismissed",
            Outcome::Failed => "failed",
        }
    }

    /// The outcome named `s`.
    #[must_use]
    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "accepted" => Some(Outcome::Accepted),
            "dismissed" => Some(Outcome::Dismissed),
            "failed" => Some(Outcome::Failed),
            _ => None,
        }
    }
}

/// One piece of feedback, as the prompt uses it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Note {
    /// The vote on an answer: 1 or -1.
    pub vote: Option<i8>,
    /// What became of a proposal.
    pub outcome: Option<Outcome>,
    /// The proposal's kind (`sql`, `tile`, ...).
    pub proposal_kind: Option<String>,
    /// A short label of the proposal: its `why`, title, term or subject.
    pub label: Option<String>,
    /// The user's note.
    pub note: Option<String>,
}

/// A feedback row of [`MemoryAudit`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct MemFeedback {
    proposal: Option<u64>,
    vote: Option<i8>,
    outcome: Option<Outcome>,
    note: Option<String>,
}

/// Where feedback is kept.
#[async_trait]
pub trait FeedbackStore: Send + Sync {
    /// Records `user`'s `vote` (1 or -1) and `note` on the answer `msg_id`.
    /// Returns the conversation's role, or `None` when `msg_id` isn't an
    /// answer in one of `user`'s conversations.
    async fn vote(
        &self,
        user: UserId,
        msg_id: u64,
        vote: i8,
        note: Option<&str>,
    ) -> Result<Option<Role>>;
    /// Records what became of the proposal `proposal_id` (its id as the
    /// stream showed it). Returns the conversation's role and the
    /// proposal's kind, or `None` when it isn't a proposal in one of
    /// `user`'s conversations.
    async fn outcome(
        &self,
        user: UserId,
        proposal_id: &str,
        outcome: Outcome,
        note: Option<&str>,
    ) -> Result<Option<(Role, String)>>;
    /// The tenant's latest outcomes and notes, newest first, at most
    /// `limit`.
    async fn recent(&self, limit: u32) -> Result<Vec<Note>>;
}

fn clip(s: &str, n: usize) -> String {
    let t = s.trim().replace('\n', " ");
    if t.chars().count() <= n {
        t
    } else {
        let mut c: String = t.chars().take(n).collect();
        c.push('…');
        c
    }
}

/// A note as stored: trimmed, at most [`MAX_NOTE`] characters, `None`
/// when empty.
#[must_use]
pub fn clean_note(note: Option<&str>) -> Option<String> {
    note.map(str::trim)
        .filter(|n| !n.is_empty())
        .map(|n| n.chars().take(MAX_NOTE).collect())
}

/// The short label of a stored proposal body (`{id, kind, body}`).
fn label_of(p: &Value) -> Option<String> {
    let b = &p["body"];
    ["why", "title", "term", "subject"]
        .iter()
        .find_map(|k| b.get(*k).and_then(Value::as_str))
        .map(|s| clip(s, LINE_CHARS))
}

/// The feedback block of a prompt, secrets masked; `None` when there is
/// nothing to learn from.
#[must_use]
pub fn prompt(notes: &[Note]) -> Option<String> {
    let mut lines = Vec::new();
    for n in notes {
        let note = n.note.as_deref().map(|t| clip(t, LINE_CHARS));
        let line = match (n.outcome, n.vote) {
            (Some(o), _) => {
                let mut l = format!(
                    "- {} {} proposal",
                    o.as_str(),
                    n.proposal_kind.as_deref().unwrap_or("a")
                );
                if let Some(lb) = &n.label {
                    let _ = write!(l, ": {lb}");
                }
                if let Some(t) = &note {
                    let _ = write!(l, " (their note: {t})");
                }
                l
            }
            (None, Some(v)) => match &note {
                Some(t) => format!(
                    "- an answer got a thumbs {}: {t}",
                    if v > 0 { "up" } else { "down" }
                ),
                None => continue,
            },
            (None, None) => continue,
        };
        lines.push(crate::egress::redact_secrets(&line));
    }
    if lines.is_empty() {
        return None;
    }
    Some(format!(
        "Recent feedback from this tenant's users, newest first. Prefer what they accepted, avoid what they dismissed, and heed their notes:\n{}\n",
        lines.join("\n")
    ))
}

#[async_trait]
impl FeedbackStore for MemoryAudit {
    async fn vote(
        &self,
        user: UserId,
        msg_id: u64,
        vote: i8,
        note: Option<&str>,
    ) -> Result<Option<Role>> {
        let mut m = self.lock();
        let Some(conv) = m
            .msgs
            .iter()
            .find(|(_, s)| s.id == msg_id && s.kind == MsgKind::Assistant)
            .map(|(c, _)| *c)
        else {
            return Ok(None);
        };
        let Some(role) = m
            .convs
            .get(&conv)
            .filter(|c| c.user == user.0)
            .map(|c| c.role)
        else {
            return Ok(None);
        };
        m.feedback.push(MemFeedback {
            proposal: None,
            vote: Some(vote),
            outcome: None,
            note: clean_note(note),
        });
        Ok(Some(role))
    }

    async fn outcome(
        &self,
        user: UserId,
        proposal_id: &str,
        outcome: Outcome,
        note: Option<&str>,
    ) -> Result<Option<(Role, String)>> {
        let mut m = self.lock();
        let Some((conv, id, kind)) = m
            .msgs
            .iter()
            .find(|(_, s)| s.kind == MsgKind::Proposal && s.body["id"] == proposal_id)
            .map(|(c, s)| {
                (
                    *c,
                    s.id,
                    s.body["kind"].as_str().unwrap_or_default().to_owned(),
                )
            })
        else {
            return Ok(None);
        };
        let Some(role) = m
            .convs
            .get(&conv)
            .filter(|c| c.user == user.0)
            .map(|c| c.role)
        else {
            return Ok(None);
        };
        m.feedback.push(MemFeedback {
            proposal: Some(id),
            vote: None,
            outcome: Some(outcome),
            note: clean_note(note),
        });
        Ok(Some((role, kind)))
    }

    async fn recent(&self, limit: u32) -> Result<Vec<Note>> {
        let m = self.lock();
        Ok(m.feedback
            .iter()
            .rev()
            .filter(|f| f.outcome.is_some() || f.note.is_some())
            .take(limit as usize)
            .map(|f| {
                let p = f
                    .proposal
                    .and_then(|id| m.msgs.iter().find(|(_, s)| s.id == id))
                    .map(|(_, s)| s.body.clone());
                Note {
                    vote: f.vote,
                    outcome: f.outcome,
                    proposal_kind: p
                        .as_ref()
                        .and_then(|b| b["kind"].as_str())
                        .map(str::to_owned),
                    label: p.as_ref().and_then(label_of),
                    note: f.note.clone(),
                }
            })
            .collect())
    }
}

#[async_trait]
impl FeedbackStore for CatalogAudit {
    async fn vote(
        &self,
        user: UserId,
        msg_id: u64,
        vote: i8,
        note: Option<&str>,
    ) -> Result<Option<Role>> {
        let mut c = self.conn().await?;
        let role: Option<String> = c
            .exec_first(
                format!(
                    "SELECT c.role FROM {} m JOIN {} c ON c.conv_id = m.conv_id \
                     WHERE m.msg_id = ? AND m.kind = 'assistant' AND c.user_id = ?",
                    self.t("ai_messages"),
                    self.t("ai_conversations")
                ),
                (msg_id, user.0),
            )
            .await
            .map_err(unavailable)?;
        let Some(role) = role.as_deref().and_then(Role::parse) else {
            return Ok(None);
        };
        c.exec_drop(
            format!(
                "INSERT INTO {} (user_id, msg_id, vote, note, created_at) \
                 VALUES (?, ?, ?, ?, UTC_TIMESTAMP(3))",
                self.t("ai_feedback")
            ),
            (user.0, msg_id, vote, clean_note(note)),
        )
        .await
        .map_err(unavailable)?;
        Ok(Some(role))
    }

    async fn outcome(
        &self,
        user: UserId,
        proposal_id: &str,
        outcome: Outcome,
        note: Option<&str>,
    ) -> Result<Option<(Role, String)>> {
        let mut c = self.conn().await?;
        let row: Option<(u64, String, Option<String>)> = c
            .exec_first(
                format!(
                    "SELECT m.msg_id, c.role, JSON_UNQUOTE(JSON_EXTRACT(m.body, '$.kind')) \
                     FROM {} c JOIN {} m ON m.conv_id = c.conv_id \
                     WHERE c.user_id = ? AND m.kind = 'proposal' \
                     AND JSON_UNQUOTE(JSON_EXTRACT(m.body, '$.id')) = ?",
                    self.t("ai_conversations"),
                    self.t("ai_messages")
                ),
                (user.0, proposal_id),
            )
            .await
            .map_err(unavailable)?;
        let Some((msg_id, role, kind)) = row else {
            return Ok(None);
        };
        let Some(role) = Role::parse(&role) else {
            return Ok(None);
        };
        c.exec_drop(
            format!(
                "INSERT INTO {} (user_id, proposal_id, outcome, note, created_at) \
                 VALUES (?, ?, ?, ?, UTC_TIMESTAMP(3))",
                self.t("ai_feedback")
            ),
            (user.0, msg_id, outcome.as_str(), clean_note(note)),
        )
        .await
        .map_err(unavailable)?;
        Ok(Some((role, kind.unwrap_or_default())))
    }

    async fn recent(&self, limit: u32) -> Result<Vec<Note>> {
        let mut c = self.conn().await?;
        let rows: Vec<(Option<i8>, Option<String>, Option<String>, Option<String>)> = c
            .exec(
                format!(
                    "SELECT f.vote, f.outcome, f.note, CAST(m.body AS CHAR) FROM {} f \
                     LEFT JOIN {} m ON m.msg_id = f.proposal_id \
                     WHERE f.outcome IS NOT NULL OR f.note IS NOT NULL \
                     ORDER BY f.fb_id DESC LIMIT ?",
                    self.t("ai_feedback"),
                    self.t("ai_messages")
                ),
                (limit,),
            )
            .await
            .map_err(unavailable)?;
        Ok(rows
            .into_iter()
            .map(|(vote, outcome, note, body)| {
                let p: Option<Value> = body.and_then(|b| serde_json::from_str(&b).ok());
                Note {
                    vote,
                    outcome: outcome.as_deref().and_then(Outcome::parse),
                    proposal_kind: p
                        .as_ref()
                        .and_then(|b| b["kind"].as_str())
                        .map(str::to_owned),
                    label: p.as_ref().and_then(label_of),
                    note,
                }
            })
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;
    use crate::audit::AuditStore;

    #[tokio::test]
    async fn feedback_is_the_owners_and_feeds_the_prompt() {
        let a = MemoryAudit::default();
        let ann = UserId(1);
        let bob = UserId(2);
        let c = a.create(ann, Role::Data, "q").await.unwrap();
        let answer = a
            .append(
                c,
                MsgKind::Assistant,
                &json!({"role": "assistant", "content": []}),
                None,
            )
            .await
            .unwrap();
        a.append(
            c,
            MsgKind::Proposal,
            &json!({"id": "p-1", "kind": "sql", "tool_use_id": "t1",
                    "body": {"db": "shop", "sql": "DELETE FROM t", "why": "remove test rows"}}),
            None,
        )
        .await
        .unwrap();
        assert_eq!(a.vote(bob, answer, 1, None).await.unwrap(), None);
        assert_eq!(
            a.vote(ann, answer, -1, Some("  too long  ")).await.unwrap(),
            Some(Role::Data)
        );
        assert_eq!(
            a.outcome(bob, "p-1", Outcome::Accepted, None)
                .await
                .unwrap(),
            None
        );
        assert_eq!(
            a.outcome(ann, "p-1", Outcome::Dismissed, Some("never delete"))
                .await
                .unwrap(),
            Some((Role::Data, "sql".to_owned()))
        );
        assert_eq!(
            a.outcome(ann, "nope", Outcome::Dismissed, None)
                .await
                .unwrap(),
            None
        );
        let notes = a.recent(PROMPT_NOTES).await.unwrap();
        assert_eq!(notes.len(), 2);
        let p = prompt(&notes).unwrap();
        assert!(
            p.contains("- dismissed sql proposal: remove test rows (their note: never delete)"),
            "{p}"
        );
        assert!(p.contains("thumbs down: too long"), "{p}");
        assert_eq!(prompt(&[]), None);
    }
}

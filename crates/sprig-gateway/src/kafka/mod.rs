//! The Kafka listener on 9092: [`KafkaFrontend`] runs `sprig-proto-kafka`'s
//! broker on each connection, over this gateway's pieces:
//!
//! - [`identity`]: SASL PLAIN and SCRAM onto the gateway's
//!   [`LoginService`](sprig_auth::mysql::LoginService);
//! - [`cluster`]: brokers from the DC's gateway records (broker id = the
//!   gateway's slot) and coordinator leases in etcd
//!   (`stream-groups/<tenant_id>/<group>`);
//! - [`catalog`]: topics as streams (read from the tenant's catalog
//!   snapshot, filtered by privilege, changed with stream DDL run as the
//!   user), and consumer groups in `stream_groups` and `stream_offsets`
//!   through `sprig-streams`' store;
//! - [`data`]: Produce, Fetch, ListOffsets and OffsetForLeaderEpoch over
//!   `sprig-streams`' publish and log reader, as `sprig-proto-kafka`'s data
//!   plane.

// Kafka API and config names fill the docs.
#![allow(clippy::doc_markdown)]

pub mod catalog;
pub mod cluster;
pub mod data;
pub mod identity;

use std::borrow::Cow;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use sprig_common::ids::DcId;
use sprig_common::{CancellationToken, Result};
use sprig_proto_kafka::broker::{Broker, BrokerConfig, ConnInfo, ProducerIds};
use sprig_proto_kafka::group::{Coordinator, GroupConfig};
use sprig_settings::{Apply, HardBounds, LiveSettings, Scopes, Setting, SettingCtx};

use crate::frontend::{ClientConn, Frontend, GatewayContext, Protocol};

type Text = Cow<'static, str>;

/// Tenant and global.
const TENANT_GLOBAL: Scopes = Scopes(0b10_0100);

macro_rules! key {
    ($(#[$doc:meta])* $name:ident: $t:ty = $key:literal, $default:expr, $scopes:expr, $hard:expr, $text:literal) => {
        $(#[$doc])*
        pub static $name: Setting<$t> = Setting {
            key: $key,
            default: $default,
            scopes: $scopes,
            hard: $hard,
            apply: Apply::Live,
            operator_only: false,
            owner: 45,
            doc: $text,
        };
        sprig_settings::register!($name);
    };
}

key!(
    /// `kafka.advertised_host`: the host Metadata gives for this gateway;
    /// empty for the gateway's advertised address.
    ADVERTISED_HOST: Text = "kafka.advertised_host", Cow::Borrowed(""), Scopes::PROCESS,
    HardBounds::NONE,
    "Host in Kafka Metadata; empty for the gateway's address"
);
key!(
    /// `kafka.sasl_mechanisms`: offered SASL mechanisms, in order. Read
    /// before login, so only the dc and global values apply.
    SASL_MECHANISMS: Text = "kafka.sasl_mechanisms", Cow::Borrowed("SCRAM-SHA-512,SCRAM-SHA-256,PLAIN"),
    Scopes::PROCESS, HardBounds::NONE,
    "Offered SASL mechanisms on the Kafka listener"
);
key!(
    /// `kafka.group_session_timeout_max_ms`.
    GROUP_SESSION_TIMEOUT_MAX_MS: u64 = "kafka.group_session_timeout_max_ms", 300_000, Scopes::PROCESS,
    HardBounds::range(6_000, 3_600_000),
    "Largest session timeout a consumer group member may ask for"
);
key!(
    /// `kafka.group_initial_rebalance_delay_ms`: how long the first
    /// rebalance of an empty group waits for more members (Kafka's
    /// `group.initial.rebalance.delay.ms`).
    GROUP_INITIAL_REBALANCE_DELAY_MS: u64 = "kafka.group_initial_rebalance_delay_ms", 3_000,
    Scopes::PROCESS, HardBounds::range(0, 300_000),
    "Delay of the first rebalance of an empty consumer group"
);

key!(
    /// `kafka.fetch_max_wait_ms`: the cap on a fetch's wait for records.
    FETCH_MAX_WAIT_MS: u64 = "kafka.fetch_max_wait_ms", 500, TENANT_GLOBAL,
    HardBounds::range(0, 30_000),
    "Cap on a Kafka fetch's wait for new records"
);

/// The Kafka front end: one broker for every connection of the listener.
pub struct KafkaFrontend {
    broker: Arc<Broker>,
}

impl std::fmt::Debug for KafkaFrontend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KafkaFrontend").finish_non_exhaustive()
    }
}

/// What the front end is built from.
pub struct KafkaDeps {
    /// Live settings.
    pub live: LiveSettings,
    /// This gateway's DC.
    pub dc: DcId,
    /// Logins.
    pub identity: Arc<identity::KafkaIdentity>,
    /// Brokers and leases.
    pub cluster: Arc<cluster::GatewayCluster>,
    /// Topics.
    pub topics: Arc<dyn sprig_proto_kafka::topics::Topics>,
    /// Groups' durable part.
    pub groups: Arc<dyn sprig_proto_kafka::group::GroupStore>,
    /// Produce, Fetch, ListOffsets, OffsetForLeaderEpoch.
    pub data: Option<Arc<dyn sprig_proto_kafka::broker::data::StreamData>>,
}

impl std::fmt::Debug for KafkaDeps {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KafkaDeps").finish_non_exhaustive()
    }
}

fn group_config(live: &LiveSettings, dc: &DcId) -> GroupConfig {
    let ctx = SettingCtx::process(dc);
    GroupConfig {
        max_session: Duration::from_millis(live.get(&GROUP_SESSION_TIMEOUT_MAX_MS, &ctx)),
        initial_rebalance_delay: Duration::from_millis(
            live.get(&GROUP_INITIAL_REBALANCE_DELAY_MS, &ctx),
        ),
        ..GroupConfig::default()
    }
}

impl KafkaFrontend {
    /// Builds the front end and starts the group coordinator's timers,
    /// which stop on `stop`.
    #[must_use]
    pub fn start(d: KafkaDeps, stop: CancellationToken) -> Self {
        let groups = Arc::new(Coordinator::new(
            Arc::clone(&d.groups),
            Arc::clone(&d.cluster) as Arc<dyn sprig_proto_kafka::group::Leases>,
            group_config(&d.live, &d.dc),
        ));
        tokio::spawn(Arc::clone(&groups).run(stop.clone()));
        // Settings follow without a restart.
        {
            let live = d.live.clone();
            let dc = d.dc.clone();
            let groups = Arc::clone(&groups);
            let stop = stop.clone();
            tokio::spawn(async move {
                loop {
                    tokio::select! {
                        () = stop.cancelled() => return,
                        () = tokio::time::sleep(Duration::from_secs(5)) => {}
                    }
                    groups.set_config(group_config(&live, &dc));
                }
            });
        }
        tokio::spawn(Arc::clone(&d.cluster).keep_leases(stop));
        let live = d.live.clone();
        let dc = d.dc.clone();
        let node = sprig_proto_kafka::broker::Cluster::node_id(d.cluster.as_ref());
        let broker = Broker {
            identity: d.identity,
            cluster: d.cluster,
            topics: d.topics,
            groups,
            data: d.data.map(|s| {
                Arc::new(sprig_proto_kafka::broker::data::DataService::new(s))
                    as Arc<dyn sprig_proto_kafka::broker::DataPlane>
            }),
            config: Arc::new(move || {
                let ctx = SettingCtx::process(&dc);
                let mechs = live.get(&SASL_MECHANISMS, &ctx);
                let tls_off = live.get(&crate::listener::settings::KAFKA_TLS, &ctx) == "off";
                BrokerConfig {
                    mechanisms: mechs
                        .split(',')
                        .map(|m| m.trim().to_ascii_uppercase())
                        .filter(|m| !m.is_empty())
                        .collect(),
                    // PLAIN without TLS only where the operator turned TLS
                    // off on this listener.
                    plain_without_tls: tls_off,
                    ..BrokerConfig::default()
                }
            }),
            producer_ids: ProducerIds::new(node),
        };
        Self {
            broker: Arc::new(broker),
        }
    }
}

#[async_trait]
impl Frontend for KafkaFrontend {
    fn protocol(&self) -> Protocol {
        Protocol::Kafka
    }

    async fn serve(&self, mut conn: ClientConn, _ctx: Arc<GatewayContext>) -> Result<()> {
        let tls = conn.tls_info();
        let info = ConnInfo {
            peer: conn.peer,
            tls: tls.is_some(),
            sni: tls.as_ref().and_then(|t| t.sni.clone()),
            client_cert: tls.as_ref().is_some_and(|t| t.client_cert),
            client_dn: tls
                .as_ref()
                .filter(|t| t.client_cert)
                .and_then(|t| t.client_dn.clone())
                .map(|d| (d.subject, d.issuer)),
        };
        let close = conn.close_soon.clone();
        match self.broker.serve(&mut conn.stream, info, close).await {
            Ok(()) => Ok(()),
            Err(e) => {
                tracing::debug!(peer = %conn.peer, error = %e, "kafka connection ended");
                Ok(())
            }
        }
    }
}

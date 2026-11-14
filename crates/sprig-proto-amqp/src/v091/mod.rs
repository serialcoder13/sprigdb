//! AMQP 0-9-1 with RabbitMQ's extensions:
//! the codec ([`codec`], [`method`], [`props`], [`frame`]) and the server
//! front end ([`server`]).

pub mod client;
pub mod codec;
pub mod frame;
pub mod method;
pub mod props;
pub mod server;

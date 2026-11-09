//! AMQP 0-9-1 with RabbitMQ's extensions (chapter 50 steps 11 and 12):
//! the codec ([`codec`], [`method`], [`props`], [`frame`]) and the server
//! front end ([`server`]).

pub mod codec;
pub mod frame;
pub mod method;
pub mod props;
pub mod server;

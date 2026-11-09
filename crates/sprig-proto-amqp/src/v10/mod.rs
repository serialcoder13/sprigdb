//! AMQP 1.0 in RabbitMQ 4's dialect (chapter 50 steps 13 and 14): the type
//! system ([`types`]), performatives and framing ([`performative`]),
//! message sections and v2 addresses ([`message`]).

pub mod client;
pub mod message;
pub mod performative;
pub mod server;
pub mod types;

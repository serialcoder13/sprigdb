//! The data commands, by type, and their dispatch.

pub mod geo;
pub mod hashes;
pub mod keys;
pub mod lists;
pub mod server;
pub mod sort;
pub mod strings;
pub mod zsets;

use bytes::Bytes;
use sprig_kv::layout::KeyType;
use sprig_proto_redis::Frame;

use crate::ctx::{Ctx, R, int_arg};
use crate::reply;
use hashes::SetOp;
use lists::End;
use zsets::{By0, ZOp};

/// Runs data command `name` (lower case) with arguments `a` (`a[0]` the
/// name) in `cx`. `None` when `name` isn't a data command.
#[allow(clippy::too_many_lines)]
pub async fn run(cx: &mut Ctx<'_>, name: &str, a: &[Bytes]) -> Option<R<Frame>> {
    Some(match name {
        // Strings.
        "get" => strings::get(cx, a).await,
        "set" => strings::set(cx, a).await,
        "setnx" => strings::setnx(cx, a).await,
        "setex" => strings::setex(cx, a, 1000, "setex").await,
        "psetex" => strings::setex(cx, a, 1, "psetex").await,
        "getset" => strings::getset(cx, a).await,
        "getdel" => strings::getdel(cx, a).await,
        "getex" => strings::getex(cx, a).await,
        "mget" => strings::mget(cx, a).await,
        "mset" => strings::mset(cx, a, false).await,
        "msetnx" => strings::mset(cx, a, true).await,
        "append" => strings::append(cx, a).await,
        "strlen" => strings::strlen(cx, a).await,
        "incr" => strings::incrby(cx, &a[1], 1).await,
        "decr" => strings::incrby(cx, &a[1], -1).await,
        "incrby" => match int_arg(&a[2]) {
            Ok(by) => strings::incrby(cx, &a[1], by).await,
            Err(e) => Err(e),
        },
        "decrby" => strings::decrby(cx, a).await,
        "incrbyfloat" => strings::incrbyfloat(cx, a).await,
        "getrange" | "substr" => strings::getrange(cx, a).await,
        "setrange" => strings::setrange(cx, a).await,
        "setbit" => strings::setbit(cx, a).await,
        "getbit" => strings::getbit(cx, a).await,
        "bitcount" => strings::bitcount(cx, a).await,
        "bitpos" => strings::bitpos(cx, a).await,
        "bitop" => strings::bitop(cx, a).await,
        "bitfield" => strings::bitfield(cx, a, false).await,
        "bitfield_ro" => strings::bitfield(cx, a, true).await,
        "lcs" => strings::lcs(cx, a).await,
        // Keys.
        "del" | "unlink" => keys::del(cx, a).await,
        "exists" => keys::exists(cx, a).await,
        "touch" => keys::touch(cx, a).await,
        "type" => keys::type_(cx, a).await,
        "expire" => keys::expire(cx, a, 1000, false).await,
        "pexpire" => keys::expire(cx, a, 1, false).await,
        "expireat" => keys::expire(cx, a, 1000, true).await,
        "pexpireat" => keys::expire(cx, a, 1, true).await,
        "ttl" => keys::ttl(cx, a, false, false).await,
        "pttl" => keys::ttl(cx, a, true, false).await,
        "expiretime" => keys::ttl(cx, a, false, true).await,
        "pexpiretime" => keys::ttl(cx, a, true, true).await,
        "persist" => keys::persist(cx, a).await,
        "rename" => keys::rename(cx, a, false).await,
        "renamenx" => keys::rename(cx, a, true).await,
        "copy" => keys::copy(cx, a).await,
        "move" => keys::move_(cx, a).await,
        "keys" => keys::keys(cx, a).await,
        "scan" => keys::scan(cx, a).await,
        "randomkey" => keys::randomkey(cx).await,
        "dbsize" => keys::dbsize(cx).await,
        "flushdb" => keys::flushdb(cx, a).await,
        "flushall" => keys::flushall(cx, a).await,
        "object" => keys::object(cx, a).await,
        "sort" => sort::sort(cx, a, false).await,
        "sort_ro" => sort::sort(cx, a, true).await,
        // Hashes.
        "hset" => hashes::hset(cx, a, false).await,
        "hmset" => hashes::hset(cx, a, true).await,
        "hsetnx" => hashes::hsetnx(cx, a).await,
        "hget" => hashes::hget(cx, a).await,
        "hmget" => hashes::hmget(cx, a).await,
        "hgetall" => hashes::hgetall(cx, a, b'a').await,
        "hkeys" => hashes::hgetall(cx, a, b'k').await,
        "hvals" => hashes::hgetall(cx, a, b'v').await,
        "hlen" => hashes::card(cx, a, KeyType::Hash).await,
        "hexists" => hashes::exists(cx, a, KeyType::Hash).await,
        "hdel" => hashes::hdel(cx, a, KeyType::Hash).await,
        "hincrby" => hashes::hincrby(cx, a).await,
        "hincrbyfloat" => hashes::hincrbyfloat(cx, a).await,
        "hstrlen" => hashes::hstrlen(cx, a).await,
        "hrandfield" => hashes::hrandfield(cx, a).await,
        "hscan" => hashes::escan(cx, a, KeyType::Hash).await,
        // Lists.
        "lpush" => lists::lpush(cx, a, End::Left, false).await,
        "rpush" => lists::lpush(cx, a, End::Right, false).await,
        "lpushx" => lists::lpush(cx, a, End::Left, true).await,
        "rpushx" => lists::lpush(cx, a, End::Right, true).await,
        "lpop" => lists::lpop(cx, a, End::Left).await,
        "rpop" => lists::lpop(cx, a, End::Right).await,
        "llen" => lists::llen(cx, a).await,
        "lrange" => lists::lrange(cx, a).await,
        "lindex" => lists::lindex(cx, a).await,
        "lset" => lists::lset(cx, a).await,
        "linsert" => lists::linsert(cx, a).await,
        "lrem" => lists::lrem(cx, a).await,
        "ltrim" => lists::ltrim(cx, a).await,
        "lpos" => lists::lpos(cx, a).await,
        "lmove" => lists::lmove(cx, a).await,
        "rpoplpush" => lists::rpoplpush(cx, a).await,
        "lmpop" => lists::lmpop(cx, a).await,
        // Sets.
        "sadd" => hashes::sadd(cx, a).await,
        "srem" => hashes::hdel(cx, a, KeyType::Set).await,
        "smembers" => hashes::smembers(cx, a).await,
        "sismember" => hashes::exists(cx, a, KeyType::Set).await,
        "smismember" => hashes::smismember(cx, a).await,
        "scard" => hashes::card(cx, a, KeyType::Set).await,
        "spop" => hashes::spop(cx, a).await,
        "srandmember" => hashes::srandmember(cx, a).await,
        "smove" => hashes::smove(cx, a).await,
        "sinter" => hashes::salgebra(cx, a, SetOp::Inter).await,
        "sunion" => hashes::salgebra(cx, a, SetOp::Union).await,
        "sdiff" => hashes::salgebra(cx, a, SetOp::Diff).await,
        "sinterstore" => hashes::sstore(cx, a, SetOp::Inter).await,
        "sunionstore" => hashes::sstore(cx, a, SetOp::Union).await,
        "sdiffstore" => hashes::sstore(cx, a, SetOp::Diff).await,
        "sintercard" => hashes::sintercard(cx, a).await,
        "sscan" => hashes::escan(cx, a, KeyType::Set).await,
        // Sorted sets.
        "zadd" => zsets::zadd(cx, a).await,
        "zincrby" => zsets::zincrby(cx, a).await,
        "zrem" => zsets::zrem(cx, a).await,
        "zscore" => zsets::zscore(cx, a).await,
        "zmscore" => zsets::zmscore(cx, a).await,
        "zcard" => zsets::zcard(cx, a).await,
        "zcount" => zsets::zcount(cx, a).await,
        "zlexcount" => zsets::zlexcount(cx, a).await,
        "zrange" => zsets::zrange(cx, a, By0::Generic, false).await,
        "zrevrange" => zsets::zrange(cx, a, By0::Rank, true).await,
        "zrangebyscore" => zsets::zrange(cx, a, By0::Score, false).await,
        "zrevrangebyscore" => zsets::zrange(cx, a, By0::Score, true).await,
        "zrangebylex" => zsets::zrange(cx, a, By0::Lex, false).await,
        "zrevrangebylex" => zsets::zrange(cx, a, By0::Lex, true).await,
        "zrangestore" => zsets::zrangestore(cx, a).await,
        "zrank" => zsets::zrank(cx, a, false).await,
        "zrevrank" => zsets::zrank(cx, a, true).await,
        "zremrangebyrank" => zsets::zremrange(cx, a, b'r').await,
        "zremrangebyscore" => zsets::zremrange(cx, a, b's').await,
        "zremrangebylex" => zsets::zremrange(cx, a, b'l').await,
        "zpopmin" => zsets::zpop(cx, a, false).await,
        "zpopmax" => zsets::zpop(cx, a, true).await,
        "zmpop" => zsets::zmpop(cx, a).await,
        "zrandmember" => zsets::zrandmember(cx, a).await,
        "zunion" => zsets::zalg(cx, a, ZOp::Union).await,
        "zinter" => zsets::zalg(cx, a, ZOp::Inter).await,
        "zdiff" => zsets::zalg(cx, a, ZOp::Diff).await,
        "zunionstore" => zsets::zalgstore(cx, a, ZOp::Union).await,
        "zinterstore" => zsets::zalgstore(cx, a, ZOp::Inter).await,
        "zdiffstore" => zsets::zalgstore(cx, a, ZOp::Diff).await,
        "zintercard" => zsets::zintercard(cx, a).await,
        "zscan" => zsets::zscan(cx, a).await,
        // Geo.
        "geoadd" => geo::geoadd(cx, a).await,
        "geopos" => geo::geopos(cx, a).await,
        "geodist" => geo::geodist(cx, a).await,
        "geohash" => geo::geohash(cx, a).await,
        "geosearch" => geo::geosearch(cx, a, false).await,
        "geosearchstore" => geo::geosearch(cx, a, true).await,
        "georadius" | "georadius_ro" => geo::georadius(cx, a, false, name.ends_with("_ro")).await,
        "georadiusbymember" | "georadiusbymember_ro" => {
            geo::georadius(cx, a, true, name.ends_with("_ro")).await
        }
        // Server commands that need only the data.
        "time" => server::time(cx).await,
        "wait" => Ok(Frame::Integer(0)),
        "waitaof" => Ok(Frame::Array(vec![Frame::Integer(0), Frame::Integer(0)])),
        _ => return None,
    })
}

/// Commands whose writes push to a key (wake blocked clients).
#[must_use]
pub fn unsupported(name: &str) -> Frame {
    match name {
        n if n.starts_with('x') => {
            reply::unsupported(n, "not supported in this version (Redis streams)")
        }
        n if n.starts_with("pf") => {
            reply::unsupported(n, "not supported in this version (HyperLogLog)")
        }
        "monitor" => reply::unsupported(name, "not offered yet"),
        n => reply::restricted(n),
    }
}

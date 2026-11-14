//! The command table: arity, flags and
//! key positions, as Redis 7.2's `COMMAND INFO` gives them.

use std::collections::HashMap;
use std::sync::OnceLock;

/// What a command is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Spec {
    /// Lower-case name.
    pub name: &'static str,
    /// Arity: `n` exactly, `-n` at least `n` (the name counts).
    pub arity: i32,
    /// Flags, one letter each (see [`Spec::has`]).
    pub flags: &'static str,
    /// First key argument (0 = none).
    pub first: i32,
    /// Last key argument (-1 = the last argument, -2 the one before).
    pub last: i32,
    /// Step between keys.
    pub step: i32,
}

impl Spec {
    /// `w` write, `r` read-only, `s` not in scripts, `p` pub/sub, `b` may
    /// block, `a` admin, `m` not queued by `MULTI` (runs at once), `S`
    /// allowed in RESP2 subscribed mode, `n` before authentication, `x`
    /// not offered (39840 or 39842), `o` a no-op.
    #[must_use]
    pub fn has(&self, flag: char) -> bool {
        self.flags.contains(flag)
    }

    /// Whether `argc` (name included) fits the arity.
    #[must_use]
    pub fn arity_ok(&self, argc: usize) -> bool {
        let argc = i64::try_from(argc).unwrap_or(i64::MAX);
        let a = i64::from(self.arity);
        if a >= 0 { argc == a } else { argc >= -a }
    }

    /// The key arguments' indexes for `argc` arguments.
    #[must_use]
    pub fn key_indexes(&self, argc: usize) -> Vec<usize> {
        if self.first <= 0 {
            return Vec::new();
        }
        let argc = i32::try_from(argc).unwrap_or(i32::MAX);
        let last = if self.last < 0 {
            argc + self.last
        } else {
            self.last
        };
        let mut out = Vec::new();
        let mut i = self.first;
        while i <= last && i < argc {
            out.push(usize::try_from(i).unwrap_or(0));
            i += self.step.max(1);
        }
        out
    }
}

macro_rules! specs {
    ($( $name:literal $arity:literal $flags:literal $first:literal $last:literal $step:literal ;)*) => {
        &[ $( Spec { name: $name, arity: $arity, flags: $flags, first: $first, last: $last, step: $step }, )* ]
    };
}

/// Every command SprigDB knows, built or not.
pub const ALL: &[Spec] = specs! {
    // Connection.
    "ping" -1 "rS" 0 0 0;
    "echo" 2 "r" 0 0 0;
    "quit" -1 "rSnm" 0 0 0;
    "reset" 1 "rSnm" 0 0 0;
    "select" 2 "r" 0 0 0;
    "hello" -1 "rsnm" 0 0 0;
    "auth" -2 "rsnm" 0 0 0;
    "client" -2 "rs" 0 0 0;
    "swapdb" 3 "wa" 0 0 0;
    // Strings.
    "get" 2 "r" 1 1 1;
    "set" -3 "w" 1 1 1;
    "setnx" 3 "w" 1 1 1;
    "setex" 4 "w" 1 1 1;
    "psetex" 4 "w" 1 1 1;
    "getset" 3 "w" 1 1 1;
    "getdel" 2 "w" 1 1 1;
    "getex" -2 "w" 1 1 1;
    "mget" -2 "r" 1 -1 1;
    "mset" -3 "w" 1 -1 2;
    "msetnx" -3 "w" 1 -1 2;
    "append" 3 "w" 1 1 1;
    "strlen" 2 "r" 1 1 1;
    "incr" 2 "w" 1 1 1;
    "decr" 2 "w" 1 1 1;
    "incrby" 3 "w" 1 1 1;
    "decrby" 3 "w" 1 1 1;
    "incrbyfloat" 3 "w" 1 1 1;
    "getrange" 4 "r" 1 1 1;
    "substr" 4 "r" 1 1 1;
    "setrange" 4 "w" 1 1 1;
    "lcs" -3 "r" 1 2 1;
    "setbit" 4 "w" 1 1 1;
    "getbit" 3 "r" 1 1 1;
    "bitcount" -2 "r" 1 1 1;
    "bitpos" -3 "r" 1 1 1;
    "bitop" -4 "w" 2 -1 1;
    "bitfield" -2 "w" 1 1 1;
    "bitfield_ro" -2 "r" 1 1 1;
    // Keys.
    "del" -2 "w" 1 -1 1;
    "unlink" -2 "w" 1 -1 1;
    "exists" -2 "r" 1 -1 1;
    "type" 2 "r" 1 1 1;
    "expire" -3 "w" 1 1 1;
    "pexpire" -3 "w" 1 1 1;
    "expireat" -3 "w" 1 1 1;
    "pexpireat" -3 "w" 1 1 1;
    "expiretime" 2 "r" 1 1 1;
    "pexpiretime" 2 "r" 1 1 1;
    "ttl" 2 "r" 1 1 1;
    "pttl" 2 "r" 1 1 1;
    "persist" 2 "w" 1 1 1;
    "rename" 3 "w" 1 2 1;
    "renamenx" 3 "w" 1 2 1;
    "copy" -3 "w" 1 2 1;
    "move" 3 "w" 1 1 1;
    "keys" 2 "r" 0 0 0;
    "scan" -2 "r" 0 0 0;
    "randomkey" 1 "r" 0 0 0;
    "dbsize" 1 "r" 0 0 0;
    "flushdb" -1 "w" 0 0 0;
    "flushall" -1 "wa" 0 0 0;
    "touch" -2 "r" 1 -1 1;
    "object" -2 "r" 2 2 1;
    "sort" -2 "w" 1 1 1;
    "sort_ro" -2 "r" 1 1 1;
    "wait" 3 "r" 0 0 0;
    "waitaof" 4 "r" 0 0 0;
    "dump" 2 "rx" 1 1 1;
    "restore" -4 "wx" 1 1 1;
    "migrate" -6 "wx" 0 0 0;
    // Hashes.
    "hset" -4 "w" 1 1 1;
    "hsetnx" 4 "w" 1 1 1;
    "hmset" -4 "w" 1 1 1;
    "hget" 3 "r" 1 1 1;
    "hmget" -3 "r" 1 1 1;
    "hgetall" 2 "r" 1 1 1;
    "hkeys" 2 "r" 1 1 1;
    "hvals" 2 "r" 1 1 1;
    "hlen" 2 "r" 1 1 1;
    "hexists" 3 "r" 1 1 1;
    "hdel" -3 "w" 1 1 1;
    "hincrby" 4 "w" 1 1 1;
    "hincrbyfloat" 4 "w" 1 1 1;
    "hstrlen" 3 "r" 1 1 1;
    "hrandfield" -2 "r" 1 1 1;
    "hscan" -3 "r" 1 1 1;
    // Lists.
    "lpush" -3 "w" 1 1 1;
    "rpush" -3 "w" 1 1 1;
    "lpushx" -3 "w" 1 1 1;
    "rpushx" -3 "w" 1 1 1;
    "lpop" -2 "w" 1 1 1;
    "rpop" -2 "w" 1 1 1;
    "llen" 2 "r" 1 1 1;
    "lrange" 4 "r" 1 1 1;
    "lindex" 3 "r" 1 1 1;
    "lset" 4 "w" 1 1 1;
    "linsert" 5 "w" 1 1 1;
    "lrem" 4 "w" 1 1 1;
    "ltrim" 4 "w" 1 1 1;
    "lpos" -3 "r" 1 1 1;
    "lmove" 5 "w" 1 2 1;
    "rpoplpush" 3 "w" 1 2 1;
    "lmpop" -4 "w" 0 0 0;
    "blpop" -3 "wb" 1 -2 1;
    "brpop" -3 "wb" 1 -2 1;
    "blmove" 6 "wb" 1 2 1;
    "brpoplpush" 4 "wb" 1 2 1;
    "blmpop" -5 "wb" 0 0 0;
    // Sets.
    "sadd" -3 "w" 1 1 1;
    "srem" -3 "w" 1 1 1;
    "smembers" 2 "r" 1 1 1;
    "sismember" 3 "r" 1 1 1;
    "smismember" -3 "r" 1 1 1;
    "scard" 2 "r" 1 1 1;
    "spop" -2 "w" 1 1 1;
    "srandmember" -2 "r" 1 1 1;
    "smove" 4 "w" 1 2 1;
    "sinter" -2 "r" 1 -1 1;
    "sinterstore" -3 "w" 1 -1 1;
    "sintercard" -3 "r" 0 0 0;
    "sunion" -2 "r" 1 -1 1;
    "sunionstore" -3 "w" 1 -1 1;
    "sdiff" -2 "r" 1 -1 1;
    "sdiffstore" -3 "w" 1 -1 1;
    "sscan" -3 "r" 1 1 1;
    // Sorted sets.
    "zadd" -4 "w" 1 1 1;
    "zrem" -3 "w" 1 1 1;
    "zscore" 3 "r" 1 1 1;
    "zmscore" -3 "r" 1 1 1;
    "zincrby" 4 "w" 1 1 1;
    "zcard" 2 "r" 1 1 1;
    "zcount" 4 "r" 1 1 1;
    "zrange" -4 "r" 1 1 1;
    "zrangestore" -5 "w" 1 2 1;
    "zrangebyscore" -4 "r" 1 1 1;
    "zrevrangebyscore" -4 "r" 1 1 1;
    "zrangebylex" -4 "r" 1 1 1;
    "zrevrangebylex" -4 "r" 1 1 1;
    "zrevrange" -4 "r" 1 1 1;
    "zrank" -3 "r" 1 1 1;
    "zrevrank" -3 "r" 1 1 1;
    "zremrangebyrank" 4 "w" 1 1 1;
    "zremrangebyscore" 4 "w" 1 1 1;
    "zremrangebylex" 4 "w" 1 1 1;
    "zlexcount" 4 "r" 1 1 1;
    "zpopmin" -2 "w" 1 1 1;
    "zpopmax" -2 "w" 1 1 1;
    "bzpopmin" -3 "wb" 1 -2 1;
    "bzpopmax" -3 "wb" 1 -2 1;
    "zmpop" -4 "w" 0 0 0;
    "bzmpop" -5 "wb" 0 0 0;
    "zrandmember" -2 "r" 1 1 1;
    "zunion" -3 "r" 0 0 0;
    "zinter" -3 "r" 0 0 0;
    "zdiff" -3 "r" 0 0 0;
    "zunionstore" -4 "w" 1 1 1;
    "zinterstore" -4 "w" 1 1 1;
    "zdiffstore" -4 "w" 1 1 1;
    "zintercard" -3 "r" 0 0 0;
    "zscan" -3 "r" 1 1 1;
    // Geo (sorted sets scored by geohash).
    "geoadd" -5 "w" 1 1 1;
    "geopos" -2 "r" 1 1 1;
    "geodist" -4 "r" 1 1 1;
    "geohash" -2 "r" 1 1 1;
    "geosearch" -7 "r" 1 1 1;
    "geosearchstore" -8 "w" 1 2 1;
    "georadius" -6 "w" 1 1 1;
    "georadius_ro" -6 "r" 1 1 1;
    "georadiusbymember" -5 "w" 1 1 1;
    "georadiusbymember_ro" -5 "r" 1 1 1;
    // Transactions.
    "multi" 1 "rsm" 0 0 0;
    "exec" 1 "wsm" 0 0 0;
    "discard" 1 "rsm" 0 0 0;
    "watch" -2 "rsm" 1 -1 1;
    "unwatch" 1 "rsm" 0 0 0;
    // Pub/sub.
    "subscribe" -2 "psS" 0 0 0;
    "unsubscribe" -1 "psS" 0 0 0;
    "psubscribe" -2 "psS" 0 0 0;
    "punsubscribe" -1 "psS" 0 0 0;
    "ssubscribe" -2 "psS" 1 -1 1;
    "sunsubscribe" -1 "psS" 1 -1 1;
    "publish" 3 "p" 0 0 0;
    "spublish" 3 "p" 1 1 1;
    "pubsub" -2 "p" 0 0 0;
    // Scripting.
    "eval" -3 "ws" 0 0 0;
    "evalsha" -3 "ws" 0 0 0;
    "eval_ro" -3 "rs" 0 0 0;
    "evalsha_ro" -3 "rs" 0 0 0;
    "script" -2 "rs" 0 0 0;
    "function" -2 "ws" 0 0 0;
    "fcall" -3 "ws" 0 0 0;
    "fcall_ro" -3 "rs" 0 0 0;
    // Server and cluster.
    "info" -1 "r" 0 0 0;
    "time" 1 "r" 0 0 0;
    "command" -1 "r" 0 0 0;
    "config" -2 "ra" 0 0 0;
    "acl" -2 "r" 0 0 0;
    "role" 1 "r" 0 0 0;
    "slowlog" -2 "ra" 0 0 0;
    "memory" -2 "r" 0 0 0;
    "latency" -2 "ra" 0 0 0;
    "lastsave" 1 "r" 0 0 0;
    "cluster" -2 "r" 0 0 0;
    "readonly" 1 "r" 0 0 0;
    "readwrite" 1 "r" 0 0 0;
    "asking" 1 "r" 0 0 0;
    "save" 1 "rao" 0 0 0;
    "bgsave" -1 "rao" 0 0 0;
    "bgrewriteaof" 1 "rao" 0 0 0;
    "lolwut" -1 "r" 0 0 0;
    "monitor" 1 "rasx" 0 0 0;
    "debug" -2 "asx" 0 0 0;
    "shutdown" -1 "asx" 0 0 0;
    "replicaof" 3 "asx" 0 0 0;
    "slaveof" 3 "asx" 0 0 0;
    "sync" 1 "asx" 0 0 0;
    "psync" -3 "asx" 0 0 0;
    "failover" -1 "asx" 0 0 0;
    "module" -2 "asx" 0 0 0;
    // Phase 2.
    "pfadd" -2 "wx" 1 1 1;
    "pfcount" -2 "rx" 1 -1 1;
    "pfmerge" -2 "wx" 1 -1 1;
    "xadd" -5 "wx" 1 1 1;
    "xrange" -4 "rx" 1 1 1;
    "xrevrange" -4 "rx" 1 1 1;
    "xlen" 2 "rx" 1 1 1;
    "xread" -4 "rx" 0 0 0;
    "xreadgroup" -7 "wx" 0 0 0;
    "xgroup" -2 "wx" 0 0 0;
    "xack" -4 "wx" 1 1 1;
    "xdel" -3 "wx" 1 1 1;
    "xtrim" -4 "wx" 1 1 1;
    "xpending" -3 "rx" 1 1 1;
    "xclaim" -6 "wx" 1 1 1;
    "xautoclaim" -6 "wx" 1 1 1;
    "xinfo" -2 "rx" 0 0 0;
    "xsetid" -3 "wx" 1 1 1;
};

/// The spec of a lower-case name.
#[must_use]
pub fn lookup(name: &str) -> Option<&'static Spec> {
    static MAP: OnceLock<HashMap<&'static str, &'static Spec>> = OnceLock::new();
    MAP.get_or_init(|| ALL.iter().map(|s| (s.name, s)).collect())
        .get(name)
        .copied()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keys_and_arity() {
        let s = lookup("mset").unwrap();
        assert_eq!(s.key_indexes(5), vec![1, 3]);
        assert!(s.arity_ok(3) && !s.arity_ok(2));
        assert_eq!(lookup("blpop").unwrap().key_indexes(4), vec![1, 2]);
        assert!(lookup("get").unwrap().arity_ok(2));
        assert!(!lookup("get").unwrap().arity_ok(3));
        let names: std::collections::HashSet<_> = ALL.iter().map(|s| s.name).collect();
        assert_eq!(names.len(), ALL.len(), "duplicate names");
    }
}

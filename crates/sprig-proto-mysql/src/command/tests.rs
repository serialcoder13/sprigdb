use std::collections::HashMap;

use bytes::Bytes;
use proptest::prelude::*;

use super::*;
use crate::resultset::column_type as ct;

/// Parameter count, bound types and long-data flags per statement id.
type StmtInfo = (u16, Option<Vec<u16>>, Vec<bool>);

struct Stmts(HashMap<u32, StmtInfo>);

impl StmtLookup for Stmts {
    fn param_info(&self, stmt_id: u32) -> Option<ParamInfo<'_>> {
        self.0.get(&stmt_id).map(|(count, types, long)| ParamInfo {
            count: *count,
            types: types.as_deref(),
            long_data: long,
        })
    }
}

const MY: Persona = Persona::MySql84;
const MARIA: Persona = Persona::MariaDb;

fn caps() -> Caps {
    Caps::server_default(MY)
}

fn round_trip(cmd: &Command, caps: Caps, persona: Persona) -> Command {
    let b = cmd.encode(caps).expect("encode");
    Command::parse(&b, caps, persona).expect("parse")
}

#[test]
fn simple_commands_round_trip() {
    let c = Caps::PROTOCOL_41;
    for cmd in [
        Command::Quit,
        Command::InitDb(Bytes::from_static(b"shop")),
        Command::Query(QueryAttrs::default(), Bytes::from_static(b"SELECT 1")),
        Command::FieldList(FieldList {
            table: Bytes::from_static(b"orders"),
            wildcard: Bytes::from_static(b"id%"),
        }),
        Command::CreateDb(Bytes::from_static(b"x")),
        Command::DropDb(Bytes::from_static(b"x")),
        Command::Statistics,
        Command::ProcessInfo,
        Command::ProcessKill(77),
        Command::Ping,
        Command::StmtPrepare(Bytes::from_static(b"SELECT ?")),
        Command::StmtSendLongData(LongData {
            stmt_id: 1,
            param: 2,
            data: Bytes::from_static(b"chunk"),
        }),
        Command::StmtClose(3),
        Command::StmtReset(4),
        Command::SetOption(MYSQL_OPTION_MULTI_STATEMENTS_OFF),
        Command::StmtFetch(5, 100),
        Command::ResetConnection,
        Command::Privileged(com::SHUTDOWN),
        Command::Unknown(0x20),
    ] {
        assert_eq!(round_trip(&cmd, c, MY), cmd, "{}", cmd.name());
    }
    assert_eq!(
        Command::parse(&Bytes::from_static(&[com::BINLOG_DUMP_GTID]), c, MY).ok(),
        Some(Command::Privileged(com::BINLOG_DUMP_GTID))
    );
    assert!(Command::parse(&Bytes::new(), c, MY).is_err());
    assert!(Command::parse(&Bytes::from_static(&[com::PROCESS_KILL, 1]), c, MY).is_err());
}

#[test]
fn query_attributes() {
    let caps = caps();
    let attrs = QueryAttrs {
        attrs: vec![
            BoundParam {
                param_type: u16::from(ct::VAR_STRING),
                name: Bytes::from_static(b"traceparent"),
                value: ParamValue::Value(Value::Bytes(Bytes::from_static(b"00-abc-def-01"))),
            },
            BoundParam {
                param_type: u16::from(ct::LONGLONG),
                name: Bytes::from_static(b"n"),
                value: ParamValue::Value(Value::Null),
            },
        ],
    };
    let cmd = Command::Query(attrs, Bytes::from_static(b"SELECT 1"));
    let got = round_trip(&cmd, caps, MY);
    assert_eq!(got, cmd);
    let Command::Query(a, sql) = got else {
        panic!("not a query")
    };
    assert_eq!(&sql[..], b"SELECT 1");
    assert_eq!(
        a.get(b"traceparent"),
        Some(&Value::Bytes(Bytes::from_static(b"00-abc-def-01")))
    );
    assert_eq!(a.get(b"n"), Some(&Value::Null));

    // The mysql client with no attributes: count 0, one set.
    let wire = Bytes::from_static(b"\x03\x00\x01SELECT 2");
    assert_eq!(
        Command::parse(&wire, caps, MY).ok(),
        Some(Command::Query(
            QueryAttrs::default(),
            Bytes::from_static(b"SELECT 2")
        ))
    );
    // Without the capability the whole rest is SQL.
    assert_eq!(
        Command::parse(&wire, Caps::PROTOCOL_41, MY).ok(),
        Some(Command::Query(
            QueryAttrs::default(),
            Bytes::from_static(b"\x00\x01SELECT 2")
        ))
    );
}

fn param(t: u8, v: Value) -> BoundParam {
    BoundParam {
        param_type: u16::from(t),
        name: Bytes::new(),
        value: ParamValue::Value(v),
    }
}

#[test]
fn execute_binds_types_then_reuses_them() {
    let caps = Caps::PROTOCOL_41;
    let first = Command::StmtExecute(StmtExecute {
        stmt_id: 1,
        flags: 0,
        iterations: 1,
        raw_params: Bytes::new(),
        params: Some(ExecuteParams {
            new_params_bound: true,
            params: vec![
                param(ct::LONGLONG, Value::Int(42)),
                param(ct::VAR_STRING, Value::Null),
                param(ct::VAR_STRING, Value::Bytes(Bytes::from_static(b"abc"))),
            ],
        }),
    });
    let b = first.encode(caps).expect("encode");
    // Header, id, flags, iterations, bitmap (param 1 NULL), bind flag.
    assert_eq!(&b[..12], &[0x17, 1, 0, 0, 0, 0, 1, 0, 0, 0, 0b010, 1]);
    let mut stmts = Stmts(HashMap::from([(1, (3, None, vec![]))]));
    let got = Command::parse_with(&b, caps, MY, &stmts).expect("parse");
    assert_eq!(
        got,
        Command::StmtExecute(StmtExecute {
            raw_params: b.slice(10..),
            ..match first.clone() {
                Command::StmtExecute(e) => e,
                _ => unreachable!(),
            }
        })
    );

    // Second execute: no types in the packet; they come from the lookup,
    // and parameter 0 got long data.
    let Command::StmtExecute(e) = &got else {
        unreachable!()
    };
    let types = e.params.as_ref().expect("params").types();
    stmts.0.insert(1, (3, Some(types), vec![true]));
    let second = Bytes::from_static(&[
        0x17, 1, 0, 0, 0, 0, 1, 0, 0, 0, 0b000, 0, 1, b'x', 2, b'y', b'z',
    ]);
    let Command::StmtExecute(e) = Command::parse_with(&second, caps, MY, &stmts).expect("parse")
    else {
        panic!("not an execute")
    };
    let p = e.params.expect("params");
    assert!(!p.new_params_bound);
    assert_eq!(p.params[0].value, ParamValue::LongData);
    assert_eq!(
        p.params[1].value,
        ParamValue::Value(Value::Bytes(Bytes::from_static(b"x")))
    );
    assert_eq!(p.params[2].param_type, u16::from(ct::VAR_STRING));

    // Unknown statement: parameters stay raw.
    let Command::StmtExecute(e) =
        Command::parse_with(&second, caps, MY, &Stmts(HashMap::new())).expect("parse")
    else {
        panic!("not an execute")
    };
    assert!(e.params.is_none());
    // No types bound anywhere: an error, not a guess.
    stmts.0.insert(1, (3, None, vec![]));
    assert!(Command::parse_with(&second, caps, MY, &stmts).is_err());
}

#[test]
fn execute_with_query_attributes() {
    let caps = caps();
    let cmd = Command::StmtExecute(StmtExecute {
        stmt_id: 2,
        flags: PARAMETER_COUNT_AVAILABLE,
        iterations: 1,
        raw_params: Bytes::new(),
        params: Some(ExecuteParams {
            new_params_bound: true,
            params: vec![
                param(ct::LONG, Value::Int(7)),
                BoundParam {
                    param_type: u16::from(ct::VAR_STRING),
                    name: Bytes::from_static(b"traceparent"),
                    value: ParamValue::Value(Value::Bytes(Bytes::from_static(b"tp"))),
                },
            ],
        }),
    });
    let b = cmd.encode(caps).expect("encode");
    let stmts = Stmts(HashMap::from([(2, (1, None, vec![]))]));
    let Command::StmtExecute(e) = Command::parse_with(&b, caps, MY, &stmts).expect("parse") else {
        panic!("not an execute")
    };
    let p = e.params.expect("params");
    assert_eq!(p.params.len(), 2);
    assert_eq!(&p.params[1].name[..], b"traceparent");
    assert_eq!(p.params[0].value, ParamValue::Value(Value::Int(7)));
}

/// Regression (fuzz target `command`): a re-execute under query
/// attributes whose parameters are all NULL carries a count larger than
/// the bytes after it (bitmap plus bind flag), and must still parse.
#[test]
fn execute_count_of_all_null_parameters_without_types() {
    let caps = caps();
    let types = vec![u16::from(ct::LONGLONG); 3];
    let stmts = Stmts(HashMap::from([(9, (3, Some(types), vec![]))]));
    // id 9, PARAMETER_COUNT_AVAILABLE, iterations 1, count 3, bitmap
    // 0b111, new_params_bind_flag 0.
    let b = Bytes::from_static(&[0x17, 9, 0, 0, 0, 0x08, 1, 0, 0, 0, 3, 0b111, 0]);
    let Command::StmtExecute(e) = Command::parse_with(&b, caps, MY, &stmts).expect("parse") else {
        panic!("not an execute")
    };
    let p = e.params.clone().expect("params");
    assert_eq!(p.params.len(), 3);
    assert!(
        p.params
            .iter()
            .all(|x| x.value == ParamValue::Value(Value::Null))
    );
    let cmd = Command::StmtExecute(e);
    assert_eq!(cmd.encode(caps).expect("encode"), b);
}

#[test]
fn bulk_execute_is_mariadb_only() {
    let caps = Caps::server_default(MARIA);
    let cmd = Command::StmtBulkExecute(BulkExecute {
        stmt_id: 0xFFFF_FFFF,
        flags: BULK_SEND_TYPES_TO_SERVER,
        raw: Bytes::new(),
        rows: Some(BulkRows {
            types: vec![u16::from(ct::LONG), u16::from(ct::VAR_STRING)],
            rows: vec![
                vec![
                    BulkValue::Value(Value::Int(1)),
                    BulkValue::Value(Value::Bytes(Bytes::from_static(b"a"))),
                ],
                vec![BulkValue::Null, BulkValue::Default],
                vec![BulkValue::Value(Value::Int(3)), BulkValue::Ignore],
            ],
        }),
    });
    let b = cmd.encode(caps).expect("encode");
    let stmts = Stmts(HashMap::from([(0xFFFF_FFFF, (2, None, vec![]))]));
    let Command::StmtBulkExecute(got) =
        Command::parse_with(&b, caps, MARIA, &stmts).expect("parse")
    else {
        panic!("not bulk")
    };
    let Command::StmtBulkExecute(want) = &cmd else {
        unreachable!()
    };
    assert_eq!(got.rows, want.rows);
    assert_eq!(
        Command::parse(&b, caps, MY).ok(),
        Some(Command::Unknown(com::STMT_BULK_EXECUTE))
    );
}

#[test]
fn change_user_round_trips() {
    let caps = caps();
    let cu = ChangeUser {
        user: Bytes::from_static(b"acme.bob"),
        auth_response: Bytes::from_static(&[9; 32]),
        database: Some(Bytes::from_static(b"shop")),
        charset: Some(255),
        plugin: Some("caching_sha2_password".to_owned()),
        attrs: vec![("program_name".to_owned(), "mysql".to_owned())],
    };
    let cmd = Command::ChangeUser(cu);
    assert_eq!(round_trip(&cmd, caps, MY), cmd);
    let old = Command::ChangeUser(ChangeUser {
        user: Bytes::from_static(b"u"),
        ..ChangeUser::default()
    });
    assert_eq!(round_trip(&old, caps, MY), old);
}

fn value_strategy() -> impl Strategy<Value = (u8, Value)> {
    prop_oneof![
        Just((ct::NULL, Value::Null)),
        any::<i64>().prop_map(|v| (ct::LONGLONG, Value::Int(v))),
        any::<i32>().prop_map(|v| (ct::LONG, Value::Int(i64::from(v)))),
        any::<i8>().prop_map(|v| (ct::TINY, Value::Int(i64::from(v)))),
        any::<f64>()
            .prop_filter("NaN never equals", |f| !f.is_nan())
            .prop_map(|v| (ct::DOUBLE, Value::Double(v))),
        prop::collection::vec(any::<u8>(), 0..300)
            .prop_map(|v| (ct::VAR_STRING, Value::Bytes(Bytes::from(v)))),
    ]
}

proptest! {
    #[test]
    fn prop_execute_round_trips(
        vals in prop::collection::vec(value_strategy(), 0..12),
        attrs in any::<bool>(),
    ) {
        let caps = if attrs { Caps::server_default(MY) } else { Caps::PROTOCOL_41 };
        let params: Vec<BoundParam> = vals.into_iter().map(|(t, v)| param(t, v)).collect();
        let count = u16::try_from(params.len()).unwrap_or(0);
        let cmd = Command::StmtExecute(StmtExecute {
            stmt_id: 5,
            flags: if attrs { PARAMETER_COUNT_AVAILABLE } else { 0 },
            iterations: 1,
            raw_params: Bytes::new(),
            params: Some(ExecuteParams { new_params_bound: true, params: params.clone() }),
        });
        let b = cmd.encode(caps).expect("encode");
        let stmts = Stmts(HashMap::from([(5, (count, None, vec![]))]));
        let Command::StmtExecute(e) = Command::parse_with(&b, caps, MY, &stmts).expect("parse") else {
            panic!("not an execute")
        };
        let got = e.params.expect("params");
        if params.is_empty() {
            prop_assert!(got.params.is_empty());
        } else {
            prop_assert_eq!(got.params, params);
        }
    }
}

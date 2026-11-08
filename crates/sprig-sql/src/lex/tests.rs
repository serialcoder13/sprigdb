use super::*;

fn kinds(src: &str, opts: &LexOptions) -> Vec<(TokenKind, String)> {
    lex(src.as_bytes(), opts)
        .unwrap()
        .into_iter()
        .filter(|t| t.kind != TokenKind::Whitespace)
        .map(|t| {
            (
                t.kind,
                String::from_utf8_lossy(t.bytes(src.as_bytes())).into_owned(),
            )
        })
        .collect()
}

fn k(src: &str) -> Vec<(TokenKind, String)> {
    kinds(src, &LexOptions::mysql84())
}

use TokenKind as T;

fn tok(kind: TokenKind, s: &str) -> (TokenKind, String) {
    (kind, s.to_owned())
}

#[test]
fn tokens_cover_every_byte_in_order() {
    let src = "SELECT /*+ H */ `a``b`.c, 'x''y' -- c\n# d\n FROM t /*!50001 x */;";
    let toks = lex(src.as_bytes(), &LexOptions::mysql84()).unwrap();
    let mut at = 0;
    for t in &toks {
        assert_eq!(t.start as usize, at);
        assert!(t.end > t.start);
        at = t.end as usize;
    }
    assert_eq!(at, src.len());
}

#[test]
fn quotes_follow_sql_mode() {
    assert_eq!(k("`a``b`"), [tok(T::QuotedIdent, "`a``b`")]);
    assert_eq!(k("\"a\"\"b\""), [tok(T::String, "\"a\"\"b\"")]);
    let ansi = LexOptions::mysql84().ansi_quotes(true);
    assert_eq!(
        kinds("\"a\"\"b\"", &ansi),
        [tok(T::QuotedIdent, "\"a\"\"b\"")]
    );
    assert_eq!(k("'it\\'s'"), [tok(T::String, "'it\\'s'")]);
    let nbe = LexOptions::mysql84().no_backslash_escapes(true);
    assert_eq!(
        kinds("'a\\' x", &nbe),
        [tok(T::String, "'a\\'"), tok(T::Word, "x")]
    );
    assert_eq!(k("'abc"), [tok(T::Unterminated, "'abc")]);
    assert_eq!(k("`ab"), [tok(T::Unterminated, "`ab")]);
}

#[test]
fn comments() {
    assert_eq!(
        k("-- x\n1"),
        [tok(T::LineComment, "-- x\n"), tok(T::Number, "1")]
    );
    assert_eq!(k("--\tx"), [tok(T::LineComment, "--\tx")]);
    assert_eq!(k("--"), [tok(T::LineComment, "--")]);
    assert_eq!(
        k("1--1"),
        [
            tok(T::Number, "1"),
            tok(T::Op, "-"),
            tok(T::Op, "-"),
            tok(T::Number, "1")
        ]
    );
    assert_eq!(k("#x\ny"), [tok(T::LineComment, "#x\n"), tok(T::Word, "y")]);
    assert_eq!(k("/* a */"), [tok(T::BlockComment, "/* a */")]);
    assert_eq!(k("/*+ A(b) */"), [tok(T::Hint, "/*+ A(b) */")]);
    assert_eq!(k("/* a"), [tok(T::Unterminated, "/* a")]);
}

#[test]
fn executable_comments_per_persona() {
    let my = LexOptions::mysql84();
    let ma = LexOptions::mariadb();
    assert_eq!(
        kinds("/*!50001 a */", &my),
        [
            tok(T::ExecOpen, "/*!50001"),
            tok(T::Word, "a"),
            tok(T::ExecClose, "*/")
        ]
    );
    assert_eq!(
        kinds("/*!90000 a */", &my),
        [tok(T::InactiveExec, "/*!90000 a */")]
    );
    assert_eq!(
        kinds("/*M!100100 a */", &my),
        [tok(T::InactiveExec, "/*M!100100 a */")]
    );
    assert_eq!(
        kinds("/*M!100100 a */", &ma),
        [
            tok(T::ExecOpen, "/*M!100100"),
            tok(T::Word, "a"),
            tok(T::ExecClose, "*/")
        ]
    );
    assert_eq!(
        kinds("/*!80016 a */", &ma),
        [tok(T::InactiveExec, "/*!80016 a */")]
    );
    // A string holding */ inside a running comment is a string.
    assert_eq!(
        kinds("/*! '*/' */", &my),
        [
            tok(T::ExecOpen, "/*!"),
            tok(T::String, "'*/'"),
            tok(T::ExecClose, "*/")
        ]
    );
    // A skipped comment ends at the first */, strings or not.
    assert_eq!(
        kinds("/*!99999 '*/' x", &my),
        [
            tok(T::InactiveExec, "/*!99999 '*/"),
            tok(T::Unterminated, "' x")
        ]
    );
    // */ outside an executable comment is two operators.
    assert_eq!(k("*/"), [tok(T::Op, "*"), tok(T::Op, "/")]);
    // The version gate follows LexOptions::version.
    assert_eq!(
        kinds("/*!80500 a */", &my.version(80500)),
        [
            tok(T::ExecOpen, "/*!80500"),
            tok(T::Word, "a"),
            tok(T::ExecClose, "*/")
        ]
    );
}

#[test]
fn literals() {
    assert_eq!(
        k("0x1F X'0A' x'0a' b'01' B'1' 0b11 N'n' n'n'"),
        [
            tok(T::HexNumber, "0x1F"),
            tok(T::HexString, "X'0A'"),
            tok(T::HexString, "x'0a'"),
            tok(T::BitString, "b'01'"),
            tok(T::BitString, "B'1'"),
            tok(T::BitNumber, "0b11"),
            tok(T::NationalString, "N'n'"),
            tok(T::NationalString, "n'n'"),
        ]
    );
    assert_eq!(
        k("1 1.5 .5 1e5 1e+5 1.5e-3 1."),
        [
            tok(T::Number, "1"),
            tok(T::Number, "1.5"),
            tok(T::Number, ".5"),
            tok(T::Number, "1e5"),
            tok(T::Number, "1e+5"),
            tok(T::Number, "1.5e-3"),
            tok(T::Number, "1."),
        ]
    );
    assert_eq!(
        k("1abc 0xZZ 1e"),
        [
            tok(T::Word, "1abc"),
            tok(T::Word, "0xZZ"),
            tok(T::Word, "1e")
        ]
    );
    assert_eq!(
        k("_utf8mb4'x'"),
        [tok(T::Word, "_utf8mb4"), tok(T::String, "'x'")]
    );
}

#[test]
fn identifiers_and_qualifiers() {
    assert_eq!(
        k("t.1c t .5"),
        [
            tok(T::Word, "t"),
            tok(T::Punct, "."),
            tok(T::Word, "1c"),
            tok(T::Word, "t"),
            tok(T::Number, ".5"),
        ]
    );
    assert_eq!(k("Ünï$_x"), [tok(T::Word, "Ünï$_x")]);
    assert_eq!(
        k("@a.b @'q' @`q` @\"q\" @@global.x @@`y` ? :="),
        [
            tok(T::UserVar, "@a.b"),
            tok(T::UserVar, "@'q'"),
            tok(T::UserVar, "@`q`"),
            tok(T::UserVar, "@\"q\""),
            tok(T::SysVar, "@@global.x"),
            tok(T::SysVar, "@@`y`"),
            tok(T::Param, "?"),
            tok(T::Op, ":="),
        ]
    );
    assert_eq!(
        k("->> <=> \x01"),
        [tok(T::Op, "->>"), tok(T::Op, "<=>"), tok(T::Other, "\x01")]
    );
}

#[test]
fn values() {
    assert_eq!(&*ident_value(b"`a``b`"), b"a`b");
    assert_eq!(&*ident_value(b"\"a\"\"b\""), b"a\"b");
    assert_eq!(&*ident_value(b"plain"), b"plain");
    assert_eq!(string_value(b"'it''s'", false).unwrap(), b"it's");
    assert_eq!(string_value(b"'a\\nb\\%'", false).unwrap(), b"a\nb\\%");
    assert_eq!(string_value(b"'a\\nb'", true).unwrap(), b"a\\nb");
    assert!(string_value(b"'open", false).is_none());
    assert!(charset_is_lexer_safe("utf8mb4"));
    assert!(charset_is_lexer_safe("latin1"));
    assert!(!charset_is_lexer_safe("GBK"));
}

#[test]
fn non_utf8_input_lexes() {
    let src = b"SELECT '\xff\xfe' FROM t\x80x";
    let toks = lex(src, &LexOptions::mariadb()).unwrap();
    assert_eq!(toks.last().unwrap().end as usize, src.len());
}

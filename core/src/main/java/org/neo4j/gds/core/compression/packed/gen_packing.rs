#!/usr/bin/env rust-script
#![allow(clippy::vec_init_then_push)]
//! ```cargo
//! [dependencies]
//! clap="3"
//! gen_java = { git = "https://github.com/knutwalker/gen_java" }
//! ```

// java sizes
const LONG: u32 = std::mem::size_of::<u64>() as _;
const LONG_BITS: u32 = u64::BITS;
const BYTE_BITS: u32 = u8::BITS;

const fn number_of_x_per_bits(block_size: u32, bits: u32, x: u32) -> u32 {
    (block_size * bits + x - 1) / x
}

const fn number_of_words_for_bits(block_size: u32, bits: u32) -> u32 {
    number_of_x_per_bits(block_size, bits, LONG_BITS)
}

const fn number_of_bytes_for_bits(block_size: u32, bits: u32) -> u32 {
    number_of_x_per_bits(block_size, bits, BYTE_BITS)
}

const fn plural(n: u32) -> &'static str {
    if n == 1 {
        ""
    } else {
        "s"
    }
}

fn main() {
    if let Err(e) = try_main() {
        eprintln!("{}", e);
        std::process::exit(1);
    }
}

fn try_main() -> Result<(), Box<dyn std::error::Error>> {
    fn parse_block_size(s: &str) -> Result<u32, String> {
        let bs = s.parse().map_err(|e| format!("must be a number: {e}"))?;

        if !(1..=LONG_BITS).contains(&bs) {
            return Err(format!("must be between 1 and {}", LONG_BITS));
        }

        if !bs.is_power_of_two() {
            return Err("must be a power of two".into());
        }

        Ok(bs)
    }

    #[derive(Copy, Clone)]
    #[repr(u8)]
    enum Includes {
        Pack = 1,
        Unpack = 2,
        PackLoop = 4,
        UnpackLoop = 8,
        Packers = 1 | 4,
        Unpackers = 2 | 8,
        Loops = 4 | 8,
    }

    impl clap::ValueEnum for Includes {
        fn value_variants<'a>() -> &'a [Self] {
            &[
                Self::Pack,
                Self::Unpack,
                Self::PackLoop,
                Self::UnpackLoop,
                Self::Packers,
                Self::Unpackers,
                Self::Loops,
            ]
        }

        fn to_possible_value<'a>(&self) -> Option<clap::PossibleValue<'a>> {
            match self {
                Self::Pack => Some("pack".into()),
                Self::Unpack => Some("unpack".into()),
                Self::PackLoop => Some("pack-loop".into()),
                Self::UnpackLoop => Some("unpack-loop".into()),
                Self::Packers => Some("packers".into()),
                Self::Unpackers => Some("unpackers".into()),
                Self::Loops => Some("loops".into()),
            }
        }
    }

    let mut matches = clap::Command::new(file!())
        .arg(
            clap::Arg::new("block-size")
                .short('b')
                .long("block-size")
                .value_parser(parse_block_size)
                .default_value("64")
                .help("How many input values will be packed as a single block."),
        )
        .arg(
            clap::Arg::new("class-name")
                .short('c')
                .long("class-name")
                .value_parser(clap::builder::NonEmptyStringValueParser::new())
                .default_value("AdjacencyPacking")
                .help("Name of the generated class."),
        )
        .arg(
            clap::Arg::new("package")
                .short('p')
                .long("package")
                .value_parser(clap::builder::NonEmptyStringValueParser::new())
                .default_value("org.neo4j.gds.core.loading")
                .help("Package of the generated class."),
        )
        .arg(
            clap::Arg::new("include")
                .short('i')
                .long("include")
                .action(clap::ArgAction::Append)
                .value_parser(clap::builder::EnumValueParser::<Includes>::new())
                .help(
                    "Which parts to include in the generated class. The default is to include all.",
                ),
        )
        .arg(clap::Arg::new("exclude")
            .short('e')
            .long("exclude")
            .action(clap::ArgAction::Append)
            .value_parser(clap::builder::EnumValueParser::<Includes>::new())
            .help(
                "Which parts to exclude from the generated class. The default is to exclude none.",
            ),
        )
        .arg(
            clap::Arg::new("output")
                .short('o')
                .long("output")
                .action(clap::ArgAction::SetTrue)
                .help("Write to a file with the named based on the class name instead of to stdout."),
        )
        .arg(
            clap::Arg::new("force")
                .short('f')
                .long("force")
                .action(clap::ArgAction::SetTrue)
                .help("If the target file exists, overwrite it."),
        )
        .get_matches();

    let block_size = matches
        .remove_one("block-size")
        .expect("has a default value");

    let class_name = matches
        .remove_one::<String>("class-name")
        .expect("has a default value");

    let package = matches.remove_one("package").expect("has a default value");

    let includes = matches
        .get_many::<Includes>("exclude")
        .unwrap_or_default()
        .map(|i| *i as u8)
        .fold(u8::MAX, |a, b| a & !b);

    let includes = matches
        .get_many::<Includes>("include")
        .map(|is| is.map(|i| *i as u8).fold(0_u8, |a, b| a | b))
        .unwrap_or(includes);

    let packers = if includes & (Includes::Pack as u8) != 0 {
        (0..=block_size).map(|i| pack(block_size, i)).collect()
    } else {
        Vec::new()
    };

    let unpackers = if includes & (Includes::Unpack as u8) != 0 {
        (0..=block_size).map(|i| unpack(block_size, i)).collect()
    } else {
        Vec::new()
    };

    let loop_packers = if includes & (Includes::PackLoop as u8) != 0 {
        (0..=block_size).map(pack_loop).collect()
    } else {
        Vec::new()
    };

    let loop_unpackers = if includes & (Includes::UnpackLoop as u8) != 0 {
        (0..=block_size).map(unpack_loop).collect()
    } else {
        Vec::new()
    };

    let class = Class {
        documentation: vec![
            format!("This class is generated by {}", file!()),
            "Do not edit this file directly.".into(),
        ],
        name: class_name.clone(),
        block_size,
        packers,
        loop_packers,
        loop_unpackers,
        unpackers,
    };

    let file = File { package, class };

    if matches
        .get_one("output")
        .copied()
        .expect("Action::SetTrue has a default value")
    {
        let mut open = std::fs::OpenOptions::new();
        open.write(true);
        if matches
            .get_one("force")
            .copied()
            .expect("Action::SetTrue has a default value")
        {
            open.create(true).truncate(true);
        } else {
            open.create_new(true);
        }

        let output = format!("{}.java", class_name);
        let output = open.open(output)?;
        let mut output = std::io::BufWriter::new(output);

        java::write_file(file, &mut output)?
    } else {
        let mut out = std::io::stdout();
        java::write_file(file, &mut out)?
    }

    Ok(())
}

#[derive(Copy, Clone)]
struct Pack {
    word: u32,
    offset: u32,
    shift: u32,
}

enum Type {
    Int,
    Long,
}

enum Inst {
    DeclareVarAndInitZero {
        name: &'static str,
        typ: Type,
    },
    DeclareNumberOfWords,
    DeclareWord {
        word: u32,
    },
    DeclareWordAndInit {
        word: u32,
        offset: u32,
    },
    DefineMask {
        constant: u64,
    },
    Pack {
        pack: Pack,
    },
    PackLoop,
    PackLoopRemainder,
    UnpackLoop,
    PackSplit {
        lower: Pack,
        upper_word: u32,
        upper_shift: u32,
    },
    Unpack {
        pack: Pack,
    },
    UnpackSplit {
        lower: Pack,
        upper_word: u32,
        upper_shift: u32,
    },
    Memset {
        size: u32,
        constant: u64,
    },
    DynamicMemset {
        constant: u64,
    },
    Read {
        name: &'static str,
    },
    Write {
        word: u32,
        offset: u32,
    },
    Return {
        offset: u32,
    },
    ReturnPtr,
}

struct CodeBlock {
    comment: Option<String>,
    code: Vec<Inst>,
}

struct Method {
    documentation: Vec<String>,
    prefix: &'static str,
    bits: u32,
    code: Vec<CodeBlock>,
    is_loop: bool,
}

struct Class {
    documentation: Vec<String>,
    name: String,
    block_size: u32,
    packers: Vec<Method>,
    loop_packers: Vec<Method>,
    loop_unpackers: Vec<Method>,
    unpackers: Vec<Method>,
}

struct File {
    package: String,
    class: Class,
}

fn pack(block_size: u32, bits: u32) -> Method {
    let words = number_of_words_for_bits(block_size, bits);
    let bytes = number_of_bytes_for_bits(block_size, bits);

    let mut code = Vec::new();

    code.push(CodeBlock {
        comment: Some(format!("Touching {words} word{}", plural(words))),
        code: (0..words).map(|i| Inst::DeclareWord { word: i }).collect(),
    });

    if bits != 0 {
        code.push(CodeBlock {
            comment: None,
            code: (0..block_size).map(|i| single_pack(bits, i)).collect(),
        });
    }

    code.push(CodeBlock {
        comment: Some(format!("Write to {} byte{}", bytes, plural(bytes))),
        code: (0..words)
            .map(|word| Inst::Write {
                word,
                offset: word * LONG,
            })
            .collect(),
    });

    code.push(CodeBlock {
        comment: None,
        code: vec![Inst::Return { offset: bytes }],
    });

    Method {
        documentation: vec![format!(
            "Packs {block_size} {bits}-bit value{} into {bytes} byte{}, touching {words} word{}.",
            plural(block_size),
            plural(bytes),
            plural(words),
        )],
        prefix: "pack",
        bits,
        code,
        is_loop: false,
    }
}

fn pack_loop(bits: u32) -> Method {
    let mut code = Vec::new();

    if bits != 0 {
        code.push(CodeBlock {
            comment: None,
            code: vec![
                Inst::DeclareVarAndInitZero {
                    name: "word",
                    typ: Type::Long,
                },
                Inst::DeclareVarAndInitZero {
                    name: "shift",
                    typ: Type::Int,
                },
            ],
        });

        code.push(CodeBlock {
            comment: None,
            code: vec![Inst::PackLoop, Inst::PackLoopRemainder],
        });
    }

    code.push(CodeBlock {
        comment: None,
        code: vec![Inst::ReturnPtr],
    });

    Method {
        documentation: vec![format!("Packs `valuesLength` {bits}-bit values.")],
        prefix: "packLoop",
        bits,
        code,
        is_loop: true,
    }
}

fn unpack(block_size: u32, bits: u32) -> Method {
    let words = number_of_words_for_bits(block_size, bits);
    let bytes = number_of_bytes_for_bits(block_size, bits);

    let mut code = Vec::new();

    code.push(CodeBlock {
        comment: Some(format!("Access {words} word{}", plural(words))),
        code: (0..words)
            .map(|word| Inst::DeclareWordAndInit {
                word,
                offset: word * LONG,
            })
            .collect(),
    });

    if bits == 0 {
        code.push(CodeBlock {
            comment: None,
            code: vec![Inst::Memset {
                size: block_size,
                constant: 0,
            }],
        });
    } else {
        code.push(CodeBlock {
            comment: None,
            code: (0..block_size)
                .map(|i| {
                    let pack = single_pack(bits, i);
                    match pack {
                        Inst::Pack { pack } => Inst::Unpack { pack },
                        Inst::PackSplit {
                            lower,
                            upper_word,
                            upper_shift,
                        } => Inst::UnpackSplit {
                            lower,
                            upper_word,
                            upper_shift,
                        },
                        _ => unreachable!(),
                    }
                })
                .collect(),
        });

        if bits != block_size {
            let mask = (1_u64 << bits) - 1;

            code.last_mut()
                .unwrap()
                .code
                .insert(0, Inst::DefineMask { constant: mask });
        }
    }

    code.push(CodeBlock {
        comment: None,
        code: vec![Inst::Return { offset: bytes }],
    });

    Method {
        documentation: vec![format!(
            "Unpacks {block_size} {bits}-bit value{} using {bytes} byte{}, touching {words} word{}.",
            plural(block_size),
            plural(bytes),
            plural(words),
        )],
        prefix: "unpack",
        bits,
        code,
        is_loop: false
    }
}

fn unpack_loop(bits: u32) -> Method {
    let mut code = Vec::new();

    if bits == 0 {
        code.push(CodeBlock {
            comment: None,
            code: vec![Inst::DynamicMemset { constant: 0 }],
        });
    } else {
        code.push(CodeBlock {
            comment: None,
            code: vec![
                Inst::DeclareNumberOfWords,
                Inst::DeclareVarAndInitZero {
                    name: "shift",
                    typ: Type::Int,
                },
                Inst::DeclareVarAndInitZero {
                    name: "offset",
                    typ: Type::Int,
                },
            ],
        });
        code.push(CodeBlock {
            comment: None,
            code: vec![Inst::Read { name: "word" }],
        });
        code.push(CodeBlock {
            comment: None,
            code: vec![Inst::UnpackLoop],
        });
    }
    code.push(CodeBlock {
        comment: None,
        code: vec![Inst::ReturnPtr],
    });

    Method {
        documentation: vec![format!("Unpacks `valuesLength` {bits}-bit values.")],
        prefix: "unpackLoop",
        bits,
        code,
        is_loop: true,
    }
}

fn single_pack(bits: u32, offset: u32) -> Inst {
    // how many bits we need to shift the current value to get to its position
    let shift = (offset * bits) % LONG_BITS;
    // the word for the lower bits of the current value
    let lower_word = offset * bits / LONG_BITS;
    // the word for the upper bits of the current value
    let upper_word = (offset * bits + bits - 1) / LONG_BITS;

    let pack = Pack {
        word: lower_word,
        offset,
        shift,
    };

    if lower_word == upper_word {
        // value fits within one word
        Inst::Pack { pack }
    } else {
        // need to split the value across multiple words
        Inst::PackSplit {
            lower: pack,
            upper_word,
            upper_shift: LONG_BITS - shift,
        }
    }
}

mod java {
    use std::{collections::HashSet, io::Write};

    use super::*;
    use gen_java::*;

    const PIN: &str = "values";
    const OFF: &str = "valuesStart";
    const LEN: &str = "valuesLength";
    const PW: &str = "packedPtr";
    const BITS: &str = "bits";
    const WORD: &str = "word";
    const SHIFT: &str = "shift";
    const LOOP_I: &str = "i";

    const BS: &str = "BLOCK_SIZE";
    const PACKERS: &str = "PACKERS";
    const UNPACKERS: &str = "UNPACKERS";
    const LOOP_PACKERS: &str = "LOOP_PACKERS";
    const LOOP_UNPACKERS: &str = "LOOP_UNPACKERS";

    const PIN_PARAM: Param = Param {
        typ: "long[]",
        ident: PIN,
    };
    const OFF_PARAM: Param = Param {
        typ: "int",
        ident: OFF,
    };
    const LEN_PARAM: Param = Param {
        typ: "int",
        ident: LEN,
    };
    const PW_PARAM: Param = Param {
        typ: "long",
        ident: PW,
    };
    const BITS_PARAM: Param = Param {
        typ: "int",
        ident: BITS,
    };

    const PARAMS: [Param; 3] = [PIN_PARAM, OFF_PARAM, PW_PARAM];
    const FULL_PARAMS: [Param; 4] = [BITS_PARAM, PIN_PARAM, OFF_PARAM, PW_PARAM];
    const LOOP_PARAMS: [Param; 4] = [PIN_PARAM, OFF_PARAM, LEN_PARAM, PW_PARAM];
    const FULL_LOOP_PARAMS: [Param; 5] = [BITS_PARAM, PIN_PARAM, OFF_PARAM, LEN_PARAM, PW_PARAM];

    fn gen_method(method: Method) -> MethodDef {
        fn value(offset: u32) -> Expr {
            Expr::bin(
                Expr::Ident(PIN),
                BinOp::Index,
                Expr::bin(Expr::Literal(offset), BinOp::Add, Expr::Ident(OFF)),
            )
        }

        let ident = |word: u32| -> String { format!("w{word}") };
        let mut mask = u64::MAX;
        let mut statements = Vec::new();

        for code in method.code {
            if code.code.is_empty() {
                continue;
            }

            if let Some(comment) = code.comment {
                statements.push(Stmt::Comment(comment));
            }

            for inst in code.code {
                match inst {
                    Inst::DeclareVarAndInitZero { name, typ } => statements.push(Stmt::Def(Def {
                        typ: match typ {
                            Type::Int => "int",
                            Type::Long => "long",
                        },
                        ident: name.to_string(),
                        value: Some(Expr::Literal(0)),
                    })),
                    Inst::DeclareNumberOfWords => statements.push(Stmt::Def(Def {
                        typ: "int",
                        ident: "words".to_string(),
                        value: Some(Expr::Call(Call::new(
                            Expr::Ident("BitUtil"),
                            "ceilDiv",
                            vec![
                                Arg::new(Expr::bin(
                                    Expr::Ident(LEN),
                                    BinOp::Mul,
                                    Expr::Literal(method.bits),
                                )),
                                Arg::new(Expr::Literal(LONG_BITS)),
                            ],
                        ))),
                    })),
                    Inst::DeclareWord { word } => {
                        statements.push(Stmt::Def(Def {
                            typ: "long",
                            ident: ident(word),
                            value: None,
                        }));
                    }
                    Inst::DeclareWordAndInit { word, offset } => {
                        statements.push(Stmt::Def(Def {
                            typ: "long",
                            ident: ident(word),
                            value: Some(Expr::Call(Call::new(
                                Expr::Ident("UnsafeUtil"),
                                "getLong",
                                vec![Arg::new(Expr::bin(
                                    Expr::Literal(offset),
                                    BinOp::Add,
                                    Expr::Ident(PW),
                                ))],
                            ))),
                        }));
                    }
                    Inst::DefineMask { constant } => {
                        mask = constant;
                    }
                    Inst::Pack {
                        pack:
                            Pack {
                                word,
                                offset,
                                shift,
                            },
                    } => {
                        let value = Expr::bin(value(offset), BinOp::Shl, Expr::Literal(shift));
                        let op = Some(BinOp::Or).filter(|_| shift != 0);
                        statements.push(Stmt::assign_op(Expr::Var(ident(word)), value, op));
                    }
                    Inst::PackLoop => {
                        let mut for_body = vec![];
                        for_body.push(Stmt::assign_op(
                            Expr::Var(WORD.to_owned()),
                            Expr::bin(
                                Expr::bin(Expr::Ident(PIN), BinOp::Index, Expr::Ident(LOOP_I)),
                                BinOp::Shl,
                                Expr::Ident(SHIFT),
                            ),
                            BinOp::Or,
                        ));

                        let mut then = vec![];
                        then.push(Stmt::Expr(Expr::Call(Call::new(
                            Expr::Ident("UnsafeUtil"),
                            "putLong",
                            vec![Arg::new(Expr::Ident(PW)), Arg::new(Expr::Ident(WORD))],
                        ))));
                        then.push(Stmt::assign_op(
                            Expr::Ident(PW),
                            Expr::Literal(8),
                            BinOp::Add,
                        ));
                        then.push(Stmt::assign(
                            Expr::Ident(WORD),
                            Expr::bin(
                                Expr::bin(Expr::Ident(PIN), BinOp::Index, Expr::Ident(LOOP_I)),
                                BinOp::Shr,
                                Expr::bin(Expr::Literal(LONG_BITS), BinOp::Sub, Expr::Ident(SHIFT)),
                            ),
                        ));
                        then.push(Stmt::assign_op(
                            Expr::Ident(SHIFT),
                            Expr::Literal(LONG_BITS),
                            BinOp::Sub,
                        ));

                        let mut elif = Vec::new();
                        elif.push(Stmt::Expr(Expr::Call(Call::new(
                            Expr::Ident("UnsafeUtil"),
                            "putLong",
                            vec![Arg::new(Expr::Ident(PW)), Arg::new(Expr::Ident(WORD))],
                        ))));
                        elif.push(Stmt::assign_op(
                            Expr::Ident(PW),
                            Expr::Literal(8),
                            BinOp::Add,
                        ));
                        elif.push(Stmt::assign(Expr::Ident(WORD), Expr::Literal(0)));
                        elif.push(Stmt::assign_op(
                            Expr::Ident(SHIFT),
                            Expr::Literal(LONG_BITS),
                            BinOp::Sub,
                        ));

                        let elif = Stmt::If {
                            cond: Expr::bin(
                                Expr::Ident(SHIFT),
                                BinOp::Eq,
                                Expr::bin(
                                    Expr::Literal(LONG_BITS),
                                    BinOp::Sub,
                                    Expr::Literal(method.bits),
                                ),
                            ),
                            then: Box::new(Stmt::Block(elif)),
                            ells: None,
                        };

                        for_body.push(Stmt::If {
                            cond: Expr::bin(
                                Expr::Ident(SHIFT),
                                BinOp::Gt,
                                Expr::bin(
                                    Expr::Literal(LONG_BITS),
                                    BinOp::Sub,
                                    Expr::Literal(method.bits),
                                ),
                            ),
                            then: Box::new(Stmt::Block(then)),
                            ells: Some(Box::new(elif)),
                        });

                        statements.push(Stmt::For {
                            init: Box::new(Stmt::Def(Def::assign("int", LOOP_I, Expr::Ident(OFF)))),
                            cond: Box::new(Expr::bin(
                                Expr::Ident(LOOP_I),
                                BinOp::Lt,
                                Expr::bin(Expr::Ident(OFF), BinOp::Add, Expr::Ident(LEN)),
                            )),
                            incr: Box::new(Stmt::Block(vec![
                                Stmt::assign_op(Expr::Ident(LOOP_I), Expr::Literal(1), BinOp::Add),
                                Stmt::assign_op(
                                    Expr::Ident(SHIFT),
                                    Expr::Literal(method.bits),
                                    BinOp::Add,
                                ),
                            ])),
                            body: Box::new(Stmt::Block(for_body)),
                        });
                    }
                    Inst::PackLoopRemainder => {
                        let mut then = vec![];
                        then.push(Stmt::Expr(Expr::Call(Call::new(
                            Expr::Ident("UnsafeUtil"),
                            "putLong",
                            vec![Arg::new(Expr::Ident(PW)), Arg::new(Expr::Ident(WORD))],
                        ))));
                        then.push(Stmt::assign_op(
                            Expr::Ident(PW),
                            Expr::Literal(8),
                            BinOp::Add,
                        ));

                        if method.bits == 64 {
                            statements.extend(then);
                        } else {
                            statements.push(Stmt::If {
                                cond: Expr::bin(Expr::Ident(SHIFT), BinOp::Neq, Expr::Literal(0)),
                                then: Box::new(Stmt::Block(then)),
                                ells: None,
                            });
                        }
                    }
                    Inst::UnpackLoop => {
                        // PIN[offset + i + OFF]
                        fn index_access(offset: &Expr, i: u32) -> Expr {
                            Expr::bin(
                                Expr::Ident(PIN),
                                BinOp::Index,
                                Expr::bin(
                                    offset.clone(),
                                    BinOp::Add,
                                    Expr::bin(Expr::Literal(i), BinOp::Add, Expr::Ident(OFF)),
                                ),
                            )
                        }
                        // PIN[offset + 1 + OFF] = word >>> shift + (i * bits) & mask
                        fn read_value_shr(
                            offset: &Expr,
                            i: u32,
                            word: &Expr,
                            shift: &Expr,
                            bits: u32,
                            mask: &Expr,
                        ) -> Stmt {
                            Stmt::assign(
                                index_access(offset, i),
                                Expr::bin(
                                    Expr::bin(
                                        word.clone(),
                                        BinOp::Shr,
                                        Expr::bin(
                                            shift.clone(),
                                            BinOp::Add,
                                            Expr::Literal(i * bits),
                                        ),
                                    ),
                                    BinOp::And,
                                    mask.clone(),
                                ),
                            )
                        }
                        // PIN[offset + i + OFF] |= word << shift & mask
                        fn read_value_shl(
                            offset: &Expr,
                            i: u32,
                            word: &Expr,
                            shift: &Expr,
                            mask: &Expr,
                        ) -> Stmt {
                            Stmt::assign_op(
                                index_access(offset, i),
                                Expr::bin(
                                    Expr::bin(word.clone(), BinOp::Shl, shift.clone()),
                                    BinOp::And,
                                    mask.clone(),
                                ),
                                BinOp::Or,
                            )
                        }
                        // word = UnsafeUtil.getLong(PW);
                        // PW += 8;
                        fn read_word_assign(word: &Expr) -> Vec<Stmt> {
                            vec![
                                Stmt::assign(
                                    word.clone(),
                                    Expr::Call(Call::new(
                                        Expr::Ident("UnsafeUtil"),
                                        "getLong",
                                        vec![Arg::new(Expr::Ident(PW))],
                                    )),
                                ),
                                Stmt::assign_op(Expr::Ident(PW), Expr::Literal(8), BinOp::Add),
                            ]
                        }

                        let word = Expr::Ident(WORD);
                        let words = Expr::Ident("words");
                        let shift = Expr::Ident("shift");
                        let offset = Expr::Ident("offset");
                        let bits = Expr::Literal(method.bits);
                        let full_values_per_word = LONG_BITS.checked_div(method.bits).unwrap_or(0);
                        let mask = Expr::HexLiteral(match method.bits {
                            LONG_BITS => u64::MAX,
                            _ => (1_u64 << method.bits) - 1,
                        });
                        let shift_upper_bound = Expr::Literal(match full_values_per_word {
                            0 => 0,
                            _ => (full_values_per_word - 1) * method.bits,
                        });

                        let mut for_body = Vec::new();

                        for i in 0..full_values_per_word {
                            for_body.push(read_value_shr(
                                &offset,
                                i,
                                &word,
                                &shift,
                                method.bits,
                                &mask,
                            ));
                        }
                        for_body.push(Stmt::assign_op(
                            shift.clone(),
                            shift_upper_bound,
                            BinOp::Add,
                        ));
                        for_body.push(Stmt::If {
                            cond: Expr::bin(
                                Expr::Ident(LOOP_I),
                                BinOp::Eq,
                                Expr::bin(words.clone(), BinOp::Sub, Expr::Literal(1)),
                            ),
                            then: Box::new(Stmt::Break),
                            ells: None,
                        });

                        let mut then_body = Vec::new();
                        then_body.push(Stmt::assign(
                            shift.clone(),
                            Expr::bin(Expr::Literal(LONG_BITS), BinOp::Sub, shift.clone()),
                        ));
                        then_body.extend(read_word_assign(&word));
                        then_body.push(read_value_shl(
                            &offset,
                            full_values_per_word - 1,
                            &word,
                            &shift,
                            &mask,
                        ));
                        then_body.push(Stmt::assign(
                            shift.clone(),
                            Expr::bin(bits.clone(), BinOp::Sub, shift.clone()),
                        ));
                        then_body.push(Stmt::assign_op(
                            offset.clone(),
                            Expr::Literal(full_values_per_word),
                            BinOp::Add,
                        ));

                        let mut else_if_body = Vec::new();
                        else_if_body.extend(read_word_assign(&word));
                        else_if_body.push(Stmt::assign(shift.clone(), Expr::Literal(0)));
                        else_if_body.push(Stmt::assign_op(
                            offset.clone(),
                            Expr::Literal(full_values_per_word),
                            BinOp::Add,
                        ));

                        let mut else_body = Vec::new();
                        else_body.push(Stmt::assign_op(shift.clone(), bits.clone(), BinOp::Add));
                        else_body.push(read_value_shr(
                            &offset,
                            full_values_per_word,
                            &word,
                            &shift,
                            0,
                            &mask,
                        ));
                        else_body.push(Stmt::assign(
                            shift.clone(),
                            Expr::bin(Expr::Literal(LONG_BITS), BinOp::Sub, shift.clone()),
                        ));
                        else_body.extend(read_word_assign(&word));
                        else_body.push(read_value_shl(
                            &offset,
                            full_values_per_word,
                            &word,
                            &shift,
                            &mask,
                        ));
                        else_body.push(Stmt::assign(
                            shift.clone(),
                            Expr::bin(bits.clone(), BinOp::Sub, shift.clone()),
                        ));
                        else_body.push(Stmt::assign_op(
                            offset.clone(),
                            Expr::Literal(full_values_per_word + 1),
                            BinOp::Add,
                        ));

                        let ells = Stmt::If {
                            cond: Expr::bin(
                                shift.clone(),
                                BinOp::Eq,
                                Expr::bin(Expr::Literal(LONG_BITS), BinOp::Sub, bits.clone()),
                            ),
                            then: Box::new(Stmt::Block(else_if_body)),
                            ells: Some(Box::new(Stmt::Block(else_body))),
                        };

                        let if_stmt = Stmt::If {
                            cond: Expr::bin(
                                shift.clone(),
                                BinOp::Gt,
                                Expr::bin(Expr::Literal(LONG_BITS), BinOp::Sub, bits.clone()),
                            ),
                            then: Box::new(Stmt::Block(then_body)),
                            ells: Some(Box::new(ells)),
                        };

                        for_body.push(if_stmt);

                        statements.push(Stmt::For {
                            init: Box::new(Stmt::Def(Def::assign("int", LOOP_I, Expr::Literal(0)))),
                            cond: Box::new(Expr::bin(
                                Expr::Ident(LOOP_I),
                                BinOp::Lt,
                                words.clone(),
                            )),
                            incr: Box::new(Stmt::assign_op(
                                Expr::Ident(LOOP_I),
                                Expr::Literal(1),
                                BinOp::Add,
                            )),
                            body: Box::new(Stmt::Block(for_body)),
                        });
                    }
                    Inst::PackSplit {
                        lower:
                            Pack {
                                word: lower_word,
                                offset,
                                shift: lower_shift,
                            },
                        upper_word,
                        upper_shift,
                    } => {
                        statements.extend([
                            Stmt::or_assign(
                                Expr::Var(ident(lower_word)),
                                Expr::bin(value(offset), BinOp::Shl, Expr::Literal(lower_shift)),
                            ),
                            Stmt::assign(
                                Expr::Var(ident(upper_word)),
                                Expr::bin(value(offset), BinOp::Shr, Expr::Literal(upper_shift)),
                            ),
                        ]);
                    }
                    Inst::Unpack {
                        pack:
                            Pack {
                                word,
                                offset,
                                shift,
                            },
                    } => {
                        let shift_expr =
                            Expr::bin(Expr::Var(ident(word)), BinOp::Shr, Expr::Literal(shift));

                        let mask = if shift + method.bits == LONG_BITS {
                            shift_expr
                        } else {
                            Expr::bin(shift_expr, BinOp::And, Expr::HexLiteral(mask))
                        };

                        statements.push(Stmt::assign(value(offset), mask));
                    }
                    Inst::UnpackSplit {
                        lower:
                            Pack {
                                word: lower_word,
                                offset,
                                shift: lower_shift,
                            },
                        upper_word,
                        upper_shift,
                    } => {
                        statements.push(Stmt::assign(
                            value(offset),
                            Expr::bin(
                                Expr::bin(
                                    Expr::bin(
                                        Expr::Var(ident(lower_word)),
                                        BinOp::Shr,
                                        Expr::Literal(lower_shift),
                                    ),
                                    BinOp::Or,
                                    Expr::bin(
                                        Expr::Var(ident(upper_word)),
                                        BinOp::Shl,
                                        Expr::Literal(upper_shift),
                                    ),
                                ),
                                BinOp::And,
                                Expr::HexLiteral(mask),
                            ),
                        ));
                    }
                    Inst::Memset { size, constant } => {
                        statements.push(Stmt::Expr(Expr::Call(Call::new(
                            Expr::Ident("java.util.Arrays"),
                            "fill",
                            vec![
                                Arg::new(Expr::Ident(PIN)),
                                Arg::new(Expr::Ident(OFF)),
                                Arg::new(Expr::bin(
                                    Expr::Ident(OFF),
                                    BinOp::Add,
                                    Expr::Literal(size),
                                )),
                                Arg::new(Expr::HexLiteral(constant)),
                            ],
                        ))));
                    }
                    Inst::DynamicMemset { constant } => {
                        statements.push(Stmt::Expr(Expr::Call(Call::new(
                            Expr::Ident("java.util.Arrays"),
                            "fill",
                            vec![
                                Arg::new(Expr::Ident(PIN)),
                                Arg::new(Expr::Ident(OFF)),
                                Arg::new(Expr::bin(Expr::Ident(OFF), BinOp::Add, Expr::Ident(LEN))),
                                Arg::new(Expr::HexLiteral(constant)),
                            ],
                        ))));
                    }
                    Inst::Read { name } => {
                        statements.push(Stmt::Def(Def::assign(
                            "long",
                            name,
                            Expr::Call(Call::new(
                                Expr::Ident("UnsafeUtil"),
                                "getLong",
                                vec![Arg::new(Expr::Ident(PW))],
                            )),
                        )));
                        statements.push(Stmt::assign_op(
                            Expr::Ident(PW),
                            Expr::Literal(8),
                            BinOp::Add,
                        ));
                    }
                    Inst::Write { word, offset } => {
                        statements.push(Stmt::Expr(Expr::Call(Call::new(
                            Expr::Ident("UnsafeUtil"),
                            "putLong",
                            vec![
                                Arg::new(Expr::bin(
                                    Expr::Literal(offset),
                                    BinOp::Add,
                                    Expr::Ident(PW),
                                )),
                                Arg::new(Expr::Var(ident(word))),
                            ],
                        ))));
                    }
                    Inst::Return { offset } => {
                        statements.push(Stmt::Return {
                            value: Expr::bin(Expr::Literal(offset), BinOp::Add, Expr::Ident(PW)),
                        });
                    }
                    Inst::ReturnPtr => statements.push(Stmt::Return {
                        value: Expr::Ident(PW),
                    }),
                }
            }
        }

        let doc = method.documentation.join("\n");
        let ident = format!("{prefix}{bits}", prefix = method.prefix, bits = method.bits);

        MethodDef {
            documentation: Some(doc),
            modifiers: "private static",
            typ: "long",
            ident,
            params: if method.is_loop {
                &LOOP_PARAMS
            } else {
                &PARAMS
            },
            code: Some(statements),
        }
    }

    fn gen_class(class: Class) -> (ClassDef, Vec<String>) {
        fn gen_assert(bs: u32) -> Stmt {
            Stmt::Assert {
                assertion: Expr::bin(Expr::Ident(BITS), BinOp::Lte, Expr::Literal(bs)),
                message: Some(Expr::bin(
                    Expr::StringLit(format!("Bits must be at most {bs} but was ")),
                    BinOp::Add,
                    Expr::Ident(BITS),
                )),
            }
        }

        let mut imports = HashSet::new();

        let mut members = vec![
            Member::Method(MethodDef {
                documentation: None,
                modifiers: "private",
                typ: "",
                ident: class.name.clone(),
                params: &[],
                code: Some(vec![]),
            }),
            Member::Def(Def {
                typ: "public static final int",
                ident: BS.into(),
                value: Some(Expr::Literal(class.block_size)),
            }),
            Member::Method(MethodDef {
                documentation: None,
                modifiers: "public static",
                typ: "int",
                ident: "advanceValueOffset".into(),
                params: &[Param {
                    typ: "int",
                    ident: OFF,
                }],
                code: Some(vec![Stmt::Return {
                    value: Expr::bin(Expr::Ident(OFF), BinOp::Add, Expr::Ident(BS)),
                }]),
            }),
        ];

        if !class.packers.is_empty() {
            imports.insert("org.neo4j.internal.unsafe.UnsafeUtil");

            members.extend([
                Member::Method(MethodDef {
                    documentation: None,
                    modifiers: "public static",
                    typ: "long",
                    ident: "pack".into(),
                    params: &FULL_PARAMS,
                    code: Some(vec![
                        gen_assert(class.block_size),
                        Stmt::Return {
                            value: Expr::Call(Call::new(
                                Expr::bin(Expr::Ident(PACKERS), BinOp::Index, Expr::Ident(BITS)),
                                "pack",
                                vec![
                                    Arg::new(Expr::Ident(PIN)),
                                    Arg::new(Expr::Ident(OFF)),
                                    Arg::new(Expr::Ident(PW)),
                                ],
                            )),
                        },
                    ]),
                }),
                Member::Class(ClassDef {
                    documentation: None,
                    annotations: vec![Call::new(Expr::NoOp, "FunctionalInterface", Vec::new())],
                    modifiers: "private",
                    typ: "interface",
                    name: "Packer".into(),
                    members: vec![Member::Method(MethodDef {
                        documentation: None,
                        modifiers: "",
                        typ: "long",
                        ident: "pack".into(),
                        params: &PARAMS,
                        code: None,
                    })],
                }),
                Member::Def(Def {
                    typ: "private static final Packer[]",
                    ident: PACKERS.into(),
                    value: Some(Expr::ArrayInit(
                        class
                            .packers
                            .iter()
                            .map(|p| {
                                Expr::MethodRef(Call::new(
                                    Expr::Var(class.name.clone()),
                                    format!("{}{}", p.prefix, p.bits),
                                    Vec::new(),
                                ))
                            })
                            .collect(),
                    )),
                }),
            ]);
        }

        if !class.unpackers.is_empty() {
            imports.insert("org.neo4j.internal.unsafe.UnsafeUtil");

            members.extend([
                Member::Method(MethodDef {
                    documentation: None,
                    modifiers: "public static",
                    typ: "long",
                    ident: "unpack".into(),
                    params: &FULL_PARAMS,
                    code: Some(vec![
                        gen_assert(class.block_size),
                        Stmt::Return {
                            value: Expr::Call(Call::new(
                                Expr::bin(Expr::Ident(UNPACKERS), BinOp::Index, Expr::Ident(BITS)),
                                "unpack",
                                vec![
                                    Arg::new(Expr::Ident(PIN)),
                                    Arg::new(Expr::Ident(OFF)),
                                    Arg::new(Expr::Ident(PW)),
                                ],
                            )),
                        },
                    ]),
                }),
                Member::Class(ClassDef {
                    documentation: None,
                    annotations: vec![Call::new(Expr::NoOp, "FunctionalInterface", Vec::new())],
                    modifiers: "private",
                    typ: "interface",
                    name: "Unpacker".into(),
                    members: vec![Member::Method(MethodDef {
                        documentation: None,
                        modifiers: "",
                        typ: "long",
                        ident: "unpack".into(),
                        params: &PARAMS,
                        code: None,
                    })],
                }),
                Member::Def(Def {
                    typ: "private static final Unpacker[]",
                    ident: UNPACKERS.into(),
                    value: Some(Expr::ArrayInit(
                        class
                            .unpackers
                            .iter()
                            .map(|p| {
                                Expr::MethodRef(Call::new(
                                    Expr::Var(class.name.clone()),
                                    format!("{}{}", p.prefix, p.bits),
                                    Vec::new(),
                                ))
                            })
                            .collect(),
                    )),
                }),
            ]);
        }

        if !class.loop_packers.is_empty() {
            imports.insert("org.neo4j.internal.unsafe.UnsafeUtil");

            members.extend([
                Member::Method(MethodDef {
                    documentation: None,
                    modifiers: "public static",
                    typ: "long",
                    ident: "loopPack".into(),
                    params: &FULL_LOOP_PARAMS,
                    code: Some(vec![
                        gen_assert(class.block_size),
                        Stmt::Return {
                            value: Expr::Call(Call::new(
                                Expr::bin(
                                    Expr::Ident(LOOP_PACKERS),
                                    BinOp::Index,
                                    Expr::Ident(BITS),
                                ),
                                "loopPack",
                                vec![
                                    Arg::new(Expr::Ident(PIN)),
                                    Arg::new(Expr::Ident(OFF)),
                                    Arg::new(Expr::Ident(LEN)),
                                    Arg::new(Expr::Ident(PW)),
                                ],
                            )),
                        },
                    ]),
                }),
                Member::Class(ClassDef {
                    documentation: None,
                    annotations: vec![Call::new(Expr::NoOp, "FunctionalInterface", Vec::new())],
                    modifiers: "private",
                    typ: "interface",
                    name: "LoopPacker".into(),
                    members: vec![Member::Method(MethodDef {
                        documentation: None,
                        modifiers: "",
                        typ: "long",
                        ident: "loopPack".into(),
                        params: &LOOP_PARAMS,
                        code: None,
                    })],
                }),
                Member::Def(Def {
                    typ: "private static final LoopPacker[]",
                    ident: LOOP_PACKERS.into(),
                    value: Some(Expr::ArrayInit(
                        class
                            .loop_packers
                            .iter()
                            .map(|p| {
                                Expr::MethodRef(Call::new(
                                    Expr::Var(class.name.clone()),
                                    format!("{}{}", p.prefix, p.bits),
                                    Vec::new(),
                                ))
                            })
                            .collect(),
                    )),
                }),
            ]);
        }

        if !class.loop_unpackers.is_empty() {
            imports.insert("org.neo4j.internal.unsafe.UnsafeUtil");
            imports.insert("org.neo4j.gds.mem.BitUtil");

            members.extend([
                Member::Method(MethodDef {
                    documentation: None,
                    modifiers: "public static",
                    typ: "long",
                    ident: "loopUnpack".into(),
                    params: &FULL_LOOP_PARAMS,
                    code: Some(vec![
                        gen_assert(class.block_size),
                        Stmt::Return {
                            value: Expr::Call(Call::new(
                                Expr::bin(
                                    Expr::Ident(LOOP_UNPACKERS),
                                    BinOp::Index,
                                    Expr::Ident(BITS),
                                ),
                                "loopUnpack",
                                vec![
                                    Arg::new(Expr::Ident(PIN)),
                                    Arg::new(Expr::Ident(OFF)),
                                    Arg::new(Expr::Ident(LEN)),
                                    Arg::new(Expr::Ident(PW)),
                                ],
                            )),
                        },
                    ]),
                }),
                Member::Class(ClassDef {
                    documentation: None,
                    annotations: vec![Call::new(Expr::NoOp, "FunctionalInterface", Vec::new())],
                    modifiers: "private",
                    typ: "interface",
                    name: "LoopUnpacker".into(),
                    members: vec![Member::Method(MethodDef {
                        documentation: None,
                        modifiers: "",
                        typ: "long",
                        ident: "loopUnpack".into(),
                        params: &LOOP_PARAMS,
                        code: None,
                    })],
                }),
                Member::Def(Def {
                    typ: "private static final LoopUnpacker[]",
                    ident: LOOP_UNPACKERS.into(),
                    value: Some(Expr::ArrayInit(
                        class
                            .loop_unpackers
                            .iter()
                            .map(|p| {
                                Expr::MethodRef(Call::new(
                                    Expr::Var(class.name.clone()),
                                    format!("{}{}", p.prefix, p.bits),
                                    Vec::new(),
                                ))
                            })
                            .collect(),
                    )),
                }),
            ]);
        }

        members.extend(
            class
                .packers
                .into_iter()
                .chain(class.loop_packers)
                .chain(class.unpackers)
                .chain(class.loop_unpackers)
                .map(gen_method)
                .map(Member::Method),
        );

        let doc = class.documentation.join("\n");

        let class = ClassDef {
            documentation: Some(doc),
            annotations: Vec::new(),
            modifiers: "public final",
            typ: "class",
            name: class.name,
            members,
        };

        let mut imports = imports.into_iter().map(String::from).collect::<Vec<_>>();
        imports.sort_unstable();

        (class, imports)
    }

    fn gen_file(file: File) -> FileDef {
        let (class, imports) = gen_class(file.class);
        let mut file = FileDef::new(
            r#"
Copyright (c) "Neo4j"
Neo4j Sweden AB [http://neo4j.com]

This file is part of Neo4j.

Neo4j is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <http://www.gnu.org/licenses/>.
"#,
            file.package,
            imports,
            class,
        );

        file.optimize();
        file
    }

    pub(super) fn write_file(file: File, to: &mut impl Write) -> std::io::Result<()> {
        let file = gen_file(file);

        let mut writer = FileWriter::new(0);
        file.print(&mut writer);
        let content = writer.into_inner().into_bytes();
        to.write_all(&content)
    }
}

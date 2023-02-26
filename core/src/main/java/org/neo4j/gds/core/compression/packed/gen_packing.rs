#!/usr/bin/env rust-script
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
        DeltaPack = 4,
        DeltaUnpack = 8,
        Packers = 1 | 4,
        Unpackers = 2 | 8,
        Delta = 4 | 8,
    }

    impl clap::ValueEnum for Includes {
        fn value_variants<'a>() -> &'a [Self] {
            &[
                Self::Pack,
                Self::Unpack,
                Self::DeltaPack,
                Self::DeltaUnpack,
                Self::Packers,
                Self::Unpackers,
                Self::Delta,
            ]
        }

        fn to_possible_value<'a>(&self) -> Option<clap::PossibleValue<'a>> {
            match self {
                Self::Pack => Some("pack".into()),
                Self::Unpack => Some("unpack".into()),
                Self::DeltaPack => Some("delta-pack".into()),
                Self::DeltaUnpack => Some("delta-unpack".into()),
                Self::Packers => Some("packers".into()),
                Self::Unpackers => Some("unpackers".into()),
                Self::Delta => Some("delta".into()),
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

    let delta_packers = if includes & (Includes::DeltaPack as u8) != 0 {
        (0..=block_size)
            .map(|i| delta_pack(block_size, i))
            .collect()
    } else {
        Vec::new()
    };

    let delta_unpackers = if includes & (Includes::DeltaUnpack as u8) != 0 {
        (0..=block_size)
            .map(|i| delta_unpack(block_size, i))
            .collect()
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
        delta_packers,
        unpackers,
        delta_unpackers,
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

enum Inst {
    Declare {
        word: u32,
    },
    DeclareAndInit {
        word: u32,
        offset: u32,
    },
    DefineMask {
        constant: u64,
    },
    Pack {
        pack: Pack,
    },
    PackSplit {
        lower: Pack,
        upper_word: u32,
        upper_shift: u32,
    },
    PackDelta {
        pack: Pack,
    },
    PackSplitDelta {
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
    UnpackDelta {
        pack: Pack,
    },
    UnpackSplitDelta {
        lower: Pack,
        upper_word: u32,
        upper_shift: u32,
    },
    Memset {
        size: u32,
        constant: u64,
    },
    Write {
        word: u32,
        offset: u32,
    },
    Return {
        offset: u32,
    },
}

struct CodeBlock {
    comment: Option<String>,
    code: Vec<Inst>,
}

struct Method {
    documentation: Vec<String>,
    prefix: &'static str,
    bits: u32,
    delta: bool,
    code: Vec<CodeBlock>,
}

struct Class {
    documentation: Vec<String>,
    name: String,
    block_size: u32,
    packers: Vec<Method>,
    delta_packers: Vec<Method>,
    unpackers: Vec<Method>,
    delta_unpackers: Vec<Method>,
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
        code: (0..words).map(|i| Inst::Declare { word: i }).collect(),
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
        delta: false,
        code,
    }
}

fn delta_pack(block_size: u32, bits: u32) -> Method {
    let mut pack = pack(block_size, bits);

    pack.documentation[0].insert_str(0, "Delta-encodes and ");
    pack.prefix = "deltaPack";
    pack.delta = true;

    for code in &mut pack.code {
        for inst in &mut code.code {
            match inst {
                Inst::Pack { pack } => {
                    *inst = Inst::PackDelta { pack: *pack };
                }
                Inst::PackSplit {
                    lower,
                    upper_shift,
                    upper_word,
                } => {
                    *inst = Inst::PackSplitDelta {
                        lower: *lower,
                        upper_shift: *upper_shift,
                        upper_word: *upper_word,
                    };
                }
                _ => {}
            }
        }
    }

    pack
}

fn unpack(block_size: u32, bits: u32) -> Method {
    let words = number_of_words_for_bits(block_size, bits);
    let bytes = number_of_bytes_for_bits(block_size, bits);

    let mut code = Vec::new();

    code.push(CodeBlock {
        comment: Some(format!("Access {words} word{}", plural(words))),
        code: (0..words)
            .map(|word| Inst::DeclareAndInit {
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
        delta: false,
        code,
    }
}

fn delta_unpack(block_size: u32, bits: u32) -> Method {
    let mut unpack = unpack(block_size, bits);

    unpack.documentation[0].insert_str(0, "Delta-decodes and ");
    unpack.prefix = "deltaUnpack";
    unpack.delta = true;

    for code in &mut unpack.code {
        for inst in &mut code.code {
            match inst {
                Inst::Unpack { pack } => {
                    *inst = Inst::UnpackDelta { pack: *pack };
                }
                Inst::UnpackSplit {
                    lower,
                    upper_shift,
                    upper_word,
                } => {
                    *inst = Inst::UnpackSplitDelta {
                        lower: *lower,
                        upper_shift: *upper_shift,
                        upper_word: *upper_word,
                    };
                }
                _ => {}
            }
        }
    }

    unpack
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
    use std::io::Write;

    use super::*;
    use gen_java::*;

    const PIN: &str = "values";
    const OFF: &str = "valuesStart";
    const PW: &str = "packedPtr";
    const PREV: &str = "previousValue";
    const BITS: &str = "bits";

    const BS: &str = "BLOCK_SIZE";
    const PACKERS: &str = "PACKERS";
    const UNPACKERS: &str = "UNPACKERS";
    const DELTA_PACKERS: &str = "DELTA_PACKERS";
    const DELTA_UNPACKERS: &str = "DELTA_UNPACKERS";

    const PIN_PARAM: Param = Param {
        typ: "long[]",
        ident: PIN,
    };
    const OFF_PARAM: Param = Param {
        typ: "int",
        ident: OFF,
    };
    const PW_PARAM: Param = Param {
        typ: "long",
        ident: PW,
    };
    const PREV_PARAM: Param = Param {
        typ: "long",
        ident: PREV,
    };
    const BITS_PARAM: Param = Param {
        typ: "int",
        ident: BITS,
    };

    const PARAMS: [Param; 3] = [PIN_PARAM, OFF_PARAM, PW_PARAM];
    const FULL_PARAMS: [Param; 4] = [BITS_PARAM, PIN_PARAM, OFF_PARAM, PW_PARAM];
    const DELTA_PARAMS: [Param; 4] = [PREV_PARAM, PIN_PARAM, OFF_PARAM, PW_PARAM];
    const FULL_DELTA_PARAMS: [Param; 5] = [BITS_PARAM, PREV_PARAM, PIN_PARAM, OFF_PARAM, PW_PARAM];

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
                    Inst::Declare { word } => {
                        statements.push(Stmt::Def(Def {
                            typ: "long",
                            ident: ident(word),
                            value: None,
                        }));
                    }
                    Inst::DeclareAndInit { word, offset } => {
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
                    Inst::PackDelta {
                        pack:
                            Pack {
                                word,
                                offset,
                                shift,
                            },
                    } => {
                        let prev = offset
                            .checked_sub(1)
                            .map(value)
                            .unwrap_or_else(|| Expr::Ident(PREV));

                        let value = value(offset);
                        let value = Expr::bin(value, BinOp::Sub, prev);
                        let value = Expr::bin(value, BinOp::Shl, Expr::Literal(shift));

                        let op = Some(BinOp::Or).filter(|_| shift != 0);
                        statements.push(Stmt::assign_op(Expr::Var(ident(word)), value, op));
                    }
                    Inst::PackSplitDelta {
                        lower:
                            Pack {
                                word: lower_word,
                                offset,
                                shift: lower_shift,
                            },
                        upper_word,
                        upper_shift,
                    } => {
                        let prev = offset
                            .checked_sub(1)
                            .map(value)
                            .unwrap_or_else(|| Expr::Ident(PREV));

                        let value = value(offset);
                        let value = Expr::bin(value, BinOp::Sub, prev);

                        statements.extend([
                            Stmt::or_assign(
                                Expr::Var(ident(lower_word)),
                                Expr::bin(value.clone(), BinOp::Shl, Expr::Literal(lower_shift)),
                            ),
                            Stmt::assign(
                                Expr::Var(ident(upper_word)),
                                Expr::bin(value, BinOp::Shr, Expr::Literal(upper_shift)),
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
                    Inst::UnpackDelta {
                        pack:
                            Pack {
                                word,
                                offset,
                                shift,
                            },
                    } => {
                        let prev = offset
                            .checked_sub(1)
                            .map(value)
                            .unwrap_or_else(|| Expr::Ident(PREV));

                        let shift_expr =
                            Expr::bin(Expr::Var(ident(word)), BinOp::Shr, Expr::Literal(shift));

                        let mask = if shift + method.bits == LONG_BITS {
                            shift_expr
                        } else {
                            Expr::bin(shift_expr, BinOp::And, Expr::HexLiteral(mask))
                        };

                        let prefix_sum = Expr::bin(mask, BinOp::Add, prev);

                        statements.push(Stmt::assign(value(offset), prefix_sum));
                    }
                    Inst::UnpackSplitDelta {
                        lower:
                            Pack {
                                word: lower_word,
                                offset,
                                shift: lower_shift,
                            },
                        upper_word,
                        upper_shift,
                    } => {
                        let prev = offset
                            .checked_sub(1)
                            .map(value)
                            .unwrap_or_else(|| Expr::Ident(PREV));

                        statements.push(Stmt::assign(
                            value(offset),
                            Expr::bin(
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
                                BinOp::Add,
                                prev,
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
            params: if method.delta { &DELTA_PARAMS } else { &PARAMS },
            code: Some(statements),
        }
    }

    fn gen_class(class: Class) -> ClassDef {
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

        if !class.delta_packers.is_empty() {
            members.extend([
                Member::Method(MethodDef {
                    documentation: None,
                    modifiers: "public static",
                    typ: "long",
                    ident: "deltaPack".into(),
                    params: &FULL_DELTA_PARAMS,
                    code: Some(vec![
                        gen_assert(class.block_size),
                        Stmt::Return {
                            value: Expr::Call(Call::new(
                                Expr::bin(
                                    Expr::Ident(DELTA_PACKERS),
                                    BinOp::Index,
                                    Expr::Ident(BITS),
                                ),
                                "deltaPack",
                                vec![
                                    Arg::new(Expr::Ident(PREV)),
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
                    name: "DeltaPacker".into(),
                    members: vec![Member::Method(MethodDef {
                        documentation: None,
                        modifiers: "",
                        typ: "long",
                        ident: "deltaPack".into(),
                        params: &DELTA_PARAMS,
                        code: None,
                    })],
                }),
                Member::Def(Def {
                    typ: "private static final DeltaPacker[]",
                    ident: DELTA_PACKERS.into(),
                    value: Some(Expr::ArrayInit(
                        class
                            .delta_packers
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

        if !class.delta_unpackers.is_empty() {
            members.extend([
                Member::Method(MethodDef {
                    documentation: None,
                    modifiers: "public static",
                    typ: "long",
                    ident: "deltaUnpack".into(),
                    params: &FULL_DELTA_PARAMS,
                    code: Some(vec![
                        gen_assert(class.block_size),
                        Stmt::Return {
                            value: Expr::Call(Call::new(
                                Expr::bin(
                                    Expr::Ident(DELTA_UNPACKERS),
                                    BinOp::Index,
                                    Expr::Ident(BITS),
                                ),
                                "deltaUnpack",
                                vec![
                                    Arg::new(Expr::Ident(PREV)),
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
                    name: "DeltaUnpacker".into(),
                    members: vec![Member::Method(MethodDef {
                        documentation: None,
                        modifiers: "",
                        typ: "long",
                        ident: "deltaUnpack".into(),
                        params: &DELTA_PARAMS,
                        code: None,
                    })],
                }),
                Member::Def(Def {
                    typ: "private static final DeltaUnpacker[]",
                    ident: DELTA_UNPACKERS.into(),
                    value: Some(Expr::ArrayInit(
                        class
                            .delta_unpackers
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
                .chain(class.unpackers)
                .chain(class.delta_packers)
                .chain(class.delta_unpackers)
                .map(gen_method)
                .map(Member::Method),
        );

        let doc = class.documentation.join("\n");

        ClassDef {
            documentation: Some(doc),
            annotations: Vec::new(),
            modifiers: "public final",
            typ: "class",
            name: class.name,
            members,
        }
    }

    fn gen_file(file: File) -> FileDef {
        let class = gen_class(file.class);
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
            vec!["org.neo4j.internal.unsafe.UnsafeUtil".into()],
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

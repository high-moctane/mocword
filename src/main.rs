use clap::Parser;
use itertools::chain;
use regex::Regex;
use rusqlite::{self, params, Connection};
use std::env;
use std::io;

#[derive(Parser, Debug)]
#[clap(author, version, about)]
struct Args {
    #[clap(short, long, default_value_t = String::new())]
    query: String,

    #[clap(short, long, default_value_t = 10)]
    limit: i64,
}

fn build_like_query(query: &str) -> String {
    format!("{}%", query.replace("%", "\\%").replace("_", "\\_"))
}

fn search(
    conn: &Connection,
    query: &[&str; 5],
    limit: i64,
) -> Result<Vec<String>, rusqlite::Error> {
    let mut stmt = conn.prepare(
        "
        select distinct
            word
        from (
            select
                5 as n, five.id as id, one5.word as word
            from
                five_grams as five
            join one_grams as one5
                on five.suffix = one5.id
            join four_grams as four
                on four.id = five.prefix
            join one_grams as one4
                on four.suffix = one4.id
            join three_grams as three
                on three.id = four.prefix
            join one_grams as one3
                on three.suffix = one3.id
            join two_grams as two
                on two.id = three.prefix
            join one_grams as one2
                on two.suffix = one2.id
            join one_grams as one1
                on two.prefix = one1.id
            where
                one1.word = ?
                and
                one2.word = ?
                and
                one3.word = ?
                and
                one4.word = ?
                and
                one5.word like ?

            union all

            select
                4 as n, four.id as id, one4.word as word
            from
                four_grams as four
            join one_grams as one4
                on four.suffix = one4.id
            join three_grams as three
                on three.id = four.prefix
            join one_grams as one3
                on three.suffix = one3.id
            join two_grams as two
                on two.id = three.prefix
            join one_grams as one2
                on two.suffix = one2.id
            join one_grams as one1
                on two.prefix = one1.id
            where
                one1.word = ?
                and
                one2.word = ?
                and
                one3.word = ?
                and
                one4.word like ?

            union all

            select
                3 as n, three.id as id, one3.word as word
            from
                three_grams as three
            join one_grams as one3
                on three.suffix = one3.id
            join two_grams as two
                on two.id = three.prefix
            join one_grams as one2
                on two.suffix = one2.id
            join one_grams as one1
                on two.prefix = one1.id
            where
                one1.word = ?
                and
                one2.word = ?
                and
                one3.word like ?

            union all

            select
                2 as n, two.id as id, one2.word as word
            from
                two_grams as two
            join one_grams as one2
                on two.suffix = one2.id
            join one_grams as one1
                on two.prefix = one1.id
            where
                one1.word = ?
                and
                one2.word like ?

            union all

            select
                1 as n, id, word
            from
                one_grams
            where
                word like ?

            order by n desc, id
        )
        limit ?
    ",
    )?;

    let words_iter = stmt.query_map(
        params![
            query[0],
            query[1],
            query[2],
            query[3],
            build_like_query(query[4]),
            query[1],
            query[2],
            query[3],
            build_like_query(query[4]),
            query[2],
            query[3],
            build_like_query(query[4]),
            query[3],
            build_like_query(query[4]),
            build_like_query(query[4]),
            limit,
        ],
        |row| Ok(row.get(0)?),
    )?;

    words_iter.collect()
}

fn predict(
    conn: &Connection,
    query: &[&str; 4],
    limit: i64,
) -> Result<Vec<String>, rusqlite::Error> {
    let mut stmt = conn.prepare(
        "
        select distinct
            word
        from (
            select
                5 as n, five.id as id, one5.word as word
            from
                five_grams as five
            join one_grams as one5
                on five.suffix = one5.id
            join four_grams as four
                on four.id = five.prefix
            join one_grams as one4
                on four.suffix = one4.id
            join three_grams as three
                on three.id = four.prefix
            join one_grams as one3
                on three.suffix = one3.id
            join two_grams as two
                on two.id = three.prefix
            join one_grams as one2
                on two.suffix = one2.id
            join one_grams as one1
                on two.prefix = one1.id
            where
                one1.word = ?
                and
                one2.word = ?
                and
                one3.word = ?
                and
                one4.word = ?

            union all

            select
                4 as n, four.id as id, one4.word as word
            from
                four_grams as four
            join one_grams as one4
                on four.suffix = one4.id
            join three_grams as three
                on three.id = four.prefix
            join one_grams as one3
                on three.suffix = one3.id
            join two_grams as two
                on two.id = three.prefix
            join one_grams as one2
                on two.suffix = one2.id
            join one_grams as one1
                on two.prefix = one1.id
            where
                one1.word = ?
                and
                one2.word = ?
                and
                one3.word = ?

            union all

            select
                3 as n, three.id as id, one3.word as word
            from
                three_grams as three
            join one_grams as one3
                on three.suffix = one3.id
            join two_grams as two
                on two.id = three.prefix
            join one_grams as one2
                on two.suffix = one2.id
            join one_grams as one1
                on two.prefix = one1.id
            where
                one1.word = ?
                and
                one2.word = ?

            union all

            select
                2 as n, two.id as id, one2.word as word
            from
                two_grams as two
            join one_grams as one2
                on two.suffix = one2.id
            join one_grams as one1
                on two.prefix = one1.id
            where
                one1.word = ?

            order by n desc, id
        )
        limit ?
    ",
    )?;

    let words_iter = stmt.query_map(
        params![
            query[0], query[1], query[2], query[3], query[1], query[2], query[3], query[2],
            query[3], query[3], limit,
        ],
        |row| Ok(row.get(0)?),
    )?;

    words_iter.collect()
}

fn main() {
    run().unwrap()
}

fn run() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    let data_path = match env::var("MOCWORD_DATA") {
        Ok(val) => val,
        Err(err) => panic!("invalid MOCWORD_DATA environment variable: {}", err),
    };

    let conn = &Connection::open(data_path)?;
    conn.execute("pragma case_sensitive_like = true", [])?;

    if args.query == "" {
        interact(conn, &args)?;
    } else {
        println!("{}", execute(conn, &args, &args.query)?.join("\n"));
    };

    Ok(())
}

fn interact(conn: &Connection, args: &Args) -> Result<(), Box<dyn std::error::Error>> {
    let scan = io::stdin();
    loop {
        let mut line = String::new();
        let cnt = scan.read_line(&mut line)?;
        if cnt == 0 {
            break;
        }
        println!(
            "{}",
            execute(conn, &args, line.trim_end_matches(&['\r', '\n']))?.join(" ")
        );
    }
    Ok(())
}

fn execute(conn: &Connection, args: &Args, line: &str) -> Result<Vec<String>, rusqlite::Error> {
    let re = Regex::new(r"\s+").unwrap();

    let is_predict = line.ends_with(" ");

    let query: Vec<_> =
        chain!(vec!["", "", "", "", ""].into_iter(), re.split(line.trim()),).collect();
    let query: [&str; 5] = query[query.len() - 5..].try_into().unwrap();

    let res = if is_predict {
        predict(conn, &query[1..].try_into().unwrap(), args.limit)?
    } else {
        search(conn, &query, args.limit)?
    };

    Ok(res)
}

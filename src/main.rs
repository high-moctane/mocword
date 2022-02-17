use itertools::{chain, Itertools};
use rusqlite::{self, params, Connection};

fn predict_one_grams(conn: &Connection, query: &str) -> Result<Vec<String>, rusqlite::Error> {
    let mut stmt = conn.prepare(
        "
        select word from one_grams where word like ? order by id
    ",
    )?;

    let words_iter = stmt.query_map(params![format!("{}%", query)], |row| Ok(row.get(0)?))?;

    words_iter.collect()
}

fn predict_two_grams(conn: &Connection, query: &str) -> Result<Vec<String>, rusqlite::Error> {
    let mut stmt = conn.prepare(
        "
        select
            one2.word
        from
            two_grams as two
        join one_grams as one2
            on two.suffix = one2.id
        join one_grams as one1
            on two.prefix = one1.id
        where
            one1.word = ?
        order by two.id
    ",
    )?;

    let words_iter = stmt.query_map(params![query], |row| Ok(row.get(0)?))?;

    words_iter.collect()
}

fn predict_three_grams(
    conn: &Connection,
    query: [&str; 2],
) -> Result<Vec<String>, rusqlite::Error> {
    let mut stmt = conn.prepare(
        "
        select
            one3.word
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
        order by three.id
    ",
    )?;

    let words_iter = stmt.query_map(params![query[0], query[1]], |row| Ok(row.get(0)?))?;

    words_iter.collect()
}

fn predict_four_grams(conn: &Connection, query: [&str; 3]) -> Result<Vec<String>, rusqlite::Error> {
    let mut stmt = conn.prepare(
        "
        select
            one4.word
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
        order by four.id
    ",
    )?;

    let words_iter =
        stmt.query_map(params![query[0], query[1], query[2]], |row| Ok(row.get(0)?))?;

    words_iter.collect()
}

fn predict_five_grams(conn: &Connection, query: [&str; 4]) -> Result<Vec<String>, rusqlite::Error> {
    let mut stmt = conn.prepare(
        "
        select
            one5.word
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
        order by five.id
    ",
    )?;

    let words_iter = stmt.query_map(params![query[0], query[1], query[2], query[3]], |row| {
        Ok(row.get(0)?)
    })?;

    words_iter.collect()
}

fn predict(conn: &Connection, query: &[&str]) -> Result<Vec<String>, rusqlite::Error> {
    match query.len() {
        0 => panic!("empty query"),
        1 => predict_two_grams(conn, query[0]),
        2 => Ok(chain!(
            predict_three_grams(conn, [query[0], query[1]])?,
            predict(conn, &query[1..])?
        )
        .unique()
        .collect()),
        3 => Ok(chain!(
            predict_four_grams(conn, [query[0], query[1], query[2]])?,
            predict(conn, &query[1..])?
        )
        .unique()
        .collect()),
        4 => Ok(chain!(
            predict_five_grams(conn, [query[0], query[1], query[2], query[3]])?,
            predict(conn, &query[1..])?
        )
        .unique()
        .collect()),
        _ => predict(conn, &query[query.len() - 4..]),
    }
}

fn main() {
    println!("Hello, world!");
}

"""The seed loader splits SQL itself; these cases are why that is not a `.split(';')`."""

from ecet.infrastructure.postgres.seed import SEED_FILES, seed_sql, split_statements


def test_plain_statements_are_split() -> None:
    assert split_statements("SELECT 1; SELECT 2;") == ["SELECT 1", "SELECT 2"]


def test_a_semicolon_inside_a_string_literal_does_not_split() -> None:
    sql = "INSERT INTO t VALUES ('six weeks; then imaging');"
    assert split_statements(sql) == [sql.rstrip(";")]


def test_a_doubled_apostrophe_stays_inside_the_literal() -> None:
    sql = "INSERT INTO t VALUES ('the member''s notes; signed');"
    assert split_statements(sql) == [sql.rstrip(";")]


def test_comment_lines_are_dropped() -> None:
    sql = "-- tenant-a's policies; five of them\nSELECT 1;"
    assert split_statements(sql) == ["SELECT 1"]


def test_a_trailing_statement_without_a_semicolon_still_counts() -> None:
    assert split_statements("SELECT 1") == ["SELECT 1"]


def test_every_seed_file_holds_only_idempotent_inserts() -> None:
    """A seed file is data, never a migration and never a delete."""
    for name in SEED_FILES:
        statements = split_statements(seed_sql(name))
        assert statements, name
        for statement in statements:
            assert statement.upper().startswith("INSERT INTO "), (name, statement[:40])
            assert "ON CONFLICT" in statement.upper(), (name, statement[:40])

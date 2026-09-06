"""Dev seed data.

SQL files rather than Python objects, because seeding is a data concern and the files
double as a readable description of the demo environment. `ecet seed` refuses to run
outside `ECET_ENV=dev`.

Format rules the splitter relies on: statements end with `;`, string literals use
single quotes with `''` for an embedded apostrophe, and comments occupy a whole line.
"""

from importlib.resources import files

from sqlalchemy.ext.asyncio import AsyncEngine

#: Applied in order — policies reference tenants, and every policy code must already
#: exist in the catalogue.
SEED_FILES = ("icd10_codes.sql", "tenants.sql", "policies.sql")


def seed_sql(name: str) -> str:
    return (files(__package__) / name).read_text(encoding="utf-8")


def split_statements(sql: str) -> list[str]:
    """Split on `;` while respecting single-quoted literals.

    `str.split(";")` breaks the moment a criteria text contains a semicolon — and the
    seeded policies do.
    """
    without_comments = "\n".join(
        line for line in sql.splitlines() if not line.strip().startswith("--")
    )
    statements: list[str] = []
    current: list[str] = []
    in_literal = False
    for character in without_comments:
        if character == "'":
            in_literal = not in_literal
            current.append(character)
        elif character == ";" and not in_literal:
            statements.append("".join(current))
            current = []
        else:
            current.append(character)
    statements.append("".join(current))
    return [statement.strip() for statement in statements if statement.strip()]


async def load_seed(engine: AsyncEngine) -> int:
    """Apply every seed file in one transaction. Returns the statement count."""
    applied = 0
    async with engine.begin() as connection:
        for name in SEED_FILES:
            for statement in split_statements(seed_sql(name)):
                await connection.exec_driver_sql(statement)
                applied += 1
    return applied

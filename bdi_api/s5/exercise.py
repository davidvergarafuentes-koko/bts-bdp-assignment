from __future__ import annotations

from pathlib import Path
from typing import Annotated, Iterable

from fastapi import APIRouter, HTTPException, status
from fastapi.params import Query
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from bdi_api.settings import Settings

settings = Settings()

s5 = APIRouter(
    responses={
        status.HTTP_404_NOT_FOUND: {"description": "Not found"},
        status.HTTP_422_UNPROCESSABLE_ENTITY: {"description": "Something is wrong with the request"},
    },
    prefix="/api/s5",
    tags=["s5"],
)


# ---------- Helpers ----------

def _engine() -> Engine:
    """
    Build a SQLAlchemy engine using Settings().db_url (env var BDI_DB_URL).
    The homework mentions Postgres, but the default may be sqlite:///hr_database.db.
    """
    return create_engine(settings.db_url, future=True)


def _is_postgres(engine: Engine) -> bool:
    return engine.dialect.name.lower() in {"postgresql", "postgres"}


def _strip_sql_comments(sql: str) -> str:
    """Remove -- line comments and /* */ block comments (best-effort)."""
    out_lines: list[str] = []
    in_block = False
    for line in sql.splitlines():
        s = line
        if not in_block:
            while "/*" in s:
                pre, rest = s.split("/*", 1)
                if "*/" in rest:
                    _, post = rest.split("*/", 1)
                    s = pre + post
                else:
                    s = pre
                    in_block = True
                    break
        else:
            if "*/" in s:
                _, post = s.split("*/", 1)
                s = post
                in_block = False
            else:
                continue

        if "--" in s:
            pre, _ = s.split("--", 1)
            s = pre
        out_lines.append(s)
    return "\n".join(out_lines)


def _split_sql_statements(sql: str) -> list[str]:
    """
    Split SQL by semicolons, attempting to respect single-quoted strings.
    Good enough for typical schema/seed scripts in this homework.
    """
    sql = _strip_sql_comments(sql)
    stmts: list[str] = []
    buf: list[str] = []
    in_single = False
    i = 0
    while i < len(sql):
        ch = sql[i]
        if ch == "'":
            if in_single and i + 1 < len(sql) and sql[i + 1] == "'":
                buf.append("''")
                i += 2
                continue
            in_single = not in_single
            buf.append(ch)
        elif ch == ";" and not in_single:
            stmt = "".join(buf).strip()
            if stmt:
                stmts.append(stmt)
            buf = []
        else:
            buf.append(ch)
        i += 1
    tail = "".join(buf).strip()
    if tail:
        stmts.append(tail)
    return stmts


def _run_sql(engine: Engine, statements: Iterable[str]) -> None:
    with engine.begin() as conn:
        for stmt in statements:
            conn.execute(text(stmt))


def _execute_sql_file(engine: Engine, sql_file: Path) -> None:
    if not sql_file.exists():
        raise FileNotFoundError(f"SQL file not found: {sql_file}")
    content = sql_file.read_text(encoding="utf-8")
    statements = _split_sql_statements(content)
    _run_sql(engine, statements)


def _base_dir() -> Path:
    """
    Try to locate the directory where hr_schema.sql and hr_seed_data.sql live.
    Common layouts:
      - bts-bdp-exercises/s5/hr_schema.sql
      - bdi_api/s5/hr_schema.sql
    """
    here = Path(__file__).resolve().parent
    candidates = [
        here,
        here.parent.parent.parent / "bts-bdp-exercises" / "s5",
        Path.cwd() / "bts-bdp-exercises" / "s5",
        Path.cwd(),
    ]
    for c in candidates:
        if (c / "hr_schema.sql").exists() and (c / "hr_seed_data.sql").exists():
            return c
    return here


def _drop_all_tables(engine: Engine) -> None:
    """
    Make /db/init idempotent: drop tables in reverse dependency order.
    For Postgres use CASCADE; for SQLite omit it.
    """
    cascade = " CASCADE" if _is_postgres(engine) else ""
    drops = [
        f"DROP TABLE IF EXISTS salary_history{cascade}",
        f"DROP TABLE IF EXISTS employee_project{cascade}",
        f"DROP TABLE IF EXISTS project{cascade}",
        f"DROP TABLE IF EXISTS employee{cascade}",
        f"DROP TABLE IF EXISTS department{cascade}",
    ]
    _run_sql(engine, drops)


def _ensure_department_exists(engine: Engine, dept_id: int) -> str:
    with engine.connect() as conn:
        row = conn.execute(
            text("SELECT name FROM department WHERE id = :dept_id"),
            {"dept_id": dept_id},
        ).mappings().first()
    if not row:
        raise HTTPException(status_code=404, detail="Department not found")
    return str(row["name"])


def _ensure_employee_exists(engine: Engine, emp_id: int) -> None:
    with engine.connect() as conn:
        row = conn.execute(
            text("SELECT id FROM employee WHERE id = :emp_id"),
            {"emp_id": emp_id},
        ).first()
    if not row:
        raise HTTPException(status_code=404, detail="Employee not found")


# ---------- Endpoints ----------

@s5.post("/db/init")
def init_database() -> str:
    """Create all HR database tables."""
    engine = _engine()
    _drop_all_tables(engine)
    sql_dir = _base_dir()
    _execute_sql_file(engine, sql_dir / "hr_schema.sql")
    return "OK"


@s5.post("/db/seed")
def seed_database() -> str:
    """Populate the HR database with sample data."""
    engine = _engine()
    sql_dir = _base_dir()
    _execute_sql_file(engine, sql_dir / "hr_seed_data.sql")
    return "OK"


@s5.get("/departments/")
def list_departments() -> list[dict]:
    """Return all departments: id, name, location."""
    engine = _engine()
    with engine.connect() as conn:
        rows = conn.execute(
            text("SELECT id, name, location FROM department ORDER BY id")
        ).mappings().all()
    return [dict(r) for r in rows]


@s5.get("/employees/")
def list_employees(
    page: Annotated[int, Query(description="Page number (1-indexed)", ge=1)] = 1,
    per_page: Annotated[int, Query(description="Number of employees per page", ge=1, le=100)] = 10,
) -> list[dict]:
    """Return employees with department name, paginated."""
    engine = _engine()
    offset = (page - 1) * per_page
    sql = """
    SELECT
        e.id,
        e.first_name,
        e.last_name,
        e.email,
        e.salary,
        d.name AS department_name
    FROM employee e
    JOIN department d ON d.id = e.department_id
    ORDER BY e.id
    OFFSET :offset
    LIMIT :limit
    """
    with engine.connect() as conn:
        rows = conn.execute(text(sql), {"offset": offset, "limit": per_page}).mappings().all()
    return [dict(r) for r in rows]


@s5.get("/departments/{dept_id}/employees")
def list_department_employees(dept_id: int) -> list[dict]:
    """Return all employees in a specific department."""
    engine = _engine()
    _ensure_department_exists(engine, dept_id)
    sql = """
    SELECT id, first_name, last_name, email, salary, hire_date
    FROM employee
    WHERE department_id = :dept_id
    ORDER BY id
    """
    with engine.connect() as conn:
        rows = conn.execute(text(sql), {"dept_id": dept_id}).mappings().all()
    return [dict(r) for r in rows]


@s5.get("/departments/{dept_id}/stats")
def department_stats(dept_id: int) -> dict:
    """Return KPI statistics for a department."""
    engine = _engine()
    _ensure_department_exists(engine, dept_id)
    sql = """
    SELECT
        d.name AS department_name,
        COUNT(DISTINCT e.id) AS employee_count,
        AVG(e.salary) AS avg_salary,
        COUNT(DISTINCT ep.project_id) AS project_count
    FROM department d
    LEFT JOIN employee e ON e.department_id = d.id
    LEFT JOIN employee_project ep ON ep.employee_id = e.id
    WHERE d.id = :dept_id
    GROUP BY d.name
    """
    with engine.connect() as conn:
        row = conn.execute(text(sql), {"dept_id": dept_id}).mappings().first()

    avg_salary = float(row["avg_salary"]) if row and row["avg_salary"] is not None else None
    return {
        "department_name": row["department_name"] if row else None,
        "employee_count": int(row["employee_count"] or 0) if row else 0,
        "avg_salary": avg_salary,
        "project_count": int(row["project_count"] or 0) if row else 0,
    }


@s5.get("/employees/{emp_id}/salary-history")
def salary_history(emp_id: int) -> list[dict]:
    """Return the salary evolution for an employee, ordered by date."""
    engine = _engine()
    _ensure_employee_exists(engine, emp_id)
    sql = """
    SELECT change_date, old_salary, new_salary, reason
    FROM salary_history
    WHERE employee_id = :emp_id
    ORDER BY change_date ASC, id ASC
    """
    with engine.connect() as conn:
        rows = conn.execute(text(sql), {"emp_id": emp_id}).mappings().all()
    return [dict(r) for r in rows]

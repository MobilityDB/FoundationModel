"""
STAGE 0: select a few calendar days per month from AIS dynamic parquet(s), to
bound how much data flows through the pipeline

Works for both dataset layouts:
  - Piraeus: many monthly files, millisecond epoch, time column 't' or 'timestamp'
  - Brest:   one 6-month file, second epoch, time column 't'
The time column and epoch unit are auto-detected; the time column is normalized
to 'timestamp' on output; the months to sample come from each file's own data
span. Day boundaries use the original UTC timestamps. Filtering pushes a
timestamp-range predicate into read_parquet so DuckDB prunes row groups and
never materializes the whole file

Usage:
    # list which calendar days have data, per month
    python run_select_days.py --input /home/ubuntu/thesis/data/ais/piraeus --list

    # Piraeus (prefix defaults to 'piraeus')
    python run_select_days.py --input /home/ubuntu/thesis/data/ais/piraeus \
        --output /home/ubuntu/thesis/data/ais/piraeus_days --days 1,7,15

    # Brest (single file)
    python run_select_days.py \
        --input /home/ubuntu/thesis/data/ais/brest/ais-data/nari_dynamic.parquet \
        --output /home/ubuntu/thesis/data/ais/brest_days --prefix brest --days 1,7,15
"""
from __future__ import annotations

import argparse
import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path

import duckdb

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)-8s %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)

def _files(input_path: Path) -> list[Path]:
    """A single parquet file, or every *dynamic*.parquet under a directory"""
    if input_path.is_file():
        return [input_path]
    return sorted(input_path.rglob('*dynamic*.parquet'))

def _time_col(con: duckdb.DuckDBPyConnection, f: Path) -> str:
    """Return the time column name ('t' or 'timestamp') present in the file"""
    names = [r[0] for r in con.execute(
        f"DESCRIBE SELECT * FROM read_parquet('{f}')").fetchall()]
    for c in ('t', 'timestamp'):
        if c in names:
            return c
    raise ValueError(f'no t/timestamp column in {f.name} (has: {names})')

def _unit_div(con: duckdb.DuckDBPyConnection, f: Path, tcol: str) -> int:
    """Epoch-unit divisor to reach seconds: 1000 for millisecond epoch, else 1"""
    mx = con.execute(f'SELECT MAX("{tcol}") FROM read_parquet(\'{f}\')').fetchone()[0]
    return 1000 if (mx is not None and mx > 1e12) else 1

def _span_months(lo_s: int, hi_s: int) -> list[tuple[int, int]]:
    """List of (year, month) covering the [lo_s, hi_s] second-epoch span"""
    lo = datetime.fromtimestamp(lo_s, tz=timezone.utc)
    hi = datetime.fromtimestamp(hi_s, tz=timezone.utc)
    out, y, m = [], lo.year, lo.month
    while (y, m) <= (hi.year, hi.month):
        out.append((y, m))
        m += 1
        if m > 12:
            m, y = 1, y + 1
    return out

def _day_bounds(year: int, month: int, day: int, div: int) -> tuple[int, int]:
    """[start, end) of a UTC calendar day, in the file's native epoch unit"""
    start = datetime(year, month, day, tzinfo=timezone.utc)
    end = start + timedelta(days=1)
    return int(start.timestamp()) * div, int(end.timestamp()) * div

def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument('--input', required=True)
    parser.add_argument('--output', required=True)
    parser.add_argument('--prefix', required=True)
    parser.add_argument('--days', default='1,7,15')
    parser.add_argument('--list', action='store_true')
    args = parser.parse_args()

    files = _files(Path(args.input))
    if not files:
        logger.error('No *dynamic*.parquet found at %s', args.input)
        return

    con = duckdb.connect()

    if args.list:
        for f in files:
            tcol = _time_col(con, f)
            div = _unit_div(con, f, tcol)
            days = con.execute(f"""
                SELECT strftime(to_timestamp("{tcol}" / {div}), '%Y-%m') AS ym,
                       COUNT(DISTINCT strftime(to_timestamp("{tcol}" / {div}), '%d')) AS d
                FROM read_parquet('{f}') GROUP BY ym ORDER BY ym
            """).fetchall()
            logger.info('%s:', f.name)
            for ym, d in days:
                logger.info('\t%s: %d days', ym, d)
        return

    days = [int(x) for x in args.days.split(',') if x.strip()]
    out_dir = Path(args.output)
    out_dir.mkdir(parents=True, exist_ok=True)

    n_written = n_empty = 0
    for f in files:
        tcol = _time_col(con, f)
        div = _unit_div(con, f, tcol)
        # Keep all columns, but normalize the time column name to 'timestamp'
        proj = ('*' if tcol == 'timestamp'
                else f'* EXCLUDE("{tcol}"), "{tcol}" AS timestamp')
        lo_t, hi_t = con.execute(
            f'SELECT MIN("{tcol}"), MAX("{tcol}") FROM read_parquet(\'{f}\')').fetchone()

        for year, month in _span_months(lo_t // div, hi_t // div):
            for day in days:
                try:
                    lo, hi = _day_bounds(year, month, day, div)
                except ValueError:
                    continue  # e.g. day 31 in a 30-day month
                out = out_dir / f'{args.prefix}-{year:04d}-{month:02d}-{day:02d}.parquet'
                con.execute(f"""
                    COPY (
                        SELECT {proj}
                        FROM read_parquet('{f}')
                        WHERE "{tcol}" >= {lo} AND "{tcol}" < {hi}
                    ) TO '{out}' (FORMAT PARQUET)
                """)
                n = con.execute(f"SELECT COUNT(*) FROM read_parquet('{out}')").fetchone()[0]
                if n == 0:
                    out.unlink(missing_ok=True)
                    n_empty += 1
                    logger.warning('\t%04d-%02d-%02d: no data (coverage gap) -- skipped', year, month, day)
                else:
                    n_written += 1
                    logger.info('\t%s -> %s (%d points)', f.name, out.name, n)

    logger.info('Done. wrote %d day-file(s), %d empty/skipped', n_written, n_empty)

if __name__ == '__main__':
    main()
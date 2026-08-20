"""
STAGE 1: Ingest & preprocess raw AIS into DuckDB, then emit the annotation inputs.

Source-specific loading lives in per-source handlers, each of which
builds a DuckDB table named `filtered`. This module orchestrates by
dispatching to the right handler, then running the steps common to every source.

Output files:
    _full_ais_parquet       All columns, forward-filled, sorted by (MMSI, ts_unix)
                            Used in Stage 3 to recover AIS fields that the annotation tool strips
    _for_annotation.txt     Space-delimited ASCII: MMSI lon lat ts_unix
                            Sorted by ts_unix in ascending order. To be used
                            as input for the Archimedes' annotation binary
    _vessel_info.csv        Semicolon-delimited: MMSI;TYPE_CODE;TYPE;DESCRIPTION
                            One row per unique MMSI, used by Stage 2 annotation binary
                            to select per-type thresholds

Common preprocessing steps (in DuckDB), applied to every source:
    1. [source handler] load raw input -> table `filtered`
    2. Parse timestamp to Unix epoch seconds (ingest.ts_unit='ms' for ms epoch)
    3. Deduplicate on (MMSI, ts_unix)
    4. Forward-fill static fields with LAST_VALUE IGNORE NULLS
    5. Sort by (MMSI, ts_unix)
"""
from __future__ import annotations

import csv
import logging
from pathlib import Path

import duckdb

from . import brest, denmark, piraeus, united_states
from .common import Cols, DEFAULT_FILL_COLS, itu_to_type

logger = logging.getLogger(__name__)

# source tag -> handler that builds the `filtered` table
_BUILDERS = {
    'dma': denmark.build_filtered,
    'us': united_states.build_filtered,
    'piraeus': piraeus.build_filtered,
    'brest': brest.build_filtered,
}

def run(raw_csv: Path | str, output_dir: Path | str, cfg: dict) -> tuple[Path, Path, Path]:
    """Preprocess one raw AIS input file (CSV or parquet, per source)"""
    raw_csv = Path(raw_csv)
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    stem = raw_csv.stem
    parquet_out = output_dir / f'{stem}_full_ais.parquet'
    txt_out = output_dir / f'{stem}_for_annotation.txt'
    vessel_info_out = output_dir / f'{stem}_vessel_info.csv'

    ingest_cfg = cfg.get('ingest', {})
    raw_cols = ingest_cfg.get('raw_columns', {})
    fill_cols = ingest_cfg.get('forward_fill_cols', DEFAULT_FILL_COLS)
    source = ingest_cfg.get('source', 'dma').lower()
    ts_format = ingest_cfg.get('ts_format', '%Y-%m-%d %H:%M:%S')
    ts_unit = ingest_cfg.get('ts_unit', 's')

    if source not in _BUILDERS:
        raise ValueError(f'Unknown ingest source {source!r}; expected one of {sorted(_BUILDERS)}')

    # Resolve raw-column names (defaults match the DMA schema)
    cols = Cols(
        mmsi=raw_cols.get('mmsi', 'MMSI'),
        ts=raw_cols.get('timestamp', '# Timestamp'),
        lat=raw_cols.get('lat', 'Latitude'),
        lon=raw_cols.get('lon', 'Longitude'),
        mobile=raw_cols.get('mobile_type', 'Type of mobile'),
    )
    col_mmsi, col_ts, col_lat, col_lon = cols.mmsi, cols.ts, cols.lat, cols.lon

    db_path = output_dir / f'{stem}_ingest.duckdb'
    con = duckdb.connect(str(db_path))
    con.execute("SET memory_limit='15GB'")
    con.execute("SET temp_directory='/tmp/duckdb_tmp/'")
    con.execute('INSTALL spatial; LOAD spatial;')

    logger.info('Reading %s (source=%s)', raw_csv, source)

    # ========== Source-specific: build the `filtered` table ==========
    _BUILDERS[source](con, raw_csv, ingest_cfg, cols)

    # ========== Parse timestamp to Unix epoch seconds ==========
    # Choose the conversion from the column's actual type (DuckDB binds every
    # branch of a CASE, so epoch() cannot appear alongside an integer column):
    #   integer epoch -> divide (by 1000 for ms), e.g. Brest seconds, Piraeus ms
    #   native TIMESTAMP -> epoch()
    #   string -> strptime(ts_format) then epoch()
    ts_type = next((r[1] for r in con.execute('DESCRIBE filtered').fetchall() if r[0] == col_ts), '')
    if ts_unit == 'ms' or ts_type in ('BIGINT', 'INTEGER', 'HUGEINT'):
        div = 1000 if ts_unit == 'ms' else 1
        ts_expr = f'CAST("{col_ts}" / {div} AS BIGINT)'
    elif ts_type.startswith('TIMESTAMP'):
        ts_expr = f'CAST(epoch("{col_ts}") AS BIGINT)'
    else:
        ts_expr = (f'CAST(epoch(strptime(CAST("{col_ts}" AS VARCHAR), '
                   f"'{ts_format}')) AS BIGINT)")
    con.execute(f"""
        CREATE OR REPLACE TABLE with_epoch AS
        SELECT *, {ts_expr} AS ts_unix
        FROM filtered
    """)
    con.execute('DROP TABLE filtered')

    # ========== Deduplicate based on (MMSI, ts_unix) ==========
    con.execute(f"""
        CREATE OR REPLACE TABLE deduped AS
        SELECT * FROM (
            SELECT *,
                ROW_NUMBER() OVER (
                    PARTITION BY "{col_mmsi}", ts_unix
                    ORDER BY ts_unix
                ) AS _rn
            FROM with_epoch
        )
        WHERE _rn = 1
    """)
    con.execute('DROP TABLE with_epoch')

    # ========== Forward-fill static fields ==========
    available = {row[0] for row in con.execute('DESCRIBE deduped').fetchall()}
    fill_exprs = []
    for col in fill_cols:
        if col in available:
            fill_exprs.append(
                f'LAST_VALUE("{col}" IGNORE NULLS) OVER '
                f'(PARTITION BY "{col_mmsi}" ORDER BY ts_unix '
                f'ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS "{col}"'
            )

    if fill_exprs:
        other_cols = [f'"{c}"' for c in available if c not in fill_cols and c != '_rn']
        select_list = ', '.join(other_cols + fill_exprs)
        con.execute(f"""
            CREATE OR REPLACE TABLE filled AS
            SELECT {select_list}
            FROM deduped
        """)
    else:
        con.execute("""
            CREATE OR REPLACE TABLE filled AS
            SELECT * EXCLUDE (_rn) FROM deduped
        """)
    con.execute('DROP TABLE deduped')

    # ========== Sort by (MMSI, ts_unix) ==========
    con.execute(f"""
        CREATE OR REPLACE TABLE sorted AS
        SELECT * FROM filled
        ORDER BY "{col_mmsi}", ts_unix
    """)
    con.execute('DROP TABLE filled')

    # ========== Output full AIS parquet ==========
    logger.info('Writing full AIS .parquet file: %s', parquet_out)
    con.execute(f"COPY sorted TO '{parquet_out}' (FORMAT PARQUET)")

    # ========== Output space-delimited ASCII for annotation ==========
    # Format: MMSI lon lat ts_unix (no header)
    logger.info('Writing annotation input: %s', txt_out)
    con.execute(f"""
        COPY (
            SELECT
                CAST("{col_mmsi}" AS BIGINT),
                CAST("{col_lon}" AS DOUBLE),
                CAST("{col_lat}" AS DOUBLE),
                CAST(ts_unix AS BIGINT)
            FROM sorted
        ) TO '{txt_out}' (DELIMITER ' ', HEADER FALSE)
    """)

    # ========== Output semicolon-delimited vessel info .csv file ==========
    # Format: MMSI;TYPE_CODE;TYPE;DESCRIPTION
    col_ship_type = raw_cols.get('ship_type', 'Ship type')
    ship_type_available = col_ship_type in {
        row[0] for row in con.execute('DESCRIBE sorted').fetchall()
    }

    if ship_type_available:
        rows = con.execute(f"""
            SELECT DISTINCT
                CAST("{col_mmsi}" AS BIGINT) AS mmsi,
                "{col_ship_type}" AS ship_type
            FROM sorted
            ORDER BY mmsi
        """).fetchall()
    else:
        rows = con.execute(f"""
        SELECT DISTINCT CAST("{col_mmsi}" AS BIGINT) AS mmsi
        FROM sorted
        ORDER BY mmsi
        """).fetchall()
        rows = [(r[0], None) for r in rows]

    logger.info('Writing vessel info .csv: %s', vessel_info_out)
    with open(vessel_info_out, 'w', newline='') as f:
        writer = csv.writer(f, delimiter=';')
        writer.writerow(['MMSI', 'TYPE_CODE', 'TYPE', 'DESCRIPTION'])
        for mmsi, ship_type_raw in rows:
            arch_type = itu_to_type(ship_type_raw)
            writer.writerow([mmsi, ship_type_raw or '', arch_type, arch_type])

    n_rows = con.execute('SELECT COUNT(*) FROM sorted').fetchone()[0]
    n_vessels = len(rows)
    logger.info('Ingest done: %d points, %d vessels -> %s', n_rows, n_vessels, output_dir)

    con.close()
    db_path.unlink(missing_ok=True)
    return parquet_out, txt_out, vessel_info_out
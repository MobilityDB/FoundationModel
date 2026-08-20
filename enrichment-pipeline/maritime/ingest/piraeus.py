"""
Piraeus (unipi) ingest handler

The dynamic files are parquet and carry only a hashed vessel_id, lon/lat, and
speed/course/heading; ship type lives in a separate static CSV. This builds a
surrogate integer MMSI (so the annotation binary and the (MMSI, ts) joins that
expect an integer id keep working), attaches the static ship type, and renames
speed/course/heading to the DMA canonical SOG/COG/Heading. The original hash is
kept in `vessel_id` for provenance. Timestamps are millisecond epoch, handled by
the common step via ingest.ts_unit='ms'
"""
from __future__ import annotations

import logging
from pathlib import Path

from .common import Cols, coord_filter_sql

logger = logging.getLogger(__name__)

def build_filtered(con, raw_path, ingest_cfg: dict, cols: Cols) -> None:
    """Load the Piraeus parquet (+ static join) and create table `filtered`"""
    static_csv = ingest_cfg.get('static_csv')
    if not static_csv or not Path(static_csv).exists():
        raise FileNotFoundError(f'Piraeus static file not found: {static_csv}')
    coord = coord_filter_sql(cols.lat, cols.lon)

    con.execute(f"""
        CREATE OR REPLACE TABLE _dyn AS
        SELECT
            d.vessel_id AS vessel_id,
            DENSE_RANK() OVER (ORDER BY d.vessel_id) AS "{cols.mmsi}",
            d.lon AS "{cols.lon}",
            d.lat AS "{cols.lat}",
            d.speed AS "SOG",
            d.course AS "COG",
            d.heading AS "Heading",
            d."{cols.ts}" AS "{cols.ts}",
            CAST(s.shiptype AS INTEGER) AS "Ship type"
        FROM read_parquet('{raw_path}') d
        LEFT JOIN read_csv_auto('{static_csv}') s ON d.vessel_id = s.vessel_id
    """)
    con.execute(f"""
        CREATE OR REPLACE TABLE filtered AS
        SELECT * FROM _dyn WHERE {coord}
    """)
    con.execute('DROP TABLE _dyn')
    # Null the AIS "not available" sentinels (Piraeus SOG piles up at 102.2)
    con.execute('UPDATE filtered SET "SOG" = NULL WHERE "SOG" >= 102.2')
    con.execute('UPDATE filtered SET "COG" = NULL WHERE "COG" >= 360')
    con.execute('UPDATE filtered SET "Heading" = NULL WHERE "Heading" >= 511')
    logger.info('Piraeus ingest: surrogate MMSI + static ship-type join, sentinels nulled')
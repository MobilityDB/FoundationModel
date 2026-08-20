"""
Brest (NARI / Naval Academy) ingest handler

The dynamic files are parquet and carry a real MMSI, a numeric navigational
status, and ROT; ship type (plus draught / name / destination) live in a
separate static parquet. This renames the NARI columns to the DMA canonical
names, maps the numeric nav-status to text (shared US/NARI code table), joins
the per-MMSI static attributes, and nulls the AIS sentinels. ROT is the raw AIS
indicator (-127..127), the same encoding as DMA, so it passes through unchanged.
Timestamps are integer epoch seconds, handled by the common step
"""
from __future__ import annotations

import logging
from pathlib import Path

from .common import Cols, coord_filter_sql, NAV_STATUS

logger = logging.getLogger(__name__)

def build_filtered(con, raw_path, ingest_cfg: dict, cols: Cols) -> None:
    """Load the Brest parquet (+ static join) and create table `filtered`"""
    static_csv = ingest_cfg.get('static_csv')
    if not static_csv or not Path(static_csv).exists():
        raise FileNotFoundError(f'Brest static file not found: {static_csv}')
    coord = coord_filter_sql(cols.lat, cols.lon)
    nav_case = '\n'.join(f'                    WHEN {code} THEN \'{txt}\''
                         for code, txt in NAV_STATUS.items())

    # One static row per MMSI: most common ship type / name / destination
    con.execute(f"""
        CREATE OR REPLACE TABLE _static AS
        SELECT sourcemmsi,
               mode(shiptype) AS "Ship type",
               mode(shipname) AS "Name",
               max(draught) AS "Draught",
               mode(destination) AS "Destination"
        FROM read_parquet('{static_csv}')
        GROUP BY sourcemmsi
    """)
    con.execute(f"""
        CREATE OR REPLACE TABLE _dyn AS
        SELECT
            d.sourcemmsi AS "{cols.mmsi}",
            d.lon AS "{cols.lon}",
            d.lat AS "{cols.lat}",
            d.speedoverground AS "SOG",
            d.courseoverground AS "COG",
            d.trueheading AS "Heading",
            d.rateofturn AS "ROT",
            CASE CAST(d.navigationalstatus AS INTEGER)
{nav_case}
                ELSE NULL
            END AS "Navigational status",
            d."{cols.ts}" AS "{cols.ts}",
            s."Ship type", s."Name", s."Draught", s."Destination"
        FROM read_parquet('{raw_path}') d
        LEFT JOIN _static s ON d.sourcemmsi = s.sourcemmsi
    """)
    con.execute(f"""
        CREATE OR REPLACE TABLE filtered AS
        SELECT * FROM _dyn WHERE {coord}
    """)
    con.execute('DROP TABLE _dyn')
    con.execute('DROP TABLE _static')
    con.execute('UPDATE filtered SET "SOG" = NULL WHERE "SOG" >= 102.2')
    con.execute('UPDATE filtered SET "COG" = NULL WHERE "COG" >= 360')
    con.execute('UPDATE filtered SET "Heading" = NULL WHERE "Heading" = 511 OR "Heading" > 359')
    logger.info('Brest ingest: numeric nav-status mapped + static join, sentinels nulled')
"""
United States (NOAA) ingest handler

Loads the CSV exactly like Denmark, then applies the US-specific cleaning:
  - null the AIS "not available" sentinels NOAA encodes numerically
    (SOG 102.3, COG 360.0, Heading 511)
  - rename the US columns to the DMA canonical set
  - map the numeric navigational status to DMA-style text
"""
from __future__ import annotations

import logging

from . import denmark
from .common import Cols, NAV_STATUS

logger = logging.getLogger(__name__)

# US column name -> DMA canonical name
_US_RENAME = {
    'Status':     'Navigational status',
    'Draft':      'Draught',
    'VesselType': 'Ship type',
    'VesselName': 'Name',
}

def build_filtered(con, raw_path, ingest_cfg: dict, cols: Cols) -> None:
    """Load the raw NOAA CSV, then normalize it to the DMA canonical layout"""
    denmark.build_filtered(con, raw_path, ingest_cfg, cols)
    raw_cols = ingest_cfg.get('raw_columns', {})

    # ---- Null AIS "not available" sentinels ----
    avail = {row[0] for row in con.execute('DESCRIBE filtered').fetchall()}
    col_sog = raw_cols.get('sog', 'SOG')
    col_cog = raw_cols.get('cog', 'COG')
    col_head = raw_cols.get('heading', 'Heading')
    if col_sog in avail:
        con.execute(f'UPDATE filtered SET "{col_sog}" = NULL WHERE "{col_sog}" >= 102.3')
    if col_cog in avail:
        con.execute(f'UPDATE filtered SET "{col_cog}" = NULL WHERE "{col_cog}" >= 360.0')
    if col_head in avail:
        con.execute(f'UPDATE filtered SET "{col_head}" = NULL '
                    f'WHERE "{col_head}" = 511 OR "{col_head}" > 359')
    logger.info('US sentinel cleaning: nulled SOG/COG/Heading not-available codes')

    # ---- Normalize column names to the DMA canonical set ----
    avail = {row[0] for row in con.execute('DESCRIBE filtered').fetchall()}
    for src_name, canon in _US_RENAME.items():
        if src_name in avail and canon not in avail:
            con.execute(f'ALTER TABLE filtered RENAME COLUMN "{src_name}" TO "{canon}"')
    logger.info('US column normalization: Status/Draft/VesselType/VesselName '
                '-> DMA canonical names')

    # ---- Numeric nav-status -> text ----
    if 'Navigational status' in {row[0] for row in
                                 con.execute('DESCRIBE filtered').fetchall()}:
        nav_case = '\n'.join(f"                    WHEN {code} THEN '{txt}'"
                             for code, txt in NAV_STATUS.items())
        con.execute(f"""
            CREATE OR REPLACE TABLE filtered AS
            SELECT * EXCLUDE ("Navigational status"),
                CASE TRY_CAST("Navigational status" AS INTEGER)
{nav_case}
                    ELSE NULL
                END AS "Navigational status"
            FROM filtered
        """)
        logger.info('US nav-status codes mapped to text')
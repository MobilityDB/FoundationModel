"""
Denmark (DMA / aisdk) ingest handler

Input is per-day CSV already in the DMA canonical column layout (SOG, COG,
Heading, ROT, Navigational status, Ship type, ...). All that is needed is to
load it, keep Class A/B mobile types, and drop invalid coordinates
"""
from __future__ import annotations

import logging

from .common import Cols, coord_filter_sql

logger = logging.getLogger(__name__)

def build_filtered(con, raw_path, ingest_cfg: dict, cols: Cols) -> None:
    """Load the raw DMA CSV and create table `filtered`"""
    mobile_filter = ingest_cfg.get('mobile_type_filter')
    coord = coord_filter_sql(cols.lat, cols.lon)

    con.execute(f"""
        CREATE OR REPLACE TABLE raw AS
        SELECT * FROM read_csv_auto('{raw_path}', header=true, sample_size=10000)
    """)

    if mobile_filter:
        values = ', '.join(f"'{v}'" for v in mobile_filter)
        con.execute(f"""
            CREATE OR REPLACE TABLE filtered AS
            SELECT * FROM raw
            WHERE "{cols.mobile}" IN ({values})
                AND {coord}
        """)
    else:
        con.execute(f"""
            CREATE OR REPLACE TABLE filtered AS
            SELECT * FROM raw
            WHERE {coord}
        """)
    con.execute('DROP TABLE raw')

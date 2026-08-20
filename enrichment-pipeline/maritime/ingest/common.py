"""
Shared helpers and code tables for the AIS ingest source handlers

Each source handler builds a DuckDB table named `filtered` from its raw input,
ingest/preprocess.py then runs the steps common to every source

This module holds what those handlers share: resolved column names,
the coordinate filter, ship-type/nav-status code tables, and the ship-type
-> Archimedes label mapping used to build vessel_info.csv
"""
from __future__ import annotations

from dataclasses import dataclass

# Static features to be forward-filled (per source, whichever are present)
DEFAULT_FILL_COLS = ['Name', 'Ship type', 'Draught', 'Length', 'Width', 'Destination', 'ETA']

# ITU ship-type code -> Archimedes type label
ITU_TO_ARCHIMEDES_TYPE: dict[int, str] = {
    30: 'Fishing',
    31: 'Tug',   # towing
    32: 'Tug',   # towing long/wide
    36: 'Sailing',
    37: 'Pleasure Craft',
    52: 'Tug',
    53: 'Tug',   # port tender
}
for _code in range(40, 50):
    ITU_TO_ARCHIMEDES_TYPE[_code] = 'High speed craft'
for _code in range(60, 70):
    ITU_TO_ARCHIMEDES_TYPE[_code] = 'Passenger'
for _code in range(70, 80):
    ITU_TO_ARCHIMEDES_TYPE[_code] = 'Cargo'
for _code in range(80, 90):
    ITU_TO_ARCHIMEDES_TYPE[_code] = 'Tanker'

TEXT_TO_ARCHIMEDES_TYPE: dict[str, str] = {
    'fishing':           'Fishing',
    'cargo':             'Cargo',
    'tanker':            'Tanker',
    'passenger':         'Passenger',
    'tug':               'Tug',
    'towing':            'Tug',
    'towing long/wide':  'Tug',
    'port tender':       'Tug',
    'sailing':           'Sailing',
    'pleasure':          'Pleasure Craft',
    'hsc':               'High speed craft',
    'dredging':          'Default',
    'pilot':             'Default',
    'sar':               'Default',
    'military':          'Default',
    'law enforcement':   'Default',
    'medical':           'Default',
    'diving':            'Default',
    'anti-pollution':    'Default',
    'other':             'Default',
    'undefined':         'Default',
    'reserved':          'Default',
    'spare 1':           'Default',
    'spare 2':           'Default',
    'not party to conflict': 'Default',
}

# AIS navigational-status codes. Sources that encode these
# numerically (NOAA, NARI/Brest) map them to DMA-style text
# at ingest so the text-keyed phase classifier works downstream
NAV_STATUS: dict[int, str] = {
    0: 'under way using engine',
    1: 'at anchor',
    2: 'not under command',
    3: 'restricted manoeuvrability',
    4: 'constrained by her draught',
    5: 'moored',
    6: 'aground',
    7: 'engaged in fishing',
    8: 'under way sailing',
    9: 'reserved for high speed craft',
    10: 'reserved for wing in ground',
    11: 'power-driven vessel towing astern',
    12: 'power-driven vessel pushing ahead',
    14: 'AIS-SART / MOB / EPIRB',
    15: 'undefined',
}

@dataclass(frozen=True)
class Cols:
    """Resolved raw-column names for a source (from ingest.raw_columns)"""
    mmsi: str
    ts: str
    lat: str
    lon: str
    mobile: str

def coord_filter_sql(lat: str, lon: str) -> str:
    """SQL predicate keeping rows with valid, non-null WGS-84 coordinates"""
    return (
        f'"{lat}" BETWEEN -90 AND 90 '
        f'AND "{lon}" BETWEEN -180 AND 180 '
        f'AND "{lat}" IS NOT NULL '
        f'AND "{lon}" IS NOT NULL'
    )

def itu_to_type(code) -> str:
    """Map a Ship_type value (code or label) to an Archimedes type string"""
    if code is None:
        return 'Default'
    text_key = str(code).strip().lower()
    if text_key in TEXT_TO_ARCHIMEDES_TYPE:
        return TEXT_TO_ARCHIMEDES_TYPE[text_key]
    # Fall back to integer code lookup
    try:
        return ITU_TO_ARCHIMEDES_TYPE.get(int(code), 'Default')
    except (TypeError, ValueError):
        return 'Default'
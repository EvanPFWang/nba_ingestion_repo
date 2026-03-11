"""Normalization utilities for bball_index_scraper

Handles:
- Numeric value parsing and normalization
- Grade standardization
- Percentile conversion
- Missing value handling
"""

import re
from typing import Any, Optional, Set

#valid grade values
VALID_GRADES: Set[str] = {
    "A+", "A", "A-",
    "B+", "B", "B-",
    "C+", "C", "C-",
    "D+", "D", "D-",
    "F",
}

#patterns for numeric values
NUMERIC_PATTERN = re.compile(r"^-?\d+\.?\d*$")
PERCENTAGE_PATTERN = re.compile(r"^-?\d+\.?\d*%$")


def normalize_stat_value(value: Any) -> Optional[float]:
    """Normalize a stat value to float

    value: Raw value (string, int, float, or None)

    returns Float value or None if unparseable
    """
    if value is None:
        return None

    value_str = str(value).strip()

    #handle empty/missing markers
    if value_str in ["", "-", "—", "N/A", "null", "None"]:
        return None

    #remove percentage sign if present
    if value_str.endswith("%"):
        value_str = value_str[:-1]

    #try parsing as float
    try:
        return float(value_str)
    except ValueError:
        pass

    #try extracting first numeric value
    match = re.search(r"-?\d+\.?\d*", value_str)
    if match:
        try:
            return float(match.group())
        except ValueError:
            pass

    return None


def normalize_grade(grade: Any) -> Optional[str]:
    """Normalize grade to standard format

    grade: Raw grade value

    returns Standardized grade string (e.g., "A+", "B-") or None
    """
    if grade is None:
        return None

    grade_str = str(grade).strip().upper()

    #handle empty/missing
    if grade_str in ["", "-", "—", "N/A", "NULL", "NONE"]:
        return None

    #exact match
    if grade_str in VALID_GRADES:
        return grade_str

    #try extracting valid grade prefix
    for valid in sorted(VALID_GRADES, key=len, reverse=True):
        if grade_str.startswith(valid):
            return valid

    #single letter grades
    if len(grade_str) == 1 and grade_str in "ABCDF":
        return grade_str

    return None


def normalize_percentile(percentile: Any) -> Optional[float]:
    """Normalize percentile to decimal (0.0-1.0)

    percentile: Raw percentile value (e.g., "88%", "0.88", "88")

    returns Decimal percentile (0.0-1.0) or None
    """
    if percentile is None:
        return None

    pct_str = str(percentile).strip()

    #handle empty/missing
    if pct_str in ["", "-", "—", "N/A"]:
        return None

    #remove percentage sign
    if pct_str.endswith("%"):
        pct_str = pct_str[:-1]

    try:
        value = float(pct_str)

        #convert to decimal if > 1
        if value > 1:
            value = value / 100.0

        #clamp to valid range
        return max(0.0, min(1.0, value))

    except ValueError:
        return None


def normalize_player_name(name: Any) -> Optional[str]:
    """Normalize player name

    name: Raw player name

    returns Cleaned player name or None
    """
    if name is None:
        return None

    name_str = str(name).strip()

    if not name_str or name_str.lower() in ["unknown", "n/a"]:
        return None

    #normalize whitespace
    name_str = " ".join(name_str.split())

    #title case
    name_str = name_str.title()

    return name_str


def normalize_season(season: Any) -> Optional[str]:
    """Normalize season string to YYYY-YYYY format

    season: Raw season (e.g., "2024-25", "2024-2025", "24-25")

    returns Normalized season string (e.g., "2024-2025") or None
    """
    if season is None:
        return None

    season_str = str(season).strip()

    #match YYYY-YY or YYYY-YYYY patterns
    match = re.match(r"(\d{4})-(\d{2,4})", season_str)
    if match:
        start_year = int(match.group(1))
        end_part = match.group(2)

        if len(end_part) == 2:
            #convert 2-digit to 4-digit
            end_year = start_year + 1
        else:
            end_year = int(end_part)

        return f"{start_year}-{end_year}"

    #match YY-YY pattern
    match = re.match(r"(\d{2})-(\d{2})", season_str)
    if match:
        start_yy = int(match.group(1))
        end_yy = int(match.group(2))

        #assume 2000s if < 50, else 1900s
        century = 2000 if start_yy < 50 else 1900
        start_year = century + start_yy
        end_year = century + end_yy

        return f"{start_year}-{end_year}"

    return None


def normalize_stat_category(category: Any) -> str:
    """Normalize stat category name

    category: Raw category name

    returns Normalized category string
    """
    if category is None:
        return "General"

    cat_str = str(category).strip()

    if not cat_str:
        return "General"

    #normalize whitespace and casing
    cat_str = " ".join(cat_str.split())
    cat_str = cat_str.title()

    #common normalizations
    replacements = {
        "Post-Play": "Post Play",
        "Postplay": "Post Play",
        "Post Up": "Post Play",
        "3-Point": "Three Point",
        "3 Point": "Three Point",
        "3pt": "Three Point",
    }

    for old, new in replacements.items():
        if old.lower() in cat_str.lower():
            cat_str = cat_str.replace(old, new)

    return cat_str


def is_valid_stat(stat_name: Any, value: Any) -> bool:
    """Check if stat is valid (not a header/label row)

    stat_name: Statistic name
    value: Stat value

    returns True if this looks like valid stat data
    """
    if stat_name is None:
        return False

    name_str = str(stat_name).strip().upper()

    #skip header-like rows
    invalid_names = {
        "STATISTIC", "STAT", "NAME", "METRIC", "VALUE",
        "PERCENTILE", "GRADE", "CATEGORY", "TYPE",
    }

    if name_str in invalid_names:
        return False

    #must have some value
    if value is None or str(value).strip() in ["", "-"]:
        return False

    return True

import datetime
from typing import Any


def parse_wmi_date(val: Any, fmt: str = '%Y%m%d') -> int | None:
    if not val:
        return None
    try:
        val = int(datetime.datetime.strptime(val, fmt).timestamp())
        if val <= 0:
            return None
        return val
    except Exception:
        return None

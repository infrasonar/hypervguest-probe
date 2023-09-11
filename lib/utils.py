import datetime
from typing import Optional, Union


def parse_wmi_date(val, fmt: Optional[str] = '%Y%m%d') -> Union[int, None]:
    if not val:
        return None
    try:
        val = int(datetime.datetime.strptime(val, fmt).timestamp())
        if val <= 0:
            return None
        return val
    except Exception:
        return None

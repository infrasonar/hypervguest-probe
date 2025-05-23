import asyncio
import datetime
import logging
from libprobe.asset import Asset
from libprobe.exceptions import (
    CheckException,
    IgnoreCheckException,
    IgnoreResultException)
from aiowmi.query import Query
from aiowmi.connection import Connection
from aiowmi.connection import Protocol as Service
from aiowmi.exceptions import WbemExInvalidClass, WbemExInvalidNamespace
from typing import List, Tuple, Dict, Optional


DTYPS_NOT_NULL = {
    int: 0,
    bool: False,
    float: 0.,
    list: [],
}
QUERY_TIMEOUT = 120


async def wmiconn(
        asset: Asset,
        asset_config: dict,
        check_config: dict) -> Tuple[Connection, Service]:
    hypervisor = check_config.get('hypervisor')
    if hypervisor is None:
        msg = 'missing hypervisor in collector configuration'
        raise CheckException(msg)
    username = asset_config.get('username')
    password = asset_config.get('password')
    if username is None or password is None:
        logging.error(f'missing credentials for {asset}')
        raise IgnoreResultException

    if '\\' in username:
        # Replace double back-slash with single if required
        username = username.replace('\\\\', '\\')
        domain, username = username.split('\\')
    elif '@' in username:
        username, domain = username.split('@')
    else:
        domain = ''

    conn = Connection(hypervisor, username, password, domain)
    service = None

    try:
        await conn.connect()
    except Exception as e:
        error_msg = str(e) or type(e).__name__
        raise CheckException(f'unable to connect: {error_msg}')

    try:
        service = await conn.negotiate_ntlm()
    except Exception as e:
        conn.close()
        error_msg = str(e) or type(e).__name__
        raise CheckException(f'unable to authenticate: {error_msg}')

    return conn, service


async def wmiquery(
        conn: Connection,
        service: Service,
        query: Query,
        refs: Optional[dict] = None,
        timeout: int = QUERY_TIMEOUT) -> List[dict]:
    rows = []

    try:
        async with query.context(conn, service, timeout=timeout) as qc:
            async for props in qc.results():  # type: ignore
                row = {}
                for name, prop in props.items():
                    if refs and name in refs and prop.is_reference():
                        await refs[name](conn, service, prop, row)
                    elif prop.value is None:
                        row[name] = DTYPS_NOT_NULL.get(prop.get_type())
                    elif isinstance(prop.value, datetime.datetime):
                        row[name] = prop.value.timestamp()
                    elif isinstance(prop.value, datetime.timedelta):
                        row[name] = prop.value.seconds
                    else:
                        row[name] = prop.value
                rows.append(row)
    except (WbemExInvalidClass, WbemExInvalidNamespace):
        raise IgnoreCheckException
    except asyncio.TimeoutError:
        raise CheckException('WMI query timed out')
    except Exception as e:
        error_msg = str(e) or type(e).__name__
        # At this point log the exception as this can be useful for debugging
        # issues with WMI queries;
        logging.exception(f'query error: {error_msg};')
        raise CheckException(error_msg)
    return rows


def wmiclose(conn: Connection, service: Service):
    service.close()
    conn.close()

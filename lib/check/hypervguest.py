import logging
from aiowmi.query import Query
from libprobe.asset import Asset
from libprobe.exceptions import IgnoreResultException
from ..utils import parse_wmi_date
from ..wmiquery import wmiconn, wmiquery, wmiclose

COMMUNICATION_STATUS = {
    0: 'Unknown',
    1: 'Not Available',
    2: 'Communication OK',
    3: 'Lost Communication',
    4: 'No Contact',
}

DETAILED_STATUS = {
    0: 'Not Available',
    1: 'No Additional Information',
    2: 'Stressed',
    3: 'Predictive Failure',
    4: 'Non-Recoverable Error',
    5: 'Supporting Entity in Error',
}

DETAILED_STATUS = {
    0: 'Not Available',
    1: 'No Additional Information',
    2: 'Stressed',
    3: 'Predictive Failure',
    4: 'Non-Recoverable Error',
    5: 'Supporting Entity in Error',
}

ENABLED_DEFAULT = {
    2: 'Enabled',
    3: 'Disabled',
    6: 'Enabled but Offline'
}

ENABLED_STATE = {
    0: 'Unknown',
    1: 'Other',
    2: 'Enabled',
    3: 'Disabled',
    4: 'Shutting Down',
    5: 'Not Applicable',
    6: 'Enabled but Offline',
    7: 'In Test',
    8: 'Deferred',
    9: 'Quiesce',
    10: 'Starting',
}

ENHANCED_SESSION_MODE_STATE = {
    2: 'Allowed and available',
    3: 'Not allowed',
    6: 'Allowed but not available',
}

# The EnabledState property can also contain more information. For example,
# when disk space is critically low, HealthState is set to 25, the virtual
# machine pauses, and EnabledState is set to 32768 (Paused).
HEALTH_STATE = {
    5: 'OK',
    20: 'Major Failure',
    25: 'Critical failure',
}

OPERATING_STATUS = {
    0: 'Unknown',
    1: 'Not Available',
    2: 'Servicing',
    3: 'Starting',
    4: 'Stopping',
    5: 'Stopped',
    6: 'Aborted',
    7: 'Dormant',
    8: 'Completed',
    9: 'Migrating',
    10: 'Emigrating',
    11: 'Immigrating',
    12: 'Snapshotting',
    13: 'Shutting Down',
    14: 'In Test',
    15: 'Transitioning',
    16: 'In Service',
}

OPERATIONAL_STATUS = {
    2: 'OK',
    3: 'Degraded',
    5: 'Predictive Failure',
    10: 'Stopped',
    11: 'In Service',
    15: 'Dormant',
}

OPERATIONAL_STATUS_MORE = {
    32768: 'Creating Snapshot',
    32769: 'Applying Snapshot',
    32770: 'Deleting Snapshot',
    32771: 'Waiting to Start',
    32772: 'Merging Disks',
    32773: 'Exporting Virtual Machine',
    32774: 'Migrating Virtual Machine',
}

PRIMARY_STATUS = {
    0: 'Unknown',
    1: 'OK',
    2: 'Degraded',
    3: 'Error',
}

REPLICATION_MODE = {
    0: 'None',
    1: 'Primary',
    2: 'Replica',
    3: 'Test Replica',
    4: 'Extended Replica',
}

REQUESTED_STATE = {
    **ENABLED_STATE,
    12: 'Not Applicable',
}


TYPE_NAME = "guest"


async def check_hypervguest(
        asset: Asset,
        asset_config: dict,
        config: dict) -> dict:
    conn, service = await wmiconn(asset, asset_config, config)

    guid = config.get('guid')
    if guid is None:
        logging.error(f'missing guid for {asset}')
        raise IgnoreResultException
    query = Query(f"""
        SELECT
            InstanceID, ElementName, InstallDate, Name,
            OperationalStatus, Status, HealthState,
            CommunicationStatus, DetailedStatus, OperatingStatus,
            PrimaryStatus, EnabledState, OtherEnabledState, RequestedState,
            EnabledDefault, TimeOfLastStateChange,
            OnTimeInMilliseconds, ProcessID, TimeOfLastConfigurationChange,
            NumberOfNumaNodes, ReplicationMode,
            LastSuccessfulBackupTime, EnhancedSessionModeState,
            HwThreadsPerCoreRealized
        FROM Msvm_ComputerSystem WHERE Name = '{guid}'
    """, namespace=r'root\virtualization\v2')
    try:
        rows = await wmiquery(conn, service, query)
        assert len(rows), 'vm not not found'
    finally:
        wmiclose(conn, service)

    for row in rows:
        row['name'] = row.pop('Name')
        row['CommunicationStatus'] = COMMUNICATION_STATUS.get(
            row['CommunicationStatus'])
        row['DetailedStatus'] = DETAILED_STATUS.get(row['DetailedStatus'])
        row['EnabledDefault'] = ENABLED_DEFAULT.get(row['EnabledDefault'])
        enabled_state = row['EnabledState']
        other_enabled_state = row.pop('OtherEnabledState')
        row['EnabledState'] = ENABLED_STATE.get(enabled_state)
        if enabled_state == 1 and isinstance(other_enabled_state, str):
            row['EnabledState'] == row['OtherEnabledState']
        row['EnhancedSessionModeState'] = ENHANCED_SESSION_MODE_STATE.get(
            row['EnhancedSessionModeState'])
        row['HealthState'] = HEALTH_STATE.get(row['HealthState'])
        row['InstallDate'] = parse_wmi_date(row['InstallDate'])
        status = row.pop('OperationalStatus')
        row['OperationalStatus'] = OPERATIONAL_STATUS.get(status[0]) \
            if len(status) else None
        row['OperationalStatusMore'] = OPERATIONAL_STATUS_MORE.get(status[1]) \
            if len(status) > 1 else None
        row['OperatingStatus'] = OPERATING_STATUS.get(row['OperatingStatus'])
        row['PrimaryStatus'] = PRIMARY_STATUS.get(row['PrimaryStatus'])
        row['ReplicationMode'] = REPLICATION_MODE.get(row['ReplicationMode'])
        row['RequestedState'] = REQUESTED_STATE.get(row['RequestedState'])
        row['TimeOfLastStateChange'] = \
            int(row['TimeOfLastStateChange'])
        row['TimeOfLastConfigurationChange'] = \
            int(row['TimeOfLastConfigurationChange'])
        row['LastSuccessfulBackupTime'] = \
            parse_wmi_date(row['LastSuccessfulBackupTime'])

    return {
        TYPE_NAME: rows
    }

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

FAILED_OVER_REPLICATION_TYPE = {
    0: 'None',
    1: 'Regular ',
    2: 'Application consistent',
    3: 'Planned',
}

# The EnabledState property can also contain more information. For example,
# when disk space is critically low, HealthState is set to 25, the virtual
# machine pauses, and EnabledState is set to 32768 (Paused).
HEALTH_STATE = {
    5: 'OK',
    20: 'Major Failure',
    25: 'Critical failure',
}

LAST_REPLICATION_TYPE = {
    0: 'None',
    1: 'Regular',
    2: 'Application consistent',
    3: 'Planned',
}

OPERATION_STATUS = {
    2: 'OK',
    3: 'Degraded',
    5: 'Predictive Failure',
    10: 'Stopped',
    11: 'In Service',
    15: 'Dormant',
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

REPLICATION_HEALTH = {
    0: 'Not applicable',
    1: 'Ok',
    2: 'Warning',
    3: 'Critical',
}

REPLICATION_MODE = {
    0: 'None',
    1: 'Primary',
    2: 'Replica',
    3: 'Test Replica',
    4: 'Extended Replica',
}

REPLICATION_STATE = {
    0: 'Disabled',
    1: 'Ready for replication',
    2: 'Waiting to complete initial replication',
    3: 'Replicating',
    4: 'Synced replication complete',
    5: 'Recovered',
    6: 'Committed',
    7: 'Suspended',
    8: 'Critical',
    9: 'Waiting to start resynchronization',
    10: 'Resynchronizing',
    11: 'Resynchronization suspended',
    12: 'Failover in progress',
    13: 'Failback in progress',
    14: 'Failback complete',
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

    guuid = config.get('guuid')
    if guuid is None:
        logging.error(f'missing guuid for {asset}')
        raise IgnoreResultException
    query = Query(f"""
        SELECT
            InstanceID, Caption, Description, ElementName, InstallDate, Name,
            OperationalStatus, Status, HealthState,
            CommunicationStatus, DetailedStatus, OperatingStatus,
            PrimaryStatus, EnabledState, OtherEnabledState, RequestedState,
            EnabledDefault, TimeOfLastStateChange,
            OnTimeInMilliseconds,ProcessID, TimeOfLastConfigurationChange,
            NumberOfNumaNodes, ReplicationState, ReplicationHealth,
            ReplicationMode, FailedOverReplicationType, LastReplicationType,
            LastApplicationConsistentReplicationTime, LastReplicationTime,
            LastSuccessfulBackupTime, EnhancedSessionModeState,
            HwThreadsPerCoreRealized
        FROM Msvm_ComputerSystem WHERE Name = '{guuid}'
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
        row['FailedOverReplicationType'] = FAILED_OVER_REPLICATION_TYPE.get(
            row['FailedOverReplicationType'])
        row['HealthState'] = HEALTH_STATE.get(row['HealthState'])
        row['InstallDate'] = parse_wmi_date(row['InstallDate'])
        row['LastReplicationType'] = LAST_REPLICATION_TYPE.get(
            row['LastReplicationType'])
        status = row.pop('OperationalStatus')
        row['OperationalStatus'] = OPERATIONAL_STATUS.get(status[0]) \
            if len(status) else None
        row['OperationalStatusMore'] = OPERATIONAL_STATUS_MORE.get(status[1]) \
            if len(status) > 1 else None
        row['OperatingStatus'] = OPERATION_STATUS.get(row['OperatingStatus'])
        row['PrimaryStatus'] = PRIMARY_STATUS.get(row['PrimaryStatus'])
        row['ReplicationHealth'] = REPLICATION_HEALTH.get(
            row['ReplicationHealth'])
        row['ReplicationMode'] = REPLICATION_MODE.get(row['ReplicationMode'])
        row['ReplicationState'] = REPLICATION_STATE.get(
            row['ReplicationState'])
        row['RequestedState'] = REQUESTED_STATE.get(row['RequestedState'])
        row['TimeOfLastStateChange'] = \
            parse_wmi_date(row['TimeOfLastStateChange'])
        row['TimeOfLastConfigurationChange'] = \
            parse_wmi_date(row['TimeOfLastConfigurationChange'])
        row['LastApplicationConsistentReplicationTime'] = \
            parse_wmi_date(row['LastApplicationConsistentReplicationTime'])
        row['LastReplicationTime'] = parse_wmi_date(row['LastReplicationTime'])
        row['LastSuccessfulBackupTime'] = \
            parse_wmi_date(row['LastSuccessfulBackupTime'])

    return {
        TYPE_NAME: rows
    }

# This is a sample Python script.

# Press ⌃R to execute it or replace it with your code.
# Press Double ⇧ to search everywhere for classes, files, tool windows, actions, and settings.
import decimal
import sys
import csv
import boto3
import datetime
from dataclasses import dataclass
from typing import *

@dataclass
class LambdaMetrics:
    time: datetime.datetime
    invocations: int
    avg_duration: float


@dataclass
class LambdaFunctionInfo:
    function_name: str
    memory_size: int


@dataclass
class DynamoDbTableMetrics:
    time: datetime.datetime
    avg_prov_rcu: float
    avg_prov_wcu: float
    sum_consumed_rcu: int
    sum_consumed_wcu: int


@dataclass
class DynamoDbTableInfo:
    table_name: str
    is_ondemand: bool


def convert_lambda_function_info(lambda_function_info) -> LambdaFunctionInfo:
    return LambdaFunctionInfo(lambda_function_info['FunctionName'], lambda_function_info['MemorySize'])


def convert_lambda_function_info_list(lambda_function_info_list) -> List[LambdaFunctionInfo]:
    ret = []
    for lambda_function_info in lambda_function_info_list:
        ret.append(convert_lambda_function_info(lambda_function_info))
    return ret


def get_lambda_info_list(session: boto3.Session) -> List[LambdaFunctionInfo]:
    client = session.client('lambda')

    res = client.list_functions()
    marker = res.get('NextMarker')
    function_list = convert_lambda_function_info_list(res['Functions'])
    while marker:
        res = client.list_functions(Marker=marker)
        marker = res.get('NextMarker')
        function_list.extend(convert_lambda_function_info_list(res['Functions']))

    return function_list


def get_dynamodb_table_info_list(session: boto3.Session) -> List[DynamoDbTableInfo]:
    client = session.client('dynamodb')

    res = client.list_tables()
    marker = res.get('LastEvaluatedTableName')
    table_name_list = res['TableNames']
    while marker:
        res = client.list_tables(ExclusiveStartTableName=marker)
        marker = res.get('LastEvaluatedTableName')
        table_name_list.extend(res['TableNames'])

    ret = []
    for table_name in table_name_list:
        res = client.describe_table(
            TableName=table_name
        )
        ret.append(DynamoDbTableInfo(table_name, False))

    return ret


def get_lambda_cw_metrics(cw_client, start_time: datetime.datetime, end_time: datetime.datetime, period: int = 3600)\
        -> Dict[str, List[LambdaMetrics]]:
    list_response = cw_client.list_metrics(
        Namespace='AWS/Lambda',
        MetricName='Invocations'
    )
    func_name_set = set()
    for m in list_response['Metrics']:
        if len(m['Dimensions']) > 0:
            for d in m['Dimensions']:
                if d['Name'] == 'FunctionName':
                    func_name_set.add(d['Value'])

    ret = {}

    for func_name in func_name_set:
        metrics_invocations_response = cw_client.get_metric_statistics(
            Namespace='AWS/Lambda',
            MetricName='Invocations',
            StartTime=start_time,
            EndTime=end_time,
            Dimensions=[
                { 'Name': 'FunctionName', 'Value': func_name }
            ],
            Period=period,
            Statistics=['Sum']
        )

        metrics_duration_response = cw_client.get_metric_statistics(
            Namespace='AWS/Lambda',
            MetricName='Duration',
            StartTime=start_time,
            EndTime=end_time,
            Dimensions=[
                {'Name': 'FunctionName', 'Value': func_name}
            ],
            Period=period,
            Statistics=['Average']
        )
        metrics_list: List[LambdaMetrics] = []
        for i in range(len(metrics_invocations_response['Datapoints'])):
            invocations_dp = metrics_invocations_response['Datapoints'][i]
            duration_dp = metrics_duration_response['Datapoints'][i]
            metrics = LambdaMetrics(invocations_dp['Timestamp'], int(invocations_dp['Sum']), duration_dp['Average'])
            metrics_list.append(metrics)
        ret[func_name] = metrics_list
    return ret


def get_dynamodb_cw_metrics(cw_client, start_time: datetime.datetime, end_time: datetime.datetime, period: int = 3600)\
        -> Dict[str, List[DynamoDbTableMetrics]]:
    list_response = cw_client.list_metrics(
        Namespace='AWS/DynamoDB',
        MetricName='ConsumedWriteCapacityUnits'
    )
    table_name_set = set()
    for m in list_response['Metrics']:
        if len(m['Dimensions']) > 0:
            for d in m['Dimensions']:
                if d['Name'] == 'TableName':
                    table_name_set.add(d['Value'])

    ret = {}
    for table_name in table_name_set:
        metrics_consumed_wcu_response = cw_client.get_metric_statistics(
            Namespace='AWS/DynamoDB',
            MetricName='ConsumedWriteCapacityUnits',
            StartTime=start_time,
            EndTime=end_time,
            Dimensions=[
                {'Name': 'TableName', 'Value': table_name}
            ],
            Period=period,
            Statistics=['Sum']
        )

        metrics_consumed_rcu_response = cw_client.get_metric_statistics(
            Namespace='AWS/DynamoDB',
            MetricName='ConsumedReadCapacityUnits',
            StartTime=start_time,
            EndTime=end_time,
            Dimensions=[
                {'Name': 'TableName', 'Value': table_name}
            ],
            Period=period,
            Statistics=['Sum']
        )

        metrics_provisioned_wcu_response = cw_client.get_metric_statistics(
            Namespace='AWS/DynamoDB',
            MetricName='ProvisionedWriteCapacityUnits',
            StartTime=start_time,
            EndTime=end_time,
            Dimensions=[
                {'Name': 'TableName', 'Value': table_name}
            ],
            Period=period,
            Statistics=['Average']
        )

        metrics_provisioned_rcu_response = cw_client.get_metric_statistics(
            Namespace='AWS/DynamoDB',
            MetricName='ProvisionedReadCapacityUnits',
            StartTime=start_time,
            EndTime=end_time,
            Dimensions=[
                {'Name': 'TableName', 'Value': table_name}
            ],
            Period=period,
            Statistics=['Average']
        )

        metrics_list: List[DynamoDbTableMetrics] = []
        for i in range(len(metrics_consumed_wcu_response['Datapoints'])):
            consumed_wcu_dp = metrics_consumed_wcu_response['Datapoints'][i]
            consumed_rcu_dp = metrics_consumed_rcu_response['Datapoints'][i]

            if len(metrics_provisioned_wcu_response['Datapoints']) > 0:
                provisioned_wcu_dp = metrics_provisioned_wcu_response['Datapoints'][i]
            else:
                provisioned_wcu_dp = {'Average': 0}

            if len(metrics_provisioned_rcu_response['Datapoints']) > 0:
                provisioned_rcu_dp = metrics_provisioned_rcu_response['Datapoints'][i]
            else:
                provisioned_rcu_dp = {'Average': 0}

            metrics = DynamoDbTableMetrics(consumed_wcu_dp['Timestamp'], int(consumed_wcu_dp['Sum']),
                                           int(consumed_rcu_dp['Sum']),
                                           int(provisioned_wcu_dp['Average']),
                                           int(provisioned_rcu_dp['Average']))
            metrics_list.append(metrics)
        ret[table_name] = metrics_list
    return ret


@dataclass
class LambdaCostInfo:
    cost_per_1m_invocations: decimal.Decimal
    cost_per_gb_x86: decimal.Decimal
    cost_per_gb_arm: decimal.Decimal

LAMBDA_COST_INFO_PER_REGION = {
    'ap-northeast-1': LambdaCostInfo(decimal.Decimal('0.20'), decimal.Decimal('0.0000166667'), decimal.Decimal('0.0000133334'))
}

@dataclass()
class LambdaCostDetail:
    timestamp: datetime
    period: int
    cost: decimal.Decimal
    memory_size: int
    invocations: int
    avg_duration: float


def get_lambda_cost(session: boto3.Session, start_time: datetime.datetime, end_time: datetime.datetime, period: int):

    lambda_function_list = get_lambda_info_list(session)

    cw_client = session.client('cloudwatch')
    cw_metrics_map = get_lambda_cw_metrics(cw_client, start_time, end_time, period)
    ret = {}
    for func_info in lambda_function_list:
        cost_list = []
        cw_metrics = cw_metrics_map.get(func_info.function_name)
        if cw_metrics:
            for m in cw_metrics:
                cost_info = LAMBDA_COST_INFO_PER_REGION['ap-northeast-1']
                cost = cost_info.cost_per_1m_invocations * m.invocations / decimal.Decimal(1000000)
                + cost_info.cost_per_gb_x86 * func_info.memory_size * decimal.Decimal(m.avg_duration) * m.invocations / 1024

                cost_list.append(LambdaCostDetail(m.time, period, cost, func_info.memory_size, m.invocations, m.avg_duration))
        ret[func_info.function_name] = cost_list
    return ret


@dataclass
class DynamoDBCostInfo:
    cost_per_ondemand_wru_per_1m: decimal.Decimal
    cost_per_ondemand_rru_per_1m: decimal.Decimal
    cost_per_provisioned_wcu: decimal.Decimal
    cost_per_provisioned_rcu: decimal.Decimal

DYNAMO_DB_COST_INFO_PER_REGION = {
    'ap-northeast-1': DynamoDBCostInfo(decimal.Decimal('1.4269'), decimal.Decimal('0.285'),
                                       decimal.Decimal('0.0007424'), decimal.Decimal('0.0001484'))
}


@dataclass()
class DynamoDbTableCostDetail:
    timestamp: datetime
    period: int
    cost: decimal.Decimal
    w_cost: decimal.Decimal
    r_cost: decimal.Decimal


def get_dynamodb_cost(session: boto3.Session, start_time: datetime.datetime, end_time: datetime.datetime, period: int):

    dynamodb_table_list = get_dynamodb_table_info_list(session)

    cw_client = session.client('cloudwatch')
    cw_metrics_map = get_dynamodb_cw_metrics(cw_client, start_time, end_time, period)
    ret = {}
    for table_info in dynamodb_table_list:
        cost_list = []
        cw_metrics = cw_metrics_map.get(table_info.table_name)
        if cw_metrics:
            for m in cw_metrics:
                cost_info = DYNAMO_DB_COST_INFO_PER_REGION['ap-northeast-1']
                if m.avg_prov_rcu == 0 and m.avg_prov_wcu == 0:
                    w_cost = cost_info.cost_per_ondemand_wru_per_1m * decimal.Decimal(m.sum_consumed_wcu) * period / 1000000
                    r_cost = cost_info.cost_per_ondemand_rru_per_1m * decimal.Decimal(m.sum_consumed_rcu) * period / 1000000
                else:
                    w_cost = cost_info.cost_per_provisioned_wcu * decimal.Decimal(m.avg_prov_wcu) * period / 3600
                    r_cost = cost_info.cost_per_provisioned_rcu * decimal.Decimal(m.avg_prov_rcu) * period / 3600
                cost = w_cost + r_cost
                cost_list.append(DynamoDbTableCostDetail(m.time, period, cost, w_cost, r_cost))
        ret[table_info.table_name] = cost_list
    return ret


def export_lambda_cost_to_csv(filename: str, session: boto3.Session,
                              start_time: datetime.datetime, end_time: datetime.datetime, period: int) -> None:

    lambda_cost = get_lambda_cost(session, start_time, end_time, period)

    headers = ['function_name', 'timestamp', 'period', 'memory_size', 'invocations', 'avg_duration', 'cost']

    with open(filename, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        for function_name, cost_list in lambda_cost.items():
            for cost_dp in cost_list:
                row = [function_name, cost_dp.timestamp, cost_dp.period, cost_dp.memory_size,
                       cost_dp.invocations, cost_dp.avg_duration, cost_dp.cost]
                writer.writerow(row)
    pass


def export_dynamodb_cost_to_csv(filename: str, session: boto3.Session,
                              start_time: datetime.datetime, end_time: datetime.datetime, period: int) -> None:

    dynamodb_cost = get_dynamodb_cost(session, start_time, end_time, period)

    headers = ['table_name', 'timestamp', 'period', 'w_cost', 'r_cost', 'cost']

    with open(filename, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        for table_name, cost_list in dynamodb_cost.items():
            for cost_dp in cost_list:
                row = [table_name, cost_dp.timestamp, cost_dp.period,
                       cost_dp.w_cost, cost_dp.r_cost, cost_dp.cost]
                writer.writerow(row)
    pass


def main(args: List[str]) -> int:
    profile_name = args[0]
    region_name = args[1]
    start_time = datetime.datetime.fromisoformat(args[2])
    end_time = datetime.datetime.fromisoformat(args[3])
    period = int(args[4])

    session = boto3.Session(profile_name=profile_name, region_name=region_name)

    export_lambda_cost_to_csv('lambda_cost.csv', session, start_time, end_time, period)
    export_dynamodb_cost_to_csv('dynamodb_cost.csv', session, start_time, end_time, period)

    return 0


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    #argv = sys.argv
    s_end_time = datetime.datetime.utcnow()
    s_start_time = s_end_time - datetime.timedelta(days=30)
    s_period = 60 * 60 * 24
    argv = ['default', 'ap-northeast-1', str(s_start_time.isoformat()), str(s_end_time.isoformat()), str(s_period)]
    exit_code = main(argv)
    sys.exit(exit_code)

# See PyCharm help at https://www.jetbrains.com/help/pycharm/

from typing import List

from dagster import (
    In,
    Nothing,
    Out,
    ResourceDefinition,
    RetryPolicy,
    RunRequest,
    ScheduleDefinition,
    SkipReason,
    graph,
    op,
    schedule,
    sensor,
    static_partitioned_config,
)
from workspaces.project.sensors import get_s3_keys
from workspaces.resources import mock_s3_resource, redis_resource, s3_resource
from workspaces.types import Aggregation, Stock


@op(required_resource_keys={'s3'},config_schema={'s3_key':str})
def get_s3_data(context):
    return [Stock.from_list(i) for i in context.resources.s3.get_data(context.op_config['s3_key'])]



@op
def process_data(context, stock_list)->Aggregation:
    max_high = None
    for stock in stock_list:
        if max_high is None or stock.high > max_high:
            max_high=stock.high
            output = Aggregation(date=stock.date,high=stock.high)
    return output


@op(required_resource_keys={'redis'})
def put_redis_data(context,agg):
    name = agg.date.strftime('%Y-%m-%d')
    value = str(agg.high)
    return context.resources.redis.put_data(name,value)


@op(required_resource_keys={'s3'})
def put_s3_data(context, agg: Aggregation):
    key_name = 'put_s3_data/agg.txt'
    return context.resources.s3.put_data(key_name,agg)



@graph
def week_3_pipeline():
    data = get_s3_data()
    agg = process_data(data)
    put_s3_data(agg)
    put_redis_data(agg)

local = {
    "ops": {"get_s3_data": {"config": {"s3_key": "prefix/stock_9.csv"}}},
}

docker = {
    "resources": {
        "s3": {
            "config": {
                "bucket": "dagster",
                "access_key": "test",
                "secret_key": "test",
                "endpoint_url": "http://localstack:4566",
            }
        },
        "redis": {
            "config": {
                "host": "redis",
                "port": 6379,
            }
        },
    },
    "ops": {"get_s3_data": {"config": {"s3_key": "prefix/stock_9.csv"}}},
}

@static_partitioned_config(partition_keys=[str(i) for i in range(1,11)])
def docker_config(partition_key: str):
    return {
    "resources": {
        "s3": {
            "config": {
                "bucket": "dagster",
                "access_key": "test",
                "secret_key": "test",
                "endpoint_url": "http://localstack:4566",
            }
        },
        "redis": {
            "config": {
                "host": "redis",
                "port": 6379,
            }
        },
    },
    "ops": {"get_s3_data": {"config": {"s3_key": "prefix/stock_{}.csv".format(partition_key)}}},
}


week_3_pipeline_local = week_3_pipeline.to_job(
    name="week_3_pipeline_local",
    config=local, 
    resource_defs={"s3": mock_s3_resource, "redis": ResourceDefinition.mock_resource()},
)

week_3_pipeline_docker = week_3_pipeline.to_job(
    name="week_3_pipeline_docker",
    config=docker_config,
    resource_defs={"s3": s3_resource, "redis": redis_resource},
    op_retry_policy=RetryPolicy(max_retries=10,delay=1),

)


week_3_schedule_local = ScheduleDefinition(job=week_3_pipeline_local, cron_schedule="*/15 * * * *")

@schedule(cron_schedule="0 * * * *", job=week_3_pipeline_docker)
def week_3_schedule_docker():
    for mo in [str(i) for i in range(1,11)]:
        request = week_3_pipeline_docker.run_request_for_partition(partition_key=mo,run_key=mo)
        yield request

@sensor
def week_3_sensor_docker():
    new_files = get_s3_keys(bucket=None, prefix=None, endpoint_url=None,
    since_key=None)
    if len(new_files)==0:
        yield SkipReason("No new s3 files found in bucket")
    else:
        for s3_key in new_files:
            tmp_conf = docker.copy()
            tmp_conf['ops']['get_s3_data']['config']["s3_key"] = s3_key
            RunRequest(run_key=s3_key,run_config=tmp_conf)

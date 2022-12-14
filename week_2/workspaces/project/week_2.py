from typing import List

from dagster import In, Nothing, Out, ResourceDefinition, String, graph, op
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
def week_2_pipeline():
    data = get_s3_data()
    agg = process_data(data)
    put_s3_data(agg)
    put_redis_data(agg)

local = {
    "ops": {"get_s3_data": {"config": {"s3_key": "prefix/stock.csv"}}},
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
    "ops": {"get_s3_data": {"config": {"s3_key": "prefix/stock.csv"}}},
}

week_2_pipeline_local = week_2_pipeline.to_job(
    name="week_2_pipeline_local", 
    config=local, 
    resource_defs={"s3": mock_s3_resource, "redis": ResourceDefinition.mock_resource()},
)

week_2_pipeline_docker = week_2_pipeline.to_job(
    name="week_2_pipeline_docker",
    config=docker,
    resource_defs={"s3": s3_resource, "redis": redis_resource},
)

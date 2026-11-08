"""Runtime configuration: ECS secrets/environment, then Airflow Variables."""
import json
import os

from airflow.sdk import Variable


def setting(name, default=None):
    value = os.environ.get(name) or Variable.get(name, default=default)
    if value is None or value == "":
        raise ValueError(f"Missing required configuration: {name}")
    return value


def spark_configuration():
    network = {}
    for key, name in (("subnets", "ECS_SUBNET_IDS"), ("securityGroups", "ECS_SECURITY_GROUP_IDS")):
        values = json.loads(setting(name))
        prefix = "subnet-" if key == "subnets" else "sg-"
        if not isinstance(values, list) or not values or not all(isinstance(v, str) and v.startswith(prefix) for v in values):
            raise ValueError(f"{name} must be a nonempty JSON array of {prefix} identifiers")
        network[key] = values
    network["assignPublicIp"] = "DISABLED"
    return {
        "cluster": setting("ECS_CLUSTER_NAME", "vueladata-cluster"),
        "task_definition": setting("ECS_SPARK_TASK_DEFINITION", "vueladata-spark-etl"),
        "container": setting("ECS_SPARK_CONTAINER_NAME", "spark-etl"),
        "bucket": setting("S3_DATA_LAKE_BUCKET"),
        "network": {"awsvpcConfiguration": network},
    }

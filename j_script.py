import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME', 's3_bucket'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
def calculate_totals(record):
    record["total_price"] = round(record["quantity"] * record["price"], 2)
    record["total_weight"] = round(record["quantity"] * record["weight"], 2)
    return record
    
input_path = f"s3://{args['s3_bucket']}/data/"
products_frame = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    recurse=True,
    connection_options={"paths": [input_path]},
    format="json",
)

orders_frame = glueContext.create_dynamic_frame.from_options(
    connection_type="dynamodb",
    connection_options={
        "dynamodb.input.tableName": "orders",
        "dynamodb.throughput.read.percent": "1.0",
        "dynamodb.splits": "100",
    },
)

joined_frame = orders_frame.join(["product_id"], ["product_id"], products_frame)
transformed_frame = joined_frame.map(f=calculate_totals)

glueContext.write_dynamic_frame.from_options(
    frame=transformed_frame,
    connection_type="dynamodb",
    connection_options={
        "dynamodb.output.tableName": "orders-transformed",
        "dynamodb.throughput.write.percent": "1.0",
    },
)

job.commit()

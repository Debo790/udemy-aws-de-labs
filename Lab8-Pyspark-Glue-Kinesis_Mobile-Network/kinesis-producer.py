import json
import csv
import boto3
import logging
from io import StringIO
import os
from datetime import datetime

# Initialize logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()

# Specify AWS region
aws_region = "eu-west-1"


def process_csv_to_kinesis(file_name):
    
    kinesis_client = boto3.client('kinesis', region_name=aws_region)

    logger.info(f"Processing file: {file_name}")

    # Get the file from current working directory
    try:
        file_path = os.path.join(os.getcwd(), file_name)
        with open(file_path, 'r') as file:
            file_content = file.read()
    except Exception as e:
        logger.error(f"Error reading file {file_name} from current directory: {e}")
        raise e

    # Read the file content using csv.DictReader
    csv_data = StringIO(file_content)
    csv_reader = csv.DictReader(csv_data)

    counter = 0
    batch_size = 100
    for row in csv_reader:
        try:
            response = kinesis_client.put_record(
                StreamName="mobile_coverage_logs",
                Data=json.dumps(row),
                PartitionKey=str(hash(row['hour']))
            )
            counter += 1
            # Check response status
            if response['ResponseMetadata']['HTTPStatusCode'] != 200:
                logger.error('Error sending message to Kinesis:', response)

            # Log after every batch_size records
            if counter % batch_size == 0:
                logger.info(f"Processed {counter} records so far...")

        except Exception as e:
            logger.error(f"Error processing record {row}: {e}")

    logger.info(f"Finished processing. Total records sent: {counter}")
    return f"Processed {counter} records from {file_name}."


def process_json_to_kinesis(path="data/messages.json"):
    kinesis_client = boto3.client("kinesis", region_name=aws_region)
    logger.info(f"Processing JSON file: {path}")

    try:
        file_path = os.path.join(os.getcwd(), path)
        with open(file_path, "r") as file:
            records = json.load(file)
    except Exception as e:
        logger.error(f"Error reading JSON file {path}: {e}")
        raise

    if not isinstance(records, list):
        raise ValueError(f"{path} must contain a JSON array.")

    counter = 0
    batch_size = 10

    for record in records:
        try:
            timestamp_value = record.get("message").get("timestamp")
            if timestamp_value is None:
                raise ValueError("Missing 'timestamp' field")

            ts = str(timestamp_value).strip().replace("Z", "+00:00")

            try:
                dt = datetime.fromisoformat(ts)
            except ValueError:
                if "." in ts:
                    dt = datetime.strptime(ts, "%Y-%m-%dT%H:%M:%S.%f%z")
                else:
                    dt = datetime.strptime(ts, "%Y-%m-%dT%H:%M:%S%z")

            partition_key = dt.strftime("%H")

            content = record.get("message").get("content")
            content_obj = json.loads(content) if isinstance(content, str) else content
            response = kinesis_client.put_record(
                StreamName="presentation_entity_booking",
                Data=json.dumps(content_obj).encode("utf-8"),
                PartitionKey=partition_key
            )

            counter += 1
            if response["ResponseMetadata"]["HTTPStatusCode"] != 200:
                logger.error("Error sending message to Kinesis: %s", response)

            if counter % batch_size == 0:
                logger.info(f"Processed {counter} records so far...")

        except Exception as e:
            logger.error(f"Error processing record {record}: {e}")

    logger.info(f"Finished processing. Total records sent: {counter}")
    return f"Processed {counter} records from {path}."


if __name__ == "__main__":

    # file_name = "data/mobile-logs.csv"
    path = "data/messages.json"
    try:
        result = process_json_to_kinesis(path)
        print(result)
    except Exception as e:
        logger.error(f"Error processing file: {e}")
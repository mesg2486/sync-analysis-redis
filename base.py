import logging
import re
import sys
import boto3
import time
import uuid
import requests
import os
import shutil
import json
import traceback
import redis
import os
import csv

# Configure logging
logging.basicConfig(stream=sys.stdout, level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S %Z')

# SQS SETUP
aws_access_key_id = 'AKIATRKQV5SYQTJ7DBGF'
aws_secret_access_key = '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'
aws_region = 'us-east-2'

# SQS queue URL
on_off_queue_url = 'https://sqs.us-east-2.amazonaws.com/243371732145/sync-main.fifo'
meetings_queue_url = 'https://sqs.us-east-2.amazonaws.com/243371732145/MeetingsQueue.fifo'
cloudfront_base_url = 'https://d2n2ldezfv2tlg.cloudfront.net'

# S3 
s3_endpoint = 'https://s3.us-east-2.amazonaws.com'

# Initialize SQS client with AWS credentials
boto = boto3.Session(
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key,
    region_name=aws_region
)

# Initialize AWS Services with AWS credentials
sqs = boto.client('sqs')
dynamodb = boto.resource('dynamodb')
s3 = boto.client('s3', endpoint_url=s3_endpoint)

# Receive messages from the SQS queue
def fetch_sqs_messages(group):
    try:
        logging.info("Fetching messages from SQS queue...")
        response = sqs.receive_message(
            QueueUrl=meetings_queue_url,
            MaxNumberOfMessages=10,  # Adjust as needed
            WaitTimeSeconds=20,  # Poll every 20 seconds
            MessageAttributeNames=['All']  # Include all message attributes
        )
        messages = response.get('Messages', [])
        logging.info(f"Found {len(messages)} messages")
        # logging.info(str(messages))
        # Filter messages by the specified group
        filtered_messages = []
        for message in messages:
            body = json.loads(message.get('Body', '{}'))
            if body.get('type') == group:
                filtered_messages.append(message)
        
        if filtered_messages:
            logging.info(f"Found {len(filtered_messages)} messages for group: {group}")
            for message in filtered_messages:
                logging.info(f"Message: {message}")
        else:
            logging.info(f"No messages found for group: {group}")
        
        return filtered_messages
    
    except Exception as e:
        logging.error(f"Error fetching messages: {e}")
        return []

# Delete message from SQS queue
def delete_message_from_queue(message):
    try:
        # Delete the message from the queue
        sqs.delete_message(
            QueueUrl=meetings_queue_url,
            ReceiptHandle=message['ReceiptHandle']
        )
        logging.info("Message deleted from the queue.")
    except Exception as e:
        logging.error(f"Error deleting message from the queue: {e}")

# Stop instance request SQS entry
def request_stop_instance(group):
    try:
        # Send a request to stop the instance
        logging.info(f"Requesting stop of {group}")

        message_body = {
            "action": "stop",
            "group": group
        }

        # Send the message to the on_off_queue
        response = sqs.send_message(
            QueueUrl=on_off_queue_url,
            MessageBody=str(message_body),
            MessageGroupId='stop-' + group,  # Ensuring messages are grouped appropriately
            MessageDeduplicationId='stop-' + group + '-' + str(int(time.time()))  # Unique ID to avoid duplication
        )

        logging.info(f"Stop request sent to {on_off_queue_url} with response: {response}")

    except Exception as e:
        logging.error(f"Error requesting stop of instance: {e}")

# Download videos from s3
def download_video(resource_path, base_cache_dir, bucket_name='sync5'):
    try:
        logging.info(f"downloading... {resource_path}")
        start = time.time()

        # init cache dir
        if os.path.exists(base_cache_dir):
            if os.path.isdir(base_cache_dir):
                shutil.rmtree(base_cache_dir, ignore_errors=True)
            elif os.path.isfile(base_cache_dir):
                os.remove(base_cache_dir)
        cache_dir = os.path.join(base_cache_dir)

        os.makedirs(cache_dir)

        # Generate a unique file name
        file_name = uuid.uuid4().hex
        cached_path = os.path.join(cache_dir, f'{file_name}.mp4')

        # Construct the full URL to the resource on CloudFront
        url = f"{cloudfront_base_url}/{resource_path}"

        # Download the file from CloudFront
        response = requests.get(url, stream=True)
        response.raise_for_status()  # Raise an HTTPError for bad responses

        # Write the downloaded content to a local file
        with open(cached_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)

        finish = time.time()
        logging.info(f"Downloaded in {finish - start}s")
        return file_name

    except: 
        log = traceback.format_exc()
        logging.error(log)

# Download resource from s3 
def download_resource(key, filename, bucket_name='syneurgy-prod'):
    logging.info('downloading')
    start = time.time()

    # Download the file from S3
    s3.download_file(bucket_name, key, filename)

    finish = time.time()
    print('downloaded in ', finish - start, 's')
    return filename

# Upload resource to s3
def upload_resource(filename, key, bucket_name='syneurgy-prod'):
    logging.info('uploading')
    start = time.time()

    # Upload the file to S3
    s3.upload_file(filename, bucket_name, key)

    finish = time.time()
    print('uploaded in ', finish - start, 's')
    return filename


################## REDIS VERSION UPDATES

# RETURN THE S3 INSTANCE
def get_s3_instance():
    # init s3
    s3_instance = boto3.client(service_name='s3', region_name='us-west-1', endpoint_url='https://s3.us-west-1.amazonaws.com', aws_access_key_id='AKIATRKQV5SYSXUS7FGQ', aws_secret_access_key='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')
    return s3_instance


# DOWNLOAD RAW VIDEO FROM S3
def download_s3_resource(resource_path, base_cache_dir, bucket_name='sync5'):
    try:
        logging.info(f"downloading... {resource_path}")
        start = time.time()

        # initialize s3 and download file in cache dir
        s3_instance = get_s3_instance()

        # init s3 cache dir
        if os.path.exists(base_cache_dir):
            if os.path.isdir(base_cache_dir):
                shutil.rmtree(base_cache_dir, ignore_errors=True)
            elif os.path.isfile(base_cache_dir):
                os.remove(base_cache_dir)
        cache_dir = os.path.join(base_cache_dir)

        os.makedirs(cache_dir)
        file_name = uuid.uuid4().hex
        cached_path = os.path.join(cache_dir, '{}.mp4'.format(file_name))
        s3_instance.download_file(bucket_name, resource_path, cached_path)

        finish = time.time()
        logging.info(f"downloaded in {finish - start}s")
        return (cached_path, file_name)
    except: 
        log = traceback.format_exc()
        logging.error(log)

def convert_speaker_labels(input_path, output_path):
    """
    Converts speaker labels in a CSV file from 'speaker_XX' to 'userXX'.
    
    Args:
        input_path (str): Path to the input CSV file.
        output_path (str): Path to save the updated CSV file.
    """
    try:
        with open(input_path, 'r') as infile, open(output_path, 'w', newline='') as outfile:
            reader = csv.reader(infile)
            writer = csv.writer(outfile)

            headers = next(reader)  # Read the header row
            writer.writerow(headers)  # Write the header row

            for row in reader:
                if 'Speaker' in headers:
                    speaker_idx = headers.index('Speaker')
                    row[speaker_idx] = row[speaker_idx].replace('speaker_', 'user')
                writer.writerow(row)

        logging.info(f"Converted speaker labels and saved to {output_path}.")
    except Exception as e:
        logging.error(f"Error converting speaker labels for {input_path}: {e}")
        logging.error(traceback.format_exc())

def upload_selected_files_to_endpoint(meeting_id, data_folder):
    try:
        # Extract the numeric ID from the meeting ID
        id_ = re.findall(r'\d+', meeting_id)[0]
        base_url = 'http://54.215.45.218:8080/s3/uploadByMeeting/'
        url = f"{base_url}{id_}"

        # Allowed file extensions
        allowed_extensions = {'.txt', '.jpg', '.csv', '.json'}

        # Mapping for renaming files during upload
        rename_map = {
            'transcription.txt': 'ASR.txt',
            'dialogue.txt': 'nlp_result.txt',
            'diarization.txt': 'SpeakerDiarization.txt',
            'emotion.txt': 'EmotionText.txt',
        }

        # Files to exclude
        excluded_files = {'output.json', 'wav'}

        logging.info(f"Uploading files to endpoint {url}...")

        # Helper function to convert speaker labels in CSV files
        def convert_speaker_labels(file_path):
            temp_file = file_path + '.tmp'
            with open(file_path, 'r') as infile, open(temp_file, 'w', newline='') as outfile:
                reader = csv.reader(infile)
                writer = csv.writer(outfile)

                header = next(reader)
                new_header = [col.replace('speaker_', 'user') for col in header]
                writer.writerow(new_header)

                for row in reader:
                    writer.writerow(row)

            os.replace(temp_file, file_path)

        # Helper function to convert user headers in specific CSV files
        def convert_user_headers(file_path):
            temp_file = file_path + '.tmp'
            with open(file_path, 'r') as infile, open(temp_file, 'w', newline='') as outfile:
                reader = csv.reader(infile)
                writer = csv.writer(outfile)

                header = next(reader)
                new_header = [re.sub(r'user_(\d+)', lambda m: f'user{int(m.group(1)):02d}', col) for col in header]
                writer.writerow(new_header)

                for row in reader:
                    writer.writerow(row)

            os.replace(temp_file, file_path)

        # Process files in the data folder
        for file_name in os.listdir(data_folder):
            file_path = os.path.join(data_folder, file_name)

            # Skip directories
            if os.path.isdir(file_path):
                continue

            # Check file extension and excluded files
            _, file_extension = os.path.splitext(file_name)
            if file_extension.lower() not in allowed_extensions or file_name in excluded_files:
                logging.info(f"Skipping unsupported or excluded file: {file_name}")
                continue

            # Convert speaker labels for specific files
            if file_name in {'dialogue.txt', 'diarization.txt', 'emotion.txt'}:
                logging.info(f"Converting speaker labels in: {file_name}")
                convert_speaker_labels(file_path)

            # Convert user headers for specific CSV files
            if file_name in {'a_results.csv', 'anchor_results.csv', 'rppg_results.csv', 'v_results.csv'}:
                logging.info(f"Converting user headers in: {file_name}")
                convert_user_headers(file_path)

            # Get the renamed file name if applicable
            upload_name = rename_map.get(file_name, file_name)

            try:
                # Upload the file to the endpoint
                logging.info(f"Uploading file: {file_name} as {upload_name}")
                with open(file_path, 'rb') as file:
                    files = {'file': (upload_name, file)}
                    response = requests.post(url, files=files)

                if response.ok:
                    logging.info(f"File Upload Success: {upload_name} \n:::: {response.text}")
                else:
                    logging.error(f"File Upload Error: {upload_name} \n:::: {response.status_code} {response.text}")

            except Exception as e:
                logging.error(f"Failed to upload {file_name}: {e}")
                logging.error(traceback.format_exc())

        logging.info("File upload process complete.")

    except Exception as e:
        logging.error("Error during file upload process:")
        logging.error(traceback.format_exc())

        
# remove double quotes from the string
def remove_quotes(input_string):
    if input_string.startswith('"') and input_string.endswith('"'):
        # If the string starts and ends with double quotes
        # Remove the first and last character (double quotes)
        logging.info("Redis entry has double quotes :(")
        return input_string[1:-1]
    else:
        # If the string does not start and end with double quotes
        return input_string

# RETURN THE REDIS CLIENT INSTANCE
def get_redis_instance():
    pool = redis.ConnectionPool(host="54.215.45.218", port=6379, db=0)
    redis_instance = redis.StrictRedis(connection_pool=pool)
    return redis_instance

# INITIALIZE REDIS AS GLOBAL
redis_instance = get_redis_instance()

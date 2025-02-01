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
s3_endpoint = 'https://s3.us-west-1.amazonaws.com'

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
def download_video(resource_path, base_cache_dir, bucket_name='sync-dev-server'):
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
def download_s3_resource(resource_path, base_cache_dir, bucket_name='sync-dev-server'):
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



# Map files to their new names
RENAME_MAP = {
    'transcription.txt': 'ASR.txt',
    'dialogue.txt': 'nlp_result.txt',
    'diarization.txt': 'SpeakerDiarization.txt',
    'emotion.txt': 'EmotionText.txt',
    # Add more if you want to rename CSVs or other files
    # 'some_old.csv': 'some_new.csv',
}

# For certain text files that are "CSV-like" but tab-delimited
# (If your .txt files are comma-delimited, change to ',' instead of '\t')
TAB_DELIMITED_FILES = {
    'SpeakerDiarization.txt',
    'EmotionText.txt',
    'nlp_result.txt',         # If needed
    'ASR.txt',                # If needed
}

def recreate_out_folder(data_folder: str) -> str:
    """Recreate (remove if exists) the 'out' subfolder inside data_folder."""
    out_folder = os.path.join(data_folder, 'out')
    if os.path.exists(out_folder):
        shutil.rmtree(out_folder)
    os.makedirs(out_folder, exist_ok=True)
    return out_folder


def normalize_speaker_user_cell(text: str) -> str:
    """
    For a given text (column header or cell):
      - If it's exactly 'Speaker', rename to 'User'
      - Else if it matches 'speaker_XX' or 'user_XX', convert to 'userXX' (zero-padded).
    """
    # 1) Handle exact 'Speaker' -> 'User'
    if text == 'Speaker':
        return 'User'

    # 2) Handle 'speaker_XX' or 'user_XX' -> 'userXX'
    match = re.match(r'^(speaker|user)_(\d+)$', text, flags=re.IGNORECASE)
    if match:
        user_id = int(match.group(2))  # the digits
        # zero-pad
        return f"user{user_id:02d}"

    # Otherwise, return as-is
    return text


def convert_file_speaker_labels(input_file: str, output_file: str, delimiter: str):
    """
    Reads `input_file` (CSV or tab-delimited text) row by row, normalizes
    anything matching 'speaker_XX'/'user_XX' → 'userXX', and writes to `output_file`.

    Also renames header cells that are exactly 'Speaker' → 'User'.
    """
    try:
        # CSV approach with chosen delimiter
        with open(input_file, 'r', encoding='utf-8-sig', newline='') as infile, \
             open(output_file, 'w', encoding='utf-8', newline='') as outfile:

            reader = csv.reader(infile, delimiter=delimiter)
            writer = csv.writer(outfile, delimiter=delimiter)

            first_row = True
            for row in reader:
                # Convert each cell in the row
                new_row = [normalize_speaker_user_cell(cell) for cell in row]
                if first_row:
                    # If there's something specifically about a blank first column
                    # (like your example had an empty first cell), you can handle it here
                    first_row = False
                writer.writerow(new_row)

        logging.info(f"Converted speaker labels and saved to {output_file}.")
    except Exception as e:
        logging.error(f"Error converting speaker labels for {input_file}: {e}")
        logging.error(traceback.format_exc())


def rename_avatar(old_name: str) -> str:
    """
    Convert 'user_0.jpg' → 'user00.jpg', 'user_11.jpg' → 'user11.jpg', etc.
    Returns the new name or old_name if no match.
    """
    match = re.match(r"user_(\d+)\.jpg", old_name, flags=re.IGNORECASE)
    if match:
        user_id = int(match.group(1))
        return f"user{user_id:02d}.jpg"
    return old_name


def prepare_final_files(data_folder: str) -> str:
    """
    1. Recreate out/ folder in data_folder.
    2. Convert or rename files as needed, putting final results into out/.
    3. Return the path to the out folder.
    """
    out_folder = recreate_out_folder(data_folder)

    for file_name in os.listdir(data_folder):
        file_path = os.path.join(data_folder, file_name)

        # Skip directories
        if os.path.isdir(file_path):
            continue

        # Skip excluded files (e.g., .wav, output.json, etc.)
        if file_name.endswith('.wav') or file_name == 'output.json':
            continue

        # Determine the final name based on RENAME_MAP
        final_name = RENAME_MAP.get(file_name, file_name)
        out_file_path = os.path.join(out_folder, final_name)

        # 1) If it's a JPG avatar, rename "user_0.jpg" -> "user00.jpg"
        _, ext = os.path.splitext(file_name)
        if ext.lower() == '.jpg' and file_name.startswith("user_"):
            avatar_renamed = rename_avatar(final_name)
            shutil.copy2(file_path, os.path.join(out_folder, avatar_renamed))
            continue

        # 2) If it ends with ".csv", parse as comma-delimited, fix speaker/user fields
        if file_name.lower().endswith('.csv'):
            convert_file_speaker_labels(
                input_file=file_path,
                output_file=out_file_path,
                delimiter=','
            )
            continue

        # 3) If it's a known tab-delimited file (like SpeakerDiarization.txt),
        #    parse and fix speaker/user fields.
        if final_name in TAB_DELIMITED_FILES:
            convert_file_speaker_labels(
                input_file=file_path,
                output_file=out_file_path,
                delimiter='\t'
            )
            continue

        # 4) Otherwise, just copy the file without changes
        shutil.copy2(file_path, out_file_path)

    return out_folder


def upload_selected_files_to_endpoint(meeting_id: str, data_folder: str):
    """
    1. Prepare final files in the out/ folder (conversions + rename).
    2. Upload them based on your existing logic:
       - Avatars → avatar_url
       - Some files → direct S3
       - Others → main REST endpoint
    """
    try:
        # Prepare final files in out/
        out_folder = prepare_final_files(data_folder)

        # Extract the numeric ID from the meeting ID

        # Endpoint for standard file uploads
        base_url = 'http://18.144.100.85:8080/s3/uploadByMeeting/'
        url = f"{base_url}{meeting_id}"

        # Avatar upload endpoint
        avatar_url = f"http://18.144.100.85:8080/video/avatar/{meeting_id}/"

        # Allowed file extensions
        allowed_extensions = {'.txt', '.jpg', '.csv', '.json'}

        # Files to exclude
        excluded_files = {'output.json', 'wav'}

        logging.info(f"Uploading files from '{out_folder}' to endpoint {url}...")

        def upload_avatar(file_path: str, renamed_file_name: str):
            """
            Uploads avatar file to the specific avatar endpoint.
            """
            # Remove the extension for the URL:
            avatar_name, ext = os.path.splitext(renamed_file_name)
            avatar_upload_url = f"{avatar_url}{avatar_name}"
            
            logging.info(f"Uploading avatar to: {avatar_upload_url}")
            with open(file_path, 'rb') as file:
                files = {'file': (renamed_file_name, file)}
                response = requests.post(avatar_upload_url, files=files)

            if response.ok:
                logging.info(f"Avatar Upload Success: {renamed_file_name}")
            else:
                logging.error(f"Avatar Upload Error: {renamed_file_name} "
                            f"\n:::: {response.status_code} {response.text}")


        # Process files in the out folder
        for file_name in os.listdir(out_folder):
            file_path = os.path.join(out_folder, file_name)

            if os.path.isdir(file_path):
                continue

            # Check file extension and excluded files
            _, file_extension = os.path.splitext(file_name)
            if file_extension.lower() not in allowed_extensions or file_name in excluded_files:
                logging.info(f"Skipping unsupported or excluded file: {file_name}")
                continue

            # Handle avatar uploads (e.g., user00.jpg, user01.jpg, etc.)
            if file_extension.lower() == '.jpg' and file_name.lower().startswith("user"):
                upload_avatar(file_path, file_name)
                continue

            # If you want certain files to go directly to S3, do so here:
            if file_name in {'ASR.txt', 'SpeakerDiarization.txt', 'EmotionText.txt', 'nlp_result.txt'}:
                # Example S3 key: "test/<id_>/<filename>"
                s3_key = f"test/{meeting_id}/{file_name}"
                logging.info(f"Uploading file directly to S3: {file_path} -> {s3_key}")
                # upload_resource(file_path, s3_key, "sync-dev-server")  # your actual S3 method
                continue

            # Otherwise, upload to the main REST endpoint
            try:
                logging.info(f"Uploading file: {file_name} to {url}")
                with open(file_path, 'rb') as file:
                    files = {'file': (file_name, file)}
                    response = requests.post(url, files=files)

                if response.ok:
                    logging.info(f"File Upload Success: {file_name} \n:::: {response.text}")
                else:
                    logging.error(f"File Upload Error: {file_name} "
                                  f"\n:::: {response.status_code} {response.text}")

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
    pool = redis.ConnectionPool(host="18.144.100.85", password="123456", port=6379, db=0)
    redis_instance = redis.StrictRedis(connection_pool=pool)
    return redis_instance

# INITIALIZE REDIS AS GLOBAL
redis_instance = get_redis_instance()

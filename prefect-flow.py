from __future__ import annotations
from typing import List, Dict, Any, Optional, Tuple
import os
import time
import requests
import boto3
from prefect import flow, task, get_run_logger
import json


# This is the API endpoint we POST to
API_ENDPOINT = "https://j9y2xa0vx0.execute-api.us-east-1.amazonaws.com/api/scatter/xfd3tf"
# This is the SQS queue we send our final, assembled phrase to
SUBMIT_QUEUE_URL = "https://sqs.us-east-1.amazonaws.com/440848399208/dp2-submit"

# Initialize the SQS client globally so it can be reused by all tasks
sqs = boto3.client('sqs')

# Trigger the API
# This task will retry up to 3 times, with a 10-second delay, if it fails.
@task(retries=3, retry_delay_seconds=10)
def trigger_api() -> str:
    """
    POST to API to populate queue
    Returns the SQS queue URL from the payload.
    """
    # Get the Prefect logger to log messages specific to this flow run
    logger = get_run_logger()
    logger.info(f"POST {API_ENDPOINT}")
    
    # Make the HTTP POST request. This tells the API to start scattering the messages.
    resp = requests.post(API_ENDPOINT, timeout=30)
    # This line will raise an error if the HTTP response was not successful (e.g., 4xx or 5xx)
    resp.raise_for_status()
    
    # Parse the JSON response from the API
    payload = resp.json()
    # Extract the unique SQS queue URL where the messages will be scattered
    sqs_url = payload["sqs_url"]
    logger.info(f"SQS URL: {sqs_url}")
    
    # Return the scatter queue URL so the next task can use it
    return sqs_url


# Define a helper task to check the status of the queue
@task(retries=3, retry_delay_seconds=10)
def get_queue_attributes(sqs_url: str) -> Dict[str, int]:
    """Get the queue URL from the API payload."""
    # Create a new SQS client (boto3 clients are not always thread-safe,
    # so it's good practice in tasks that might run concurrently)
    sqs = boto3.client("sqs")
    # Request "All" attributes for the given queue
    attrs = sqs.get_queue_attributes(QueueUrl=sqs_url, AttributeNames=["All"]).get("Attributes", {})
    
    # We only care about these three message counts
    keys = [
        "ApproximateNumberOfMessages",
        "ApproximateNumberOfMessagesNotVisible",
        "ApproximateNumberOfMessagesDelayed",
    ]
    # This dictionary comprehension safely gets each count, defaults to "0" if missing,
    # and converts it to an integer.
    return {k: int(attrs.get(k, "0")) for k in keys}


# Define a simple helper function (not a task) to parse a single message
def _parse_one(msg: Dict[str, Any]) -> Tuple[int, str]:
    """Helper function to extract data from one SQS message."""
    # Extract the order_no from the message's attributes and cast it to an integer
    order_no = int(msg['MessageAttributes']['order_no']['StringValue'])
    # Extract the 'word' from the message's attributes
    word = msg['MessageAttributes']["word"]["StringValue"]
    # Return the data as a (number, word) tuple
    return order_no, word


# Receive all messages from the scatter queue
@task(retries=3, retry_delay_seconds=10)
def receive_all_messages(sqs_url: str, expected: int = 21) -> List[Tuple[int, str]]:
    """
    Long-poll the queue until we collect all expected messages
    (or until none remain visible/delayed). Deletes each message after capture.
    """
    logger = get_run_logger()
    # This list will store all the (order_no, word) tuples we collect
    pairs: List[Tuple[int, str]] = []
    # This counter tracks how many times we've polled and received nothing
    empty_polls = 0

    # Loop until we have collected the expected number of messages
    while len(pairs) < expected:
        # Request messages from the scatter queue
        resp = sqs.receive_message(
            QueueUrl=sqs_url,
            AttributeNames=["All"],
            MessageAttributeNames=["All"],
            MaxNumberOfMessages=10,      # SQS max batch size
            VisibilityTimeout=30,        # Hide messages for 30s after we read them
            WaitTimeSeconds=10,          # Use SQS Long Polling to wait up to 10s
        )
        
        # Safely get the 'Messages' list from the response; default to an empty list
        msgs = resp.get("Messages", [])

        # This block handles the case where we received 0 messages in our poll
        if not msgs:
            empty_polls += 1
            # Run the get_queue_attributes task to check the queue's real status
            # .submit() runs the task and returns a "future" (a reference to the run)
            counts_future = get_queue_attributes.submit(sqs_url)
            # .result() waits for the future to complete and returns its value
            counts = counts_future.result()
            # Sum all message counts (visible, delayed, not-visible)
            remaining = sum(counts.values())
            logger.info(f"No messages received. Queue Counts={counts}, collected={len(pairs)}/{expected}")
            
            # if the queue reports 0 messages and
            # we've had at least 2 empty polls, we assume we're done.
            if remaining == 0 and empty_polls >= 2:
                logger.warning("Queue shows 0 remaining; stopping collection early.")
                break # Exit the while loop
            
            # If we got no messages but more are expected, pause briefly
            time.sleep(5)
            # Go to the top of the while loop to poll again
            continue

        # If we did get messages, reset the empty poll counter
        empty_polls = 0
        
        # Process each message in the batch we received
        for m in msgs:
            try:
                # Try to parse the message
                pair = _parse_one(m)
                pairs.append(pair)
            except KeyError:
                # If parsing fails (e.g., bad message), log it and skip
                logger.warning(f"Failed to parse message: {m.get('MessageId')}. Skipping.")
            finally:
                # Always delete the message from the queue after processing,
                # whether it succeeded or failed, to prevent it from being read again.
                sqs.delete_message(QueueUrl=sqs_url, ReceiptHandle=m["ReceiptHandle"])

        # Log our progress after processing a batch
        logger.info(f"Collected {len(pairs)}/{expected} so far…")
        # Small pause
        time.sleep(0.5)

    # Once the while loop is finished, return the complete list of pairs
    return pairs


# Define Task 3 (Part 1): Assemble the phrase
@task
def assemble_phrase(pairs: List[Tuple[int, str]]) -> str:
    """
    Task 3 (part 1): sort by order_no and join words.
    """
    # This list comprehension does two things:
    # 1. sorted(pairs, key=lambda t: t[0]) sorts the list of tuples based on the first item (the order_no)
    # 2. [w for _, w in ...] extracts just the second item (the word) from the sorted list
    words = [w for _, w in sorted(pairs, key=lambda t: t[0])]
    
    # Join all the words in the sorted list with a single space
    return " ".join(words)


# Send the final solution
@task(retries=3, retry_delay_seconds=10)
def send_solution(uvaid: str, phrase: str, platform: str = "prefect") -> None:
    """
    Task 3 (part 2): submit the final phrase to dp2-submit with attributes.
    """
    logger = get_run_logger()
    try:
        # Send a new message to the final submission queue
        response = sqs.send_message(
            QueueUrl=SUBMIT_QUEUE_URL,
            # The MessageBody is required, but the real data is in the attributes
            MessageBody="DP2 Submission",
            # Attach our data (UVA ID, phrase, platform) as Message Attributes
            MessageAttributes={
                'uvaid': {
                    'DataType': 'String',
                    'StringValue': uvaid
                },
                'phrase': {
                    'DataType': 'String',
                    'StringValue': phrase
                },
                'platform': {
                    'DataType': 'String',
                    'StringValue': platform
                }
            }
        )
        # Log the unique MessageId
        logger.info(f"Submit response: {response.get('MessageId')}")
        
    except Exception as e:
        # If sending fails, log the error and let Prefect handle the retry
        logger.error(f"Error sending solution: {e}")
        raise e


# Define the main workflow
@flow(name="dp2-prefect")
def main():
    logger = get_run_logger()
    # UVA ID
    uvaid = "xfd3tf"
    
    # Step 1: Call the trigger_api task and get the scatter queue URL
    scatter_queue_url = trigger_api()
    
    # Step 2: Pass the scatter URL to the receive_all_messages task.
    # This task will run until it collects all messages.
    pairs = receive_all_messages(sqs_url=scatter_queue_url)
    
    # Step 3: Pass the list of pairs to the assemble_phrase task.
    phrase = assemble_phrase(pairs)
    
    # Log the assembled phrase
    logger.info(f"PHRASE: {phrase}")
    
    # Step 4: Pass the phrase and uvaid to the send_solution task.
    send_solution(uvaid, phrase, platform="prefect")

    
# Executing the script
if __name__ == "__main__":
    main()

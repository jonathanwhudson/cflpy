import json
import os
import pathlib
import time

import backoff
import ratelimit
import requests

import config
from download_history import download_history

MINUTE = 60
"""Amount of seconds in a minute"""


def mkdir(dir_path: os.path) -> None:
    """
    Make a directory if it does not exist
    :param dir_path: The directory to make
    :return: The file system is changed so the directory now exists
    """
    if type(dir_path) != os.path and type(dir_path) != pathlib.WindowsPath:
        raise ValueError(f"Type of path_dir <{type(dir_path)}> should be os.path!")
    if not os.path.exists(dir_path):
        os.mkdir(dir_path)


def downloader(url: str, file_path: os.path, parameters: dict = {}, reset: bool = False, errors: bool = False) -> bool:
    """
    Download an url to a file (takes parameters, checks exists, resets, and loops on API errors)
    :param url: The url to download
    :param file_path: The file path to store the result at
    :param parameters: The additional parameters (beyond API key)
    :param reset: Whether file should be deleted and re-downloaded
    :param errors: Whether we should loop on json errors
    :return: True if downloaded, False if file was there and not re-downloaded
    """
    if type(url) != str:
        raise ValueError(f"Type of url <{type(url)}> should be str!")
    if type(file_path) != os.path and type(file_path) != pathlib.WindowsPath:
        raise ValueError(f"Type of file_path <{type(file_path)}> should be os.path!")
    if type(parameters) != dict:
        raise ValueError(f"Type of parameters <{type(parameters)}> should be dict!")
    if type(reset) != bool:
        raise ValueError(f"Type of reset <{type(reset)}> should be bool!")
    # Add in API key to parameters
    parameters["key"] = config.API_KEY
    # Reset means make sure it is gone be for retrying
    downloaded = False
    if reset and os.path.exists(file_path):
        os.remove(file_path)
    # If not there just download
    if not os.path.exists(file_path):
        download(url, file_path, parameters)
        downloaded = True
    # Check errors regardless
    while errors and check_errors(file_path):
        print(f"Error in {file_path} re-downloading!")
        os.remove(file_path)
        download(url, file_path, parameters)
        downloaded = True
    return downloaded


def download(url: str, file_path: os.path, parameters: dict) -> None:
    """
    Download an url to a file (takes parameters)
    :param url: The url to download
    :param file_path: The file path to store the result at
    :param parameters: The parameters (needs API key added)
    :return: None (but file should now be on local machine)
    """
    # Make sure history is loaded for pacing of API calls
    download_history.load()
    # Convert into full URL
    full_url = f"{url}?"
    for key, value in parameters.items():
        full_url += f"{key}={value},"
    # Loop until download successful
    while True:
        print(f"Downloading:{full_url}")
        # Wait until the API requests are at correct rate
        download_history.wait()
        # Track new request
        download_history.track_now()
        download_history.save()
        # Get response
        response = download_limited(url, parameters)
        # Check if successful and if so then store
        if response.status_code == requests.codes.ok:
            with open(file_path, "w") as file:
                if response.text:
                    file.write(response.text)
                    return
        # If we failed then loop again after message
        if response.status_code == 429:
            print(
                f"API responded 'Too Many Requests' (429) sleep {MINUTE}s and retry! Download history at size {download_history.size()}.")
            time.sleep(MINUTE)
        else:
            print(f"API responded with failure code ({response.status_code}) sleep {MINUTE}s and retry!")
            time.sleep(MINUTE)


def backoff_handler(details) -> None:
    """
    This handles printing a message if back-off is requested
    :param details: The storage info for the function call to make message from
    :return: None
    """
    url = details['args'][0]
    full_url = f"{url}?"
    for key, value in details['args'][1].items():
        full_url += f"{key}={value},"
    wait = details['wait']
    tries = details['tries']
    print(f"Backing off:{full_url} {wait:3.1f}s after {tries}.")


@backoff.on_exception(backoff.constant, ratelimit.RateLimitException, max_tries=5, on_backoff=backoff_handler,
                      jitter=None, interval=15)
@ratelimit.limits(calls=config.API_MAX_CALLS_PER_MINUTE, period=MINUTE)
def download_limited(url: str, parameters: dict[str, str]) -> requests.Response:
    """
    This is a secondary rate limited function in-case download history isn't covering it
    :param url: The url to download
    :param parameters: The parameters (needs API key added)
    :return: None (but file should be in response)
    """
    return requests.get(url, params=parameters)


def check_errors(file_path: os.path) -> bool:
    """
    Used to check CFL API json to see if it says it is correct
    :param file_path: The file to open and look at
    :return: True if something fails, False if passes
    """
    with open(file_path) as file:
        text = file.read()
    if not text:
        print(f"Nothing in {file_path}!")
        return True
    json_string = json.loads(text)
    errors = json_string['errors']
    if errors:
        print(f"Error in json 'errors' category for {file_path}!")
        return True
    return False

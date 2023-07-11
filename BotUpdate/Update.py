# Resource Folder Import
import os
import sys
sys.path.append(os.environ['autobot_modules'])

# Imports
import requests
from config import API_KEY, BASE_API_URL

def update_bot_status(bot_name, status):
        content = {'status': status, 'key':API_KEY}
        url = f"{BASE_API_URL}api/bots/{bot_name}"
        try:
            requests.post(url, json=content)
        except:
            print("Could not update bot status...")

def add_bot_queue(bot_name, task_name, additional_info=None) -> None:
    """
    Adds a task to the bot's queue.
    """
    if additional_info:
        data = {'name': task_name, 'additional_info': additional_info}
    else:
        data = {'name': task_name}
    url = f"{BASE_API_URL}api/bots/{bot_name}/queue"
    try:
        requests.post(url, json=data)
    except:
        print('Error updating bot queue...')

def add_bot_history(bot_name, task_name, additional_info=None) -> None:
    """
    Adds a task to the bot's history.
    """
    if additional_info:
        data = {'name': task_name, 'additional_info': additional_info}
    else:
        data = {'name': task_name}
    url = f"{BASE_API_URL}api/bots/{bot_name}/history"
    try:
        requests.post(url, json=data)
    except:
        print('Error updating bot history...')

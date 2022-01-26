from slack import WebClient
from slack.errors import SlackApiError
from slack.web.slack_response import SlackResponse


def send_message(token: str, message: str) -> int:
    client = WebClient(token=token)

    try:
        response = client.chat_postMessage(
        channel="airflowcourse",
        text=message)

        return response.status_code
        
    except SlackApiError as e:
        assert e.response["error"] 
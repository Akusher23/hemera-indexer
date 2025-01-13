import json
import logging
import os

import flask
import requests
from flask_restx import Resource

from hemera.api.app.alarm import alarm_namespace
from hemera.common.utils.config import get_config

app_config = get_config()
LARK_BASE_URL = "https://open.larksuite.com/open-apis"
MESSAGE_ENDPOINT = "/im/v1/messages"
TOKEN_ENDPOINT = "/auth/v3/tenant_access_token/internal"


@alarm_namespace.route("/v1/alarm/test")
class CheckGrafanaData(Resource):
    def post(self):
        if not flask.request.is_json:
            logging.error("Not JSON request")
            return {"error": "Content type must be application/json"}, 400
        body = flask.request.json
        logging.info(body)

        return "ok", 200


@alarm_namespace.route("/v1/alarm/receive")
class ReceiveAlarm(Resource):
    def post(self):
        if flask.request.is_json:
            request_body = flask.request.json
        else:
            request_body = flask.request.form.to_dict()

        content = request_body.get("content", None)
        if content is None:
            return "Message cannot be empty", 400

        token = get_access_token()
        status = send_message(token, content)
        return status["msg"], 200


def get_access_token():
    auth_url = f"{LARK_BASE_URL}{TOKEN_ENDPOINT}"
    auth_payload = {"app_id": app_config.alarm_app_id, "app_secret": app_config.alarm_app_secret}
    auth_headers = {"Content-Type": "application/json"}
    auth_response = requests.post(auth_url, json=auth_payload, headers=auth_headers)
    auth_data = auth_response.json()
    return auth_data.get("tenant_access_token")


def send_message(access_token, content):
    url = f"{LARK_BASE_URL}{MESSAGE_ENDPOINT}?receive_id_type=open_id"

    req = {
        "receive_id": os.environ.get("RECEIVE_ID", ""),
        "msg_type": "text",
        "content": json.dumps({"text": content}),
    }

    payload = json.dumps(req)

    headers = {"Authorization": f"Bearer {access_token}", "Content-Type": "application/json"}

    response = requests.request("post", url, headers=headers, data=payload)
    return json.loads(response.text)

import json
from unittest import mock

import pytest
import requests

from uploader import uploader


def _make_body(tmp_path):
    """Write a small output file and return a message body pointing at it.

    uploader joins the path onto "/output"; os.path.join discards that prefix
    when the second argument is absolute, so the worker reads from tmp_path.
    """
    output = tmp_path / "result.json"
    output.write_text("{}")
    return json.dumps({"cog_id": "abc123", "cdr_output": str(output)})


def _run_worker(body):
    worker = uploader.Worker()
    worker.process(mock.Mock(delivery_tag=1), mock.Mock(), body)
    worker.run()  # run synchronously; no thread needed
    return worker


def _http_response(status):
    response = requests.Response()
    response.status_code = status
    response._content = b'{"detail": "server said no"}'
    response.url = "https://cdr.test/v1/maps/publish/features"
    return response


def test_connection_error_is_recorded(tmp_path):
    error = requests.exceptions.ConnectionError("connection refused")
    with mock.patch.object(uploader.requests, "post", side_effect=error):
        worker = _run_worker(_make_body(tmp_path))
    assert worker.exception is error


def test_http_error_is_recorded(tmp_path):
    with mock.patch.object(uploader.requests, "post", return_value=_http_response(500)):
        worker = _run_worker(_make_body(tmp_path))
    assert isinstance(worker.exception, requests.exceptions.HTTPError)


def test_successful_upload_records_no_exception(tmp_path):
    with mock.patch.object(uploader.requests, "post", return_value=_http_response(200)):
        worker = _run_worker(_make_body(tmp_path))
    assert worker.exception is None


class _SyncWorker(uploader.Worker):
    """Worker whose start() runs inline so main() sees it finished immediately.

    Like a real thread, an exception escaping run() is swallowed rather than
    propagated to main(), so this reproduces the production routing decision.
    """

    def start(self):
        try:
            self.run()
        except Exception:
            pass

    def is_alive(self):
        return False


@pytest.mark.parametrize(
    "post_kwargs, expected_queue",
    [
        ({"side_effect": requests.exceptions.ConnectionError("down")}, "upload.error"),
        ({"return_value": _http_response(503)}, "upload.error"),
        ({"return_value": _http_response(200)}, "completed"),
    ],
)
def test_main_routes_to_correct_queue(tmp_path, post_kwargs, expected_queue):
    body = _make_body(tmp_path)
    channel = mock.Mock()
    # one message, then end the otherwise-infinite loop
    channel.consume.return_value = iter([(mock.Mock(delivery_tag=7), mock.Mock(), body)])
    connection = mock.Mock()
    connection.channel.return_value = channel

    with mock.patch.object(uploader.pika, "BlockingConnection", return_value=connection), \
         mock.patch.object(uploader, "Worker", _SyncWorker), \
         mock.patch.object(uploader.requests, "post", **post_kwargs):
        with pytest.raises(StopIteration):
            uploader.main()

    publish = channel.basic_publish.call_args.kwargs
    assert publish["routing_key"] == f"{uploader.prefix}{expected_queue}"
    if expected_queue == "upload.error":
        assert "exception" in json.loads(publish["body"])
    channel.basic_ack.assert_called_once_with(delivery_tag=7)

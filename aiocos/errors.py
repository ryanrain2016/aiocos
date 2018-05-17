import re

ERROR_RE=re.compile(r'<(\w+)>([^<]+)</\1>')

__all__=['CosClientError', 'CosServiceError']

class CosClientError(Exception):
    "客户端错误"
    pass

class CosServiceError(Exception):

    def __init__(self, msg, status):
        self._msg = msg
        info = ERROR_RE.findall(msg)
        self._info = dict(info)
        self._http_status = status

    def get_origin_msg(self):
        return self._msg

    def get_digest_msg(self):
        return self._info

    def get_status_code(self):
        return self._http_status

    def get_error_code(self):
        return self._info['Code']

    def get_error_msg(self):
        return self._info['Message']

    def get_trace_id(self):
        return self._info['TraceId']

    def get_request_id(self):
        return self._info['RequestId']

    def get_resource_location(self):
        return self._info['Resource']


import json
import logging
import os
import socket
import struct
import sys
import threading
import time
import traceback
import zlib
import nsq
import requests


from dmoj import sysinfo
from dmoj.judgeenv import get_supported_problems, get_runtime_versions

logger = logging.getLogger('dmoj.judge')
timer = time.clock if os.name == 'nt' else time.time


class JudgeAuthenticationFailed(Exception):
    pass


class PacketManager(object):
    SIZE_PACK = struct.Struct('!I')

    def __init__(self, url, key):
        self.key = key
        self.url = url
        # Exponential backoff: starting at 4 seconds.
        # Certainly hope it won't stack overflow, since it will take days if not years.

    def _send_packet(self, packet):

        try:
            packet['key'] = self.key
            resp = requests.post(self.url, data=json.dumps(packet)) 
            res = resp.json()
            res['code'] = 0
            return res
        except Exception as ex:
            print ex
            return {'code': -1, 'msg': ex}

    def handshake(self, problems, runtimes, id, key):
        self._send_packet({'name': 'handshake',
                           'problems': problems,
                           'executors': runtimes,
                           'id': id,
                           'key': key})

    def invocation_begin_packet(self, current_submission):
        logger.info('Begin invoking: %d', current_submission)
        self._send_packet({'name': 'invocation-begin',
                           'invocation-id': current_submission})

    def invocation_end_packet(self, result, current_submission):
        # logger.info('End invoking: %d', self.judge.current_submission)
        self.fallback = 4
        self._send_packet({'name': 'invocation-end',
                           'output': result.proc_output,
                           'status': result.status_flag,
                           'time': result.execution_time,
                           'memory': result.max_memory,
                           'feedback': result.feedback,
                           'invocation-id': current_submission})

    def supported_problems_packet(self, problems):
        logger.info('Update problems')
        self._send_packet({'name': 'supported-problems',
                           'problems': problems})

    def test_case_status_packet(self, result, current_submission):
        self._send_packet({'name': 'test-case-status',
                           'submission-id': current_submission,
                           'position': result.case.position,
                           'status': result.result_flag,
                           'time': result.execution_time,
                           'memory': result.max_memory,
                           'output': result.output})

    def compile_error_packet(self, log, current_submission):
        self.fallback = 4
        self._send_packet({'name': 'compile-error',
                           'submission-id': current_submission,
                           'log': log})

    def compile_message_packet(self, log, current_submission):
        logger.info('Compile message: %d', current_submission)
        self._send_packet({'name': 'compile-message',
                           'submission-id': current_submission,
                           'log': log})

    def internal_error_packet(self, message, current_submission):
        logger.info('Internal error: %d', current_submission)
        self._send_packet({'name': 'internal-error',
                           'submission-id': current_submission,
                           'message': message})

    def begin_grading_packet(self, current_submission):
        logger.info('Begin grading: %d', current_submission)
        self._send_packet({'name': 'grading-begin',
                           'submission-id': current_submission})

    def grading_end_packet(self, current_submission):
        logger.info('End grading: %d', current_submission)
        self.fallback = 4
        self._send_packet({'name': 'grading-end',
                           'submission-id': current_submission})

    def batch_begin_packet(self, current_submission):
        self._batch += 1
        self._send_packet({'name': 'batch-begin',
                           'submission-id': current_submission})

    def batch_end_packet(self, current_submission):
        self._send_packet({'name': 'batch-end',
                           'submission-id': current_submission})

    def current_submission_packet(self, current_submission):
        logger.info('Current submission query: %d', current_submission)
        self._send_packet({'name': 'current-submission-id',
                           'submission-id': current_submission})

    def submission_terminated_packet(self, current_submission):
        logger.info('Submission aborted: %d', current_submission)
        self._send_packet({'name': 'submission-terminated',
                           'submission-id': current_submission})

    def ping_packet(self, when):
        data = {'name': 'ping-response',
                'when': when,
                'time': time.time()}
        for fn in sysinfo.report_callbacks:
            key, value = fn()
            data[key] = value
        self._send_packet(data)

    def submission_acknowledged_packet(self, sub_id):
        self._send_packet({'name': 'submission-acknowledged',
                           'submission-id': sub_id})

    def invocation_acknowledged_packet(self, sub_id):
        self._send_packet({'name': 'submission-acknowledged',
                           'invocation-id': sub_id})

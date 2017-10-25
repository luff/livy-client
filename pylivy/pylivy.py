#!/usr/bin/env python
#
# Copyright (c) 2017 luyi@neucloud.cn
#

import json
import requests
import time


class LivyClient(object):

  def __init__(self, rest_api, username='', password='', insecure=False):
    self.rest_api = rest_api
    self._s = requests.session()
    if insecure:
      requests.packages.urllib3.disable_warnings()
    self._verify = not insecure
    if username and password:
      self._s.auth = (username, password)

  def _process_response(self, response):
    code = response.status_code
    if code < 400:
      return
    cl = response.headers.get('Content-Length')
    ct = response.headers.get('content-type')
    if cl and cl == '0':
      response.raise_for_status()
    if ct and ct.startswith('application/json'):
      e = json.dumps(response.json(), indent=2)
    else:
      e = response.content
    raise Exception(
      'Server return code {}, response message:\n{}'
      .format(code, e)
    )

  def wait_session_start(self, session_id, timeout=60):
    state = None
    while state != 'idle' and timeout > 0:
      time.sleep(1)
      timeout -= 1
      state = self.get_session_state(session_id).get('state')
    return state == 'idle'

  def wait_session_statement_ready(self, session_id, statement_id, timeout=30):
    state = None
    while state != 'available' and timeout > 0:
      time.sleep(1)
      timeout -= 1
      state = self.get_session_statement(session_id, statement_id).get('state')
    return state == 'available'

  def get_sessions(self, begin, size):
    p = { "from": begin, "size": size }
    r = self._s.get(
      '{}/sessions'
      .format(self.rest_api),
      params=p,
      verify=self._verify
    )
    self._process_response(r)
    return r.json().get('sessions')

  def get_session_state(self, session_id):
    r = self._s.get(
      '{}/sessions/{}/state'
      .format(self.rest_api, session_id),
      verify=self._verify
    )
    self._process_response(r)
    return r.json()

  def get_session_log(self, session_id, begin, size):
    p = { "from": begin, "size": size }
    r = self._s.get(
      '{}/sessions/{}/log'
      .format(self.rest_api, session_id),
      params=p,
      verify=self._verify
    )
    self._process_response(r)
    return r.json()

  def get_session(self, session_id):
    r = self._s.get(
      '{}/sessions/{}'
      .format(self.rest_api, session_id),
      verify=self._verify
    )
    self._process_response(r)
    return r.json()

  def post_session(self, **kwargs):
    r = self._s.post(
      '{}/sessions'
      .format(self.rest_api),
      data=json.dumps(kwargs),
      verify=self._verify
    )
    self._process_response(r)
    return r.json()

  def delete_session(self, session_id):
    r = self._s.delete(
      '{}/sessions/{}'
      .format(self.rest_api, session_id),
      verify=self._verify
    )
    self._process_response(r)
    return r.json()

  def get_session_statements(self, session_id):
    r = self._s.get(
      '{}/sessions/{}/statements'
      .format(self.rest_api, session_id),
      verify=self._verify
    )
    self._process_response(r)
    return r.json()

  def post_session_statement(self, session_id, code):
    d = { "code": code }
    r = self._s.post(
      '{}/sessions/{}/statements'
      .format(self.rest_api, session_id),
      data=json.dumps(d),
      verify=self._verify
    )
    self._process_response(r)
    return r.json()

  def get_session_statement(self, session_id, statement_id):
    r = self._s.get(
      '{}/sessions/{}/statements/{}'
      .format(self.rest_api, session_id, statement_id),
      verify=self._verify
    )
    self._process_response(r)
    return r.json()

  def post_session_statement_cancel(self, session_id, statement_id):
    r = self._s.post(
      '{}/sessions/{}/statements/{}/cancel'
      .format(self.rest_api, session_id, statement_id),
      verify=self._verify
    )
    self._process_response(r)
    return r.json()

  def get_batches(self, begin, size):
    p = { "from": begin, "size": size }
    r = self._s.get(
      '{}/batches'
      .format(self.rest_api),
      params=p,
      verify=self._verify
    )
    self._process_response(r)
    return r.json()

  def post_batch(self, **kwargs):
    r = self._s.post(
      '{}/batches'
      .format(self.rest_api),
      data=json.dumps(kwargs),
      verify=self._verify
    )
    self._process_response(r)
    return r.json()

  def get_batch(self, batch_id):
    r = self._s.get(
      '{}/batches/{}'
      .format(self.rest_api, batch_id),
      verify=self._verify
    )
    self._process_response(r)
    return r.json()

  def get_batch_state(self, batch_id):
    r = self._s.get(
      '{}/batches/{}/state'
      .format(self.rest_api, batch_id),
      verify=self._verify
    )
    self._process_response(r)
    return r.json()

  def delete_batch(self, batch_id):
    r = self._s.delete(
      '{}/batches/{}'
      .format(self.rest_api, batch_id),
      verify=self._verify
    )
    self._process_response(r)
    return r.json()

  def get_batch_log(self, batch_id, begin, size):
    p = { "from": begin, "size": size }
    r = self._s.get(
      '{}/batches/{}/log'
      .format(self.rest_api, batch_id),
      params=p,
      verify=self._verify
    )
    self._process_response(r)
    return r.json()


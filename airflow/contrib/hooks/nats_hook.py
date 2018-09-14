# -*- coding: utf-8 -*-
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

from pynats import NATSClient

from airflow.hooks.base_hook import BaseHook
from socket import timeout


class NATSHook(BaseHook):
    def __init__(self, nats_conn_id='nats_default', subject='default', timeout=5.000):
        self.nats_conn_id = nats_conn_id
        self.subject = subject
        self.timeout = timeout

    def get_conn_url(self):
        conn = self.get_connection(self.nats_conn_id)
        return "nats://{host}:{port}".format(conn)

    def get_one_message(self):
        msg = None
        with NATSClient(url=self.get_conn_url(), socket_timeout=self.timeout) as client:
            def callback(message):
                nonlocal msg
                msg = message.payload

            client.subscribe(self.subject, callback=callback, max_messages=1)
            try:
                client.wait(count=1)
            except timeout:
                self.log.info("no message... :(")
            finally:
                return msg

    def publish(self, message):
        with NATSClient(url=self.get_conn_url(), socket_timeout=self.timeout) as client:
            client.publish(self.subject, payload=message.encode())

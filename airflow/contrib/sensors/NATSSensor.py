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
from airflow.contrib.hooks.nats_hook import NATSHook
from airflow.sensors.base_sensor_operator import BaseSensorOperator
from airflow.utils.decorators import apply_defaults


class NATSSensor(BaseSensorOperator):
    """
    Checks for the existence of a message for a NATS subject (and passes
    message through XCom).
    """
    template_fields = ('subject',)
    ui_color = '#f0eee4'

    @apply_defaults
    def __init__(self, subject, nats_conn_id, *args, **kwargs):
        """
        Create a new NATSSensor

        :param subject: The subject to be monitored
        :type subject: str
        :param nats_conn_id: The connection ID to use when connecting to NATS Server.
        :type nats_conn_id: str
        """
        super(NATSSensor, self).__init__(*args, **kwargs)
        self.nats_conn_id = nats_conn_id
        self.subject = subject

        self._message = None

    def execute(self, context):
        """Overridden to allow messages to be passed"""
        super(NATSSensor, self).execute(context)
        return self._message

    def poke(self, context):
        self.log.info('Sensor check existence of message for subject: %s', self.subject)
        nats_hook = NATSHook(nats_conn_id=self.nats_conn_id, subject=self.subject)
        self._message = nats_hook.get_one_message()
        self.log.info('Sensor received message: %s', self._message)
        return self._message

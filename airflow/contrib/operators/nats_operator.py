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
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class NATSPublishOperator(BaseOperator):
    template_fields = ('subject', 'message')
    ui_color = '#ff7f50'

    @apply_defaults
    def __init__(self, subject, message, nats_conn_id='nats_default', *args, **kwargs):
        super(NATSPublishOperator, self).__init__(*args, **kwargs)

        self.nats_conn_id = nats_conn_id
        self.subject = subject
        self.message = message

    def execute(self, context):
        hook = NATSHook(nats_conn_id=self.nats_conn_id, subject=self.subject)
        hook.publish(self.message)


class NATSGetMessageOperator(BaseOperator):
    template_fields = ('subject',)
    ui_color = '#ff7f50'

    @apply_defaults
    def __init__(self, subject, nats_conn_id='nats_default', *args, **kwargs):
        super(NATSGetMessageOperator, self).__init__(*args, **kwargs)

        self.nats_conn_id = nats_conn_id
        self.subject = subject

    def execute(self, context):
        hook = NATSHook(nats_conn_id=self.nats_conn_id, subject=self.subject)
        message = hook.get_one_message()
        if message is not None:
            return message
        else:
            self.log.info('Message for subject: %s was not found', self.subject)

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.sail.mq.client.trace;

import org.sail.mq.common.topic.TopicValidator;

public class TraceConstants {

    public static final String GROUP_NAME_PREFIX = "_INNER_TRACE_PRODUCER";
    public static final char CONTENT_SPLITOR = (char) 1;
    public static final char FIELD_SPLITOR = (char) 2;
    public static final String TRACE_INSTANCE_NAME = "PID_CLIENT_INNER_TRACE_PRODUCER";
    public static final String TRACE_TOPIC_PREFIX = TopicValidator.SYSTEM_TOPIC_PREFIX + "TRACE_DATA_";
    public static final String TO_PREFIX = "To_";
    public static final String FROM_PREFIX = "From_";
    public static final String END_TRANSACTION = "EndTransaction";
    public static final String ROCKETMQ_SERVICE = "sailmq";
    public static final String ROCKETMQ_SUCCESS = "sailmq.success";
    public static final String ROCKETMQ_TAGS = "sailmq.tags";
    public static final String ROCKETMQ_KEYS = "sailmq.keys";
    public static final String ROCKETMQ_STORE_HOST = "sailmq.store_host";
    public static final String ROCKETMQ_BODY_LENGTH = "sailmq.body_length";
    public static final String ROCKETMQ_MSG_ID = "sailmq.mgs_id";
    public static final String ROCKETMQ_MSG_TYPE = "sailmq.mgs_type";
    public static final String ROCKETMQ_REGION_ID = "sailmq.region_id";
    public static final String ROCKETMQ_TRANSACTION_ID = "sailmq.transaction_id";
    public static final String ROCKETMQ_TRANSACTION_STATE = "sailmq.transaction_state";
    public static final String ROCKETMQ_IS_FROM_TRANSACTION_CHECK = "sailmq.is_from_transaction_check";
    public static final String ROCKETMQ_RETRY_TIMERS = "sailmq.retry_times";
}

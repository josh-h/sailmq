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
package org.sail.mq.broker.metrics;

public class BrokerMetricsConstant {
    public static final String OPEN_TELEMETRY_METER_NAME = "broker-meter";

    public static final String GAUGE_PROCESSOR_WATERMARK = "sailmq_processor_watermark";
    public static final String GAUGE_BROKER_PERMISSION = "sailmq_broker_permission";
    public static final String GAUGE_TOPIC_NUM = "sailmq_topic_number";
    public static final String GAUGE_CONSUMER_GROUP_NUM = "sailmq_consumer_group_number";

    public static final String COUNTER_MESSAGES_IN_TOTAL = "sailmq_messages_in_total";
    public static final String COUNTER_MESSAGES_OUT_TOTAL = "sailmq_messages_out_total";
    public static final String COUNTER_THROUGHPUT_IN_TOTAL = "sailmq_throughput_in_total";
    public static final String COUNTER_THROUGHPUT_OUT_TOTAL = "sailmq_throughput_out_total";
    public static final String HISTOGRAM_MESSAGE_SIZE = "sailmq_message_size";
    public static final String HISTOGRAM_TOPIC_CREATE_EXECUTE_TIME = "sailmq_topic_create_execution_time";
    public static final String HISTOGRAM_CONSUMER_GROUP_CREATE_EXECUTE_TIME = "sailmq_consumer_group_create_execution_time";

    public static final String GAUGE_PRODUCER_CONNECTIONS = "sailmq_producer_connections";
    public static final String GAUGE_CONSUMER_CONNECTIONS = "sailmq_consumer_connections";

    public static final String GAUGE_CONSUMER_LAG_MESSAGES = "sailmq_consumer_lag_messages";
    public static final String GAUGE_CONSUMER_LAG_LATENCY = "sailmq_consumer_lag_latency";
    public static final String GAUGE_CONSUMER_INFLIGHT_MESSAGES = "sailmq_consumer_inflight_messages";
    public static final String GAUGE_CONSUMER_QUEUEING_LATENCY = "sailmq_consumer_queueing_latency";
    public static final String GAUGE_CONSUMER_READY_MESSAGES = "sailmq_consumer_ready_messages";
    public static final String COUNTER_CONSUMER_SEND_TO_DLQ_MESSAGES_TOTAL = "sailmq_send_to_dlq_messages_total";

    public static final String COUNTER_COMMIT_MESSAGES_TOTAL = "sailmq_commit_messages_total";
    public static final String COUNTER_ROLLBACK_MESSAGES_TOTAL = "sailmq_rollback_messages_total";
    public static final String HISTOGRAM_FINISH_MSG_LATENCY = "sailmq_finish_message_latency";
    public static final String GAUGE_HALF_MESSAGES = "sailmq_half_messages";

    public static final String LABEL_CLUSTER_NAME = "cluster";
    public static final String LABEL_NODE_TYPE = "node_type";
    public static final String NODE_TYPE_BROKER = "broker";
    public static final String LABEL_NODE_ID = "node_id";
    public static final String LABEL_AGGREGATION = "aggregation";
    public static final String AGGREGATION_DELTA = "delta";
    public static final String LABEL_PROCESSOR = "processor";

    public static final String LABEL_TOPIC = "topic";
    public static final String LABEL_INVOCATION_STATUS = "invocation_status";
    public static final String LABEL_IS_RETRY = "is_retry";
    public static final String LABEL_IS_SYSTEM = "is_system";
    public static final String LABEL_CONSUMER_GROUP = "consumer_group";
    public static final String LABEL_MESSAGE_TYPE = "message_type";
    public static final String LABEL_LANGUAGE = "language";
    public static final String LABEL_VERSION = "version";
    public static final String LABEL_CONSUME_MODE = "consume_mode";
}

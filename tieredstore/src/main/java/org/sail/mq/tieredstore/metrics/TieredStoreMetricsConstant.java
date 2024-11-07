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
package org.sail.mq.tieredstore.metrics;

public class TieredStoreMetricsConstant {
    public static final String HISTOGRAM_API_LATENCY = "sailmq_tiered_store_api_latency";
    public static final String HISTOGRAM_PROVIDER_RPC_LATENCY = "sailmq_tiered_store_provider_rpc_latency";
    public static final String HISTOGRAM_UPLOAD_BYTES = "sailmq_tiered_store_provider_upload_bytes";
    public static final String HISTOGRAM_DOWNLOAD_BYTES = "sailmq_tiered_store_provider_download_bytes";

    public static final String GAUGE_DISPATCH_BEHIND = "sailmq_tiered_store_dispatch_behind";
    public static final String GAUGE_DISPATCH_LATENCY = "sailmq_tiered_store_dispatch_latency";
    public static final String COUNTER_MESSAGES_DISPATCH_TOTAL = "sailmq_tiered_store_messages_dispatch_total";
    public static final String COUNTER_MESSAGES_OUT_TOTAL = "sailmq_tiered_store_messages_out_total";
    public static final String COUNTER_GET_MESSAGE_FALLBACK_TOTAL = "sailmq_tiered_store_get_message_fallback_total";

    public static final String GAUGE_CACHE_COUNT = "sailmq_tiered_store_read_ahead_cache_count";
    public static final String GAUGE_CACHE_BYTES = "sailmq_tiered_store_read_ahead_cache_bytes";
    public static final String COUNTER_CACHE_ACCESS = "sailmq_tiered_store_read_ahead_cache_access_total";
    public static final String COUNTER_CACHE_HIT = "sailmq_tiered_store_read_ahead_cache_hit_total";

    public static final String GAUGE_STORAGE_MESSAGE_RESERVE_TIME = "sailmq_storage_message_reserve_time";

    public static final String LABEL_OPERATION = "operation";
    public static final String LABEL_SUCCESS = "success";

    public static final String LABEL_PATH = "path";
    public static final String LABEL_TOPIC = "topic";
    public static final String LABEL_GROUP = "group";
    public static final String LABEL_QUEUE_ID = "queue_id";
    public static final String LABEL_FILE_TYPE = "file_type";

    // blob constants
    public static final String STORAGE_MEDIUM_BLOB = "blob";

    public static final String OPERATION_API_GET_MESSAGE = "get_message";
    public static final String OPERATION_API_GET_EARLIEST_MESSAGE_TIME = "get_earliest_message_time";
    public static final String OPERATION_API_GET_TIME_BY_OFFSET = "get_time_by_offset";
    public static final String OPERATION_API_GET_OFFSET_BY_TIME = "get_offset_by_time";
    public static final String OPERATION_API_QUERY_MESSAGE = "query_message";
}

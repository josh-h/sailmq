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
package org.sail.mq.client.impl.consumer;

import org.sail.mq.client.impl.factory.MQClientInstance;
import org.sail.mq.common.ServiceThread;
import org.sail.mq.logging.org.slf4j.Logger;
import org.sail.mq.logging.org.slf4j.LoggerFactory;

public class RebalanceService extends ServiceThread {
    private static long waitInterval =
        Long.parseLong(System.getProperty(
            "sailmq.client.rebalance.waitInterval", "20000"));
    private static long minInterval =
        Long.parseLong(System.getProperty(
            "sailmq.client.rebalance.minInterval", "1000"));
    private final Logger log = LoggerFactory.getLogger(RebalanceService.class);
    private final MQClientInstance mqClientFactory;
    private long lastRebalanceTimestamp = System.currentTimeMillis();

    public RebalanceService(MQClientInstance mqClientFactory) {
        this.mqClientFactory = mqClientFactory;
    }

    @Override
    public void run() {
        log.info(this.getServiceName() + " service started");

        long realWaitInterval = waitInterval;
        while (!this.isStopped()) {
            this.waitForRunning(realWaitInterval);

            long interval = System.currentTimeMillis() - lastRebalanceTimestamp;
            if (interval < minInterval) {
                realWaitInterval = minInterval - interval;
            } else {
                boolean balanced = this.mqClientFactory.doRebalance();
                realWaitInterval = balanced ? waitInterval : minInterval;
                lastRebalanceTimestamp = System.currentTimeMillis();
            }
        }

        log.info(this.getServiceName() + " service end");
    }

    @Override
    public String getServiceName() {
        return RebalanceService.class.getSimpleName();
    }
}

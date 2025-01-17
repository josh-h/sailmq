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
package org.sail.mq.broker.latency;

import java.util.List;
import java.util.ArrayList;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import org.sail.mq.broker.BrokerController;
import org.sail.mq.common.AbstractBrokerRunnable;
import org.sail.mq.common.Pair;
import org.sail.mq.common.ThreadFactoryImpl;
import org.sail.mq.common.UtilAll;
import org.sail.mq.common.constant.LoggerName;
import org.sail.mq.common.future.FutureTaskExt;
import org.sail.mq.common.utils.ThreadUtils;
import org.sail.mq.logging.org.slf4j.Logger;
import org.sail.mq.logging.org.slf4j.LoggerFactory;
import org.sail.mq.remoting.netty.RequestTask;
import org.sail.mq.remoting.protocol.RemotingSysResponseCode;

/**
 * BrokerFastFailure will cover {@link BrokerController#getSendThreadPoolQueue()} and {@link
 * BrokerController#getPullThreadPoolQueue()}
 */
public class BrokerFastFailure {
    private static final Logger LOGGER = LoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);
    private final ScheduledExecutorService scheduledExecutorService;
    private final BrokerController brokerController;

    private volatile long jstackTime = System.currentTimeMillis();

    private final List<Pair<BlockingQueue<Runnable>, Supplier<Long>>> cleanExpiredRequestQueueList = new ArrayList<>();

    public BrokerFastFailure(final BrokerController brokerController) {
        this.brokerController = brokerController;
        initCleanExpiredRequestQueueList();
        this.scheduledExecutorService = ThreadUtils.newScheduledThreadPool(1,
            new ThreadFactoryImpl("BrokerFastFailureScheduledThread", true,
                brokerController == null ? null : brokerController.getBrokerConfig()));
    }

    private void initCleanExpiredRequestQueueList() {
        cleanExpiredRequestQueueList.add(new Pair<>(this.brokerController.getSendThreadPoolQueue(), () -> this.brokerController.getBrokerConfig().getWaitTimeMillsInSendQueue()));
        cleanExpiredRequestQueueList.add(new Pair<>(this.brokerController.getPullThreadPoolQueue(), () -> this.brokerController.getBrokerConfig().getWaitTimeMillsInPullQueue()));
        cleanExpiredRequestQueueList.add(new Pair<>(this.brokerController.getLitePullThreadPoolQueue(), () -> this.brokerController.getBrokerConfig().getWaitTimeMillsInLitePullQueue()));
        cleanExpiredRequestQueueList.add(new Pair<>(this.brokerController.getHeartbeatThreadPoolQueue(), () -> this.brokerController.getBrokerConfig().getWaitTimeMillsInHeartbeatQueue()));
        cleanExpiredRequestQueueList.add(new Pair<>(this.brokerController.getEndTransactionThreadPoolQueue(), () -> this.brokerController.getBrokerConfig().getWaitTimeMillsInTransactionQueue()));
        cleanExpiredRequestQueueList.add(new Pair<>(this.brokerController.getAckThreadPoolQueue(), () -> this.brokerController.getBrokerConfig().getWaitTimeMillsInAckQueue()));
        cleanExpiredRequestQueueList.add(new Pair<>(this.brokerController.getAdminBrokerThreadPoolQueue(), () -> this.brokerController.getBrokerConfig().getWaitTimeMillsInAdminBrokerQueue()));
    }

    public static RequestTask castRunnable(final Runnable runnable) {
        try {
            if (runnable instanceof FutureTaskExt) {
                FutureTaskExt object = (FutureTaskExt) runnable;
                return (RequestTask) object.getRunnable();
            }
        } catch (Throwable e) {
            LOGGER.error(String.format("castRunnable exception, %s", runnable.getClass().getName()), e);
        }

        return null;
    }

    public void start() {
        this.scheduledExecutorService.scheduleAtFixedRate(new AbstractBrokerRunnable(this.brokerController.getBrokerConfig()) {
            @Override
            public void run0() {
                if (brokerController.getBrokerConfig().isBrokerFastFailureEnable()) {
                    cleanExpiredRequest();
                }
            }
        }, 1000, 10, TimeUnit.MILLISECONDS);
    }

    private void cleanExpiredRequest() {

        while (this.brokerController.getMessageStore().isOSPageCacheBusy()) {
            try {
                if (!this.brokerController.getSendThreadPoolQueue().isEmpty()) {
                    final Runnable runnable = this.brokerController.getSendThreadPoolQueue().poll(0, TimeUnit.SECONDS);
                    if (null == runnable) {
                        break;
                    }

                    final RequestTask rt = castRunnable(runnable);
                    if (rt != null) {
                        rt.returnResponse(RemotingSysResponseCode.SYSTEM_BUSY, String.format(
                            "[PCBUSY_CLEAN_QUEUE]broker busy, start flow control for a while, period in queue: %sms, "
                                + "size of queue: %d",
                            System.currentTimeMillis() - rt.getCreateTimestamp(),
                            this.brokerController.getSendThreadPoolQueue().size()));
                    }
                } else {
                    break;
                }
            } catch (Throwable ignored) {
            }
        }

        for (Pair<BlockingQueue<Runnable>, Supplier<Long>> pair : cleanExpiredRequestQueueList) {
            cleanExpiredRequestInQueue(pair.getObject1(), pair.getObject2().get());
        }
    }

    void cleanExpiredRequestInQueue(final BlockingQueue<Runnable> blockingQueue, final long maxWaitTimeMillsInQueue) {
        while (true) {
            try {
                if (!blockingQueue.isEmpty()) {
                    final Runnable runnable = blockingQueue.peek();
                    if (null == runnable) {
                        break;
                    }
                    final RequestTask rt = castRunnable(runnable);
                    if (rt == null || rt.isStopRun()) {
                        break;
                    }

                    final long behind = System.currentTimeMillis() - rt.getCreateTimestamp();
                    if (behind >= maxWaitTimeMillsInQueue) {
                        if (blockingQueue.remove(runnable)) {
                            rt.setStopRun(true);
                            rt.returnResponse(RemotingSysResponseCode.SYSTEM_BUSY, String.format("[TIMEOUT_CLEAN_QUEUE]broker busy, start flow control for a while, period in queue: %sms, size of queue: %d", behind, blockingQueue.size()));
                            if (System.currentTimeMillis() - jstackTime > 15000) {
                                jstackTime = System.currentTimeMillis();
                                LOGGER.warn("broker jstack \n " + UtilAll.jstack());
                            }
                        }
                    } else {
                        break;
                    }
                } else {
                    break;
                }
            } catch (Throwable ignored) {
            }
        }
    }

    public synchronized void addCleanExpiredRequestQueue(BlockingQueue<Runnable> cleanExpiredRequestQueue,
        Supplier<Long> maxWaitTimeMillsInQueueSupplier) {
        cleanExpiredRequestQueueList.add(new Pair<>(cleanExpiredRequestQueue, maxWaitTimeMillsInQueueSupplier));
    }

    public void shutdown() {
        this.scheduledExecutorService.shutdown();
    }
}

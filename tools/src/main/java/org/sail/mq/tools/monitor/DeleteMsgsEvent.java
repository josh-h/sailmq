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

package org.sail.mq.tools.monitor;

import org.sail.mq.remoting.protocol.topic.OffsetMovedEvent;

public class DeleteMsgsEvent {
    private OffsetMovedEvent offsetMovedEvent;
    private long eventTimestamp;

    public OffsetMovedEvent getOffsetMovedEvent() {
        return offsetMovedEvent;
    }

    public void setOffsetMovedEvent(OffsetMovedEvent offsetMovedEvent) {
        this.offsetMovedEvent = offsetMovedEvent;
    }

    public long getEventTimestamp() {
        return eventTimestamp;
    }

    public void setEventTimestamp(long eventTimestamp) {
        this.eventTimestamp = eventTimestamp;
    }

    @Override
    public String toString() {
        return "DeleteMsgsEvent [offsetMovedEvent=" + offsetMovedEvent + ", eventTimestamp=" + eventTimestamp
            + "]";
    }
}

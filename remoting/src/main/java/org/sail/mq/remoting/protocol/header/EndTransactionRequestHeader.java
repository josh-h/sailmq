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

package org.sail.mq.remoting.protocol.header;

import com.google.common.base.MoreObjects;
import org.sail.mq.common.action.Action;
import org.sail.mq.common.action.SailMQAction;
import org.sail.mq.common.resource.ResourceType;
import org.sail.mq.common.resource.SailMQResource;
import org.sail.mq.common.sysflag.MessageSysFlag;
import org.sail.mq.remoting.annotation.CFNotNull;
import org.sail.mq.remoting.annotation.CFNullable;
import org.sail.mq.remoting.exception.RemotingCommandException;
import org.sail.mq.remoting.protocol.RequestCode;
import org.sail.mq.remoting.rpc.RpcRequestHeader;

@SailMQAction(value = RequestCode.END_TRANSACTION, action = Action.PUB)
public class EndTransactionRequestHeader extends RpcRequestHeader {
    @SailMQResource(ResourceType.TOPIC)
    private String topic;
    @CFNotNull
    private String producerGroup;
    @CFNotNull
    private Long tranStateTableOffset;
    @CFNotNull
    private Long commitLogOffset;
    @CFNotNull
    private Integer commitOrRollback; // TRANSACTION_COMMIT_TYPE
    // TRANSACTION_ROLLBACK_TYPE
    // TRANSACTION_NOT_TYPE

    @CFNullable
    private Boolean fromTransactionCheck = false;

    @CFNotNull
    private String msgId;

    private String transactionId;

    @Override
    public void checkFields() throws RemotingCommandException {
        if (MessageSysFlag.TRANSACTION_NOT_TYPE == this.commitOrRollback) {
            return;
        }

        if (MessageSysFlag.TRANSACTION_COMMIT_TYPE == this.commitOrRollback) {
            return;
        }

        if (MessageSysFlag.TRANSACTION_ROLLBACK_TYPE == this.commitOrRollback) {
            return;
        }

        throw new RemotingCommandException("commitOrRollback field wrong");
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public String getProducerGroup() {
        return producerGroup;
    }

    public void setProducerGroup(String producerGroup) {
        this.producerGroup = producerGroup;
    }

    public Long getTranStateTableOffset() {
        return tranStateTableOffset;
    }

    public void setTranStateTableOffset(Long tranStateTableOffset) {
        this.tranStateTableOffset = tranStateTableOffset;
    }

    public Long getCommitLogOffset() {
        return commitLogOffset;
    }

    public void setCommitLogOffset(Long commitLogOffset) {
        this.commitLogOffset = commitLogOffset;
    }

    public Integer getCommitOrRollback() {
        return commitOrRollback;
    }

    public void setCommitOrRollback(Integer commitOrRollback) {
        this.commitOrRollback = commitOrRollback;
    }

    public Boolean getFromTransactionCheck() {
        return fromTransactionCheck;
    }

    public void setFromTransactionCheck(Boolean fromTransactionCheck) {
        this.fromTransactionCheck = fromTransactionCheck;
    }

    public String getMsgId() {
        return msgId;
    }

    public void setMsgId(String msgId) {
        this.msgId = msgId;
    }

    public String getTransactionId() {
        return transactionId;
    }

    public void setTransactionId(String transactionId) {
        this.transactionId = transactionId;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("producerGroup", producerGroup)
            .add("tranStateTableOffset", tranStateTableOffset)
            .add("commitLogOffset", commitLogOffset)
            .add("commitOrRollback", commitOrRollback)
            .add("fromTransactionCheck", fromTransactionCheck)
            .add("msgId", msgId)
            .add("transactionId", transactionId)
            .toString();
    }
}

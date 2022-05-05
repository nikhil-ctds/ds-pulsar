/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pulsar.broker.systopic;

import com.google.common.collect.Sets;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.pulsar.broker.admin.impl.BrokersBase;
import org.apache.pulsar.broker.namespace.NamespaceService;
import org.apache.pulsar.broker.service.BrokerTestBase;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.common.naming.TopicName;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import java.util.ArrayList;

@Test(groups = "broker")
public class PartitionedSystemTopicTest extends BrokerTestBase {

    static final int PARTITIONS = 5;

    @BeforeMethod
    @Override
    protected void setup() throws Exception {
        resetConfig();
        conf.setAllowAutoTopicCreation(false);
        conf.setAllowAutoTopicCreationType("partitioned");
        conf.setDefaultNumPartitions(PARTITIONS);

        conf.setSystemTopicEnabled(true);
        conf.setTopicLevelPoliciesEnabled(true);

        super.baseSetup();
    }

    @AfterMethod(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test
    public void testHealthCheckTopicNotOffload() throws Exception {
        final String heartbeatNamespace = NamespaceService.getHeartbeatNamespace(pulsar.getAdvertisedAddress(),
                pulsar.getConfig());
        TopicName topicName = TopicName.get("persistent://" + heartbeatNamespace
                        + "/" + BrokersBase.HEALTH_CHECK_TOPIC_SUFFIX);
        PersistentTopic persistentTopic = (PersistentTopic) pulsar.getBrokerService()
                .getTopic(topicName.toString(), true).get().get();
        admin.brokers().healthcheck();
        admin.topics().triggerOffload(topicName.toString(), MessageId.earliest);
        Assert.assertEquals(admin.topics().getStats(topicName.toString()).getMsgInCounter(), 1);
        Assert.assertEquals(persistentTopic.getManagedLedger().getOffloadedSize(), 0);
    }
}

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.geode.internal.tcp;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Properties;

import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.distributed.ConfigurationProperties;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.distributed.internal.DistributionConfigImpl;
import org.apache.geode.distributed.internal.DistributionImpl;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.test.dunit.DistributedTestUtils;
import org.apache.geode.test.dunit.rules.DistributedRule;

public class TCPConduitBareBonesDUnitTest {

  @Rule
  public DistributedRule distributedRule = new DistributedRule(0);

  @Test
  public void testShortTermSharedConnections() throws Exception {
    // create and close many shared, ordered connections. Also see CloseConnectionTest,
    // which ensures that the outgoing socket in a shared connection is properly closed
    Properties properties = new Properties();
    properties.put(ConfigurationProperties.LOG_LEVEL, "fine");
    properties.put(ConfigurationProperties.LOCATORS, DistributedTestUtils.getLocators());
    DistributionConfig configuration = new DistributionConfigImpl(properties);
    InternalDistributedSystem distributedSystem =
        (InternalDistributedSystem) InternalDistributedSystem.connect(configuration.toProperties());
    TCPConduit conduit =
        ((DistributionImpl) distributedSystem.getDistributionManager().getDistribution())
            .getDirectChannel().getConduit();

    InternalDistributedMember memberID =
        distributedSystem.getDistributionManager().getDistribution().getView().getCreator();
    ConnectionTable.threadWantsSharedResources();

    try {
      for (int i = 0; i < 1000; i++) {
        long buffersSize =
            distributedSystem.getDistributionManager().getStats().getSenderBufferSize(true);
        // connect to the dunit locator. The first iteration will get the Connection that was formed
        // during startup, but we'll close it and start creating new ones.
        Connection connection =
            conduit.getConnection(memberID, true, false, System.currentTimeMillis(), 15000, 0);
        System.out.println("Test iteration " + i + ": " + connection);
        connection.requestClose("for test");
        // the connection should be stopped at this point
        assertThat(connection.isReceiverStopped()).isTrue();
        // make sure there are no double releases of ByteBuffers
        assertThat(distributedSystem.getDistributionManager().getStats().getSenderBufferSize(true))
            .isEqualTo(buffersSize);
      }
    } finally {
      distributedSystem.disconnect();
    }
  }
}

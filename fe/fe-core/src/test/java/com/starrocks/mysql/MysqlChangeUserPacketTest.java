// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.mysql;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

public class MysqlChangeUserPacketTest {
    private ByteBuffer byteBuffer;

    @BeforeEach
    public void setUp() {
        MysqlSerializer serializer = MysqlSerializer.newInstance();
        // code
        serializer.writeInt1(17);
        // user name
        serializer.writeNulTerminateString("testUser");
        // plugin data
        serializer.writeInt1(20);
        byte[] buf = new byte[20];
        for (int i = 0; i < 20; ++i) {
            buf[i] = (byte) ('a' + i);
        }
        serializer.writeBytes(buf);
        // database
        serializer.writeNulTerminateString("testDb");
        // character set
        serializer.writeInt2(33);
        //plugin
        serializer.writeNulTerminateString("");

        //conn attribute
        serializer.writeVInt(10);
        serializer.writeLenEncodedString("key");
        serializer.writeLenEncodedString("value");

        byteBuffer = serializer.toByteBuffer();
    }

    @Test
    public void testRead() {
        MysqlChangeUserPacket packet = new MysqlChangeUserPacket(MysqlCapability.DEFAULT_CAPABILITY);
        Assertions.assertTrue(packet.readFrom(byteBuffer));
        Assertions.assertEquals("testUser", packet.getUser());
        Assertions.assertEquals("testDb", packet.getDb());
        Assertions.assertEquals("value", packet.getConnectAttributes().get("key"));
    }

}
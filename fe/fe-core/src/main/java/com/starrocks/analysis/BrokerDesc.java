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

package com.starrocks.analysis;

import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import com.google.gson.annotations.SerializedName;
import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;
import com.starrocks.common.util.PrintableMap;
import com.starrocks.sql.ast.LoadStmt;
import com.starrocks.sql.parser.NodePosition;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;

// Broker descriptor
//
// Broker example:
// WITH BROKER "broker0"
// (
//   "username" = "user0",
//   "password" = "password0"
// )
public class BrokerDesc implements ParseNode, Writable {
    @SerializedName("n")
    private String name;
    @SerializedName("p")
    private Map<String, String> properties;

    private final NodePosition pos;

    // Only used for recovery
    private BrokerDesc() {
        pos = NodePosition.ZERO;
    }

    public BrokerDesc(Map<String, String> properties) {
        this(properties, NodePosition.ZERO);
    }

    public BrokerDesc(String name, Map<String, String> properties) {
        this(name, properties, NodePosition.ZERO);
    }
    public BrokerDesc(String name, Map<String, String> properties, NodePosition pos) {
        this.pos = pos;
        this.name = name;
        this.properties = properties;
        if (this.properties == null) {
            this.properties = Maps.newHashMap();
        }
    }

    public BrokerDesc(Map<String, String> properties, NodePosition pos) {
        this.pos = pos;
        this.name = "";
        this.properties = properties;
        if (this.properties == null) {
            this.properties = Maps.newHashMap();
        }
    }

    public boolean hasBroker() {
        return !Strings.isNullOrEmpty(name);
    }

    public String getName() {
        return name;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public String getMergeConditionStr() {
        if (properties.containsKey(LoadStmt.MERGE_CONDITION)) {
            return properties.get(LoadStmt.MERGE_CONDITION);
        }
        return "";
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, name);
        out.writeInt(properties.size());
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            Text.writeString(out, entry.getKey());
            Text.writeString(out, entry.getValue());
        }
    }

    public void readFields(DataInput in) throws IOException {
        name = Text.readString(in);
        int size = in.readInt();
        properties = Maps.newHashMap();
        for (int i = 0; i < size; ++i) {
            final String key = Text.readString(in);
            final String val = Text.readString(in);
            properties.put(key, val);
        }
    }

    public static BrokerDesc read(DataInput in) throws IOException {
        BrokerDesc desc = new BrokerDesc();
        desc.readFields(in);
        return desc;
    }

    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(" WITH BROKER ").append(name);
        if (properties != null && !properties.isEmpty()) {
            PrintableMap<String, String> printableMap = new PrintableMap<>(properties, " = ", true, false, true);
            sb.append(" (").append(printableMap.toString()).append(")");
        }
        return sb.toString();
    }

    @Override
    public NodePosition getPos() {
        return pos;
    }
}

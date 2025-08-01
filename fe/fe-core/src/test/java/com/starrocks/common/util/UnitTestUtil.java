// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/test/java/org/apache/doris/common/util/UnitTestUtil.java

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

package com.starrocks.common.util;

import com.google.common.collect.Lists;
import com.starrocks.catalog.AggregateType;
import com.starrocks.catalog.BaseTableInfo;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.DataProperty;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.KeysType;
import com.starrocks.catalog.LocalTablet;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.MaterializedIndex.IndexState;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.RandomDistributionInfo;
import com.starrocks.catalog.Replica;
import com.starrocks.catalog.Replica.ReplicaState;
import com.starrocks.catalog.SinglePartitionInfo;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.TabletMeta;
import com.starrocks.catalog.Type;
import com.starrocks.catalog.View;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.system.Backend;
import com.starrocks.thrift.TDisk;
import com.starrocks.thrift.TStorageMedium;
import com.starrocks.thrift.TStorageType;
import com.starrocks.thrift.TTabletType;
import org.junit.jupiter.api.Assertions;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

// for unit test
public class UnitTestUtil {
    public static final String DB_NAME = "testDb";
    public static final String TABLE_NAME = "testTable";
    public static final String MATERIALIZED_VIEW_NAME = "testMV";
    public static final String PARTITION_NAME = "testTable";
    public static final int SCHEMA_HASH = 0;
    public static final String VIEW_NAME = "testView";

    public static Database createDb(long dbId, long tableId, long partitionId, long indexId,
                                    long tabletId, long backendId, long version, KeysType type) {
        // GlobalStateMgr.getCurrentState().getTabletInvertedIndex().clear();

        // table
        OlapTable table = createOlapTable(dbId, tableId, partitionId, indexId, tabletId,
                backendId, version, type, Table.TableType.OLAP);

        // db
        Database db = new Database(dbId, DB_NAME);
        db.registerTableUnlocked(table);
        return db;
    }

    public static Database createDbByName(long dbId, long tableId, long partitionId, long indexId,
                                    long tabletId, long backendId, long version, KeysType type, String dbName,
                                    String tableName) {
        // GlobalStateMgr.getCurrentState().getTabletInvertedIndex().clear();

        // table
        OlapTable table = createOlapTableByName(dbId, tableId, partitionId, indexId, tabletId,
                backendId, version, type, Table.TableType.OLAP, tableName);

        // db
        Database db = new Database(dbId, dbName);
        db.registerTableUnlocked(table);
        return db;
    }

    public static OlapTable createOlapTableByName(long dbId, long tableId, long partitionId, long indexId,
                                            long tabletId, long backendId, long version, KeysType type,
                                            Table.TableType tableType, String tableName) {
        // replica
        long replicaId = 0;
        Replica replica1 = new Replica(replicaId, backendId, ReplicaState.NORMAL, version, 0);
        Replica replica2 = new Replica(replicaId + 1, backendId + 1, ReplicaState.NORMAL, version, 0);
        Replica replica3 = new Replica(replicaId + 2, backendId + 2, ReplicaState.NORMAL, version, 0);

        // tablet
        LocalTablet tablet = new LocalTablet(tabletId);

        // index
        MaterializedIndex index = new MaterializedIndex(indexId, IndexState.NORMAL);
        TabletMeta tabletMeta = new TabletMeta(dbId, tableId, partitionId, indexId, TStorageMedium.HDD);
        index.addTablet(tablet, tabletMeta);

        tablet.addReplica(replica1);
        tablet.addReplica(replica2);
        tablet.addReplica(replica3);

        // partition
        RandomDistributionInfo distributionInfo = new RandomDistributionInfo(10);
        Partition partition = new Partition(partitionId, partitionId + 100,  PARTITION_NAME, index, distributionInfo);

        // columns
        List<Column> columns = new ArrayList<Column>();
        Column temp = new Column("k1", Type.INT);
        temp.setIsKey(true);
        columns.add(temp);
        temp = new Column("k2", Type.INT);
        temp.setIsKey(true);
        columns.add(temp);
        columns.add(new Column("v", Type.DOUBLE, false, AggregateType.SUM, "0", ""));

        List<Column> keysColumn = new ArrayList<Column>();
        temp = new Column("k1", Type.INT);
        temp.setIsKey(true);
        keysColumn.add(temp);
        temp = new Column("k2", Type.INT);
        temp.setIsKey(true);
        keysColumn.add(temp);

        // table
        PartitionInfo partitionInfo = new SinglePartitionInfo();
        partitionInfo.setDataProperty(partitionId, DataProperty.DEFAULT_DATA_PROPERTY);
        partitionInfo.setReplicationNum(partitionId, (short) 3);
        partitionInfo.setIsInMemory(partitionId, false);
        partitionInfo.setTabletType(partitionId, TTabletType.TABLET_TYPE_DISK);
        if (tableType == Table.TableType.MATERIALIZED_VIEW) {
            MaterializedView.MvRefreshScheme mvRefreshScheme = new MaterializedView.MvRefreshScheme();
            MaterializedView mv = new MaterializedView(tableId, dbId, MATERIALIZED_VIEW_NAME, columns,
                    type, partitionInfo, distributionInfo, mvRefreshScheme);
            Deencapsulation.setField(mv, "baseIndexId", indexId);
            mv.addPartition(partition);
            mv.setIndexMeta(indexId, tableName, columns, 0, SCHEMA_HASH, (short) 1, TStorageType.COLUMN,
                    type);
            return mv;
        } else {
            OlapTable table = new OlapTable(tableId, tableName, columns,
                    type, partitionInfo, distributionInfo);
            Deencapsulation.setField(table, "baseIndexId", indexId);
            table.addPartition(partition);
            table.setIndexMeta(indexId, tableName, columns, 0, SCHEMA_HASH, (short) 1, TStorageType.COLUMN,
                    type);
            return table;
        }
    }

    public static OlapTable createOlapTable(long dbId, long tableId, long partitionId, long indexId,
                                            long tabletId, long backendId, long version, KeysType type,
                                            Table.TableType tableType) {
        return createOlapTableWithName(TABLE_NAME, dbId, tableId, partitionId, indexId,
                tabletId, backendId, version, type,
                tableType);
    }

    public static OlapTable createOlapTableWithName(String tableName, long dbId, long tableId, long partitionId,
                                                    long indexId, long tabletId, long backendId, long version,
                                                    KeysType type, Table.TableType tableType) {
        // replica
        long replicaId = 0;
        Replica replica1 = new Replica(replicaId, backendId, ReplicaState.NORMAL, version, 0);
        Replica replica2 = new Replica(replicaId + 1, backendId + 1, ReplicaState.NORMAL, version, 0);
        Replica replica3 = new Replica(replicaId + 2, backendId + 2, ReplicaState.NORMAL, version, 0);

        // tablet
        LocalTablet tablet = new LocalTablet(tabletId);

        // index
        MaterializedIndex index = new MaterializedIndex(indexId, IndexState.NORMAL);
        TabletMeta tabletMeta = new TabletMeta(dbId, tableId, partitionId, indexId, TStorageMedium.HDD);
        index.addTablet(tablet, tabletMeta);

        tablet.addReplica(replica1);
        tablet.addReplica(replica2);
        tablet.addReplica(replica3);

        // partition
        RandomDistributionInfo distributionInfo = new RandomDistributionInfo(10);
        Partition partition = new Partition(partitionId, partitionId + 100, PARTITION_NAME, index, distributionInfo);

        // columns
        List<Column> columns = new ArrayList<Column>();
        Column temp = new Column("k1", Type.INT);
        temp.setIsKey(true);
        columns.add(temp);
        temp = new Column("k2", Type.INT);
        temp.setIsKey(true);
        columns.add(temp);
        columns.add(new Column("v", Type.DOUBLE, false, AggregateType.SUM, "0", ""));

        List<Column> keysColumn = new ArrayList<Column>();
        temp = new Column("k1", Type.INT);
        temp.setIsKey(true);
        keysColumn.add(temp);
        temp = new Column("k2", Type.INT);
        temp.setIsKey(true);
        keysColumn.add(temp);

        // table
        PartitionInfo partitionInfo = new SinglePartitionInfo();
        partitionInfo.setDataProperty(partitionId, DataProperty.DEFAULT_DATA_PROPERTY);
        partitionInfo.setReplicationNum(partitionId, (short) 3);
        partitionInfo.setIsInMemory(partitionId, false);
        partitionInfo.setTabletType(partitionId, TTabletType.TABLET_TYPE_DISK);

        if (tableType == Table.TableType.MATERIALIZED_VIEW) {
            MaterializedView.MvRefreshScheme mvRefreshScheme = new MaterializedView.MvRefreshScheme();
            MaterializedView mv = new MaterializedView(tableId, dbId, tableName, columns,
                    type, partitionInfo, distributionInfo, mvRefreshScheme);
            Deencapsulation.setField(mv, "baseIndexId", indexId);
            mv.addPartition(partition);
            mv.setIndexMeta(indexId, tableName, columns, 0, SCHEMA_HASH, (short) 1, TStorageType.COLUMN,
                    type);
            return mv;
        } else {
            OlapTable table = new OlapTable(tableId, tableName, columns,
                    type, partitionInfo, distributionInfo);
            Deencapsulation.setField(table, "baseIndexId", indexId);
            table.addPartition(partition);
            table.setIndexMeta(indexId, tableName, columns, 0, SCHEMA_HASH, (short) 1, TStorageType.COLUMN,
                    type);
            table.setType(Table.TableType.OLAP);
            return table;
        }
    }

    public static MaterializedView createMaterializedView(OlapTable baseTable, long dbId, long tableId, long partitionId,
                                                          long indexId,
                                                          long tabletId, long backendId, long version) {
        OlapTable table = createOlapTableWithName(MATERIALIZED_VIEW_NAME, dbId, tableId, partitionId, indexId, tabletId,
                backendId, version, KeysType.DUP_KEYS, Table.TableType.MATERIALIZED_VIEW);
        MaterializedView mv = (MaterializedView) table;

        // set mv's base table infos
        {
            List<BaseTableInfo> baseTableInfos = Lists.newArrayList();
            BaseTableInfo baseTableInfo1 = new BaseTableInfo(dbId, DB_NAME, baseTable.getName(), baseTable.getId());
            baseTableInfos.add(baseTableInfo1);
            mv.setBaseTableInfos(baseTableInfos);
            String defineSql = String.format("select * from %s.%s", DB_NAME, baseTable.getName());
            mv.setViewDefineSql(defineSql);
            mv.setSimpleDefineSql(defineSql);
        }

        // set base table's relative mvs
        {
            baseTable.getRelatedMaterializedViews().add(mv.getMvId());
        }
        return mv;
    }

    public static Database createDbWithMaterializedView(long dbId, long tableId, long partitionId, long indexId,
                                                        long tabletId, long backendId, long version, KeysType type) {
        // table
        OlapTable table = createOlapTable(dbId, tableId, partitionId, indexId, tabletId,
                backendId, version, type, Table.TableType.OLAP);

        OlapTable mv = createMaterializedView(table, dbId, tableId + 1, partitionId + 1, indexId + 1, tabletId + 1,
                backendId, version);
        // db
        Database db = new Database(dbId, DB_NAME);
        db.registerTableUnlocked(table);
        db.registerTableUnlocked(mv);

        return db;
    }
    public static Backend createBackend(long id, String host, int heartPort, int bePort, int httpPort) {
        Backend backend = new Backend(id, host, heartPort);
        backend.updateOnce(bePort, httpPort, 10000);
        return backend;
    }

    public static Backend createBackend(long id, String host, int heartPort, int bePort, int httpPort,
                                        long totalCapacityB, long availableCapacityB) {
        Backend backend = createBackend(id, host, heartPort, bePort, httpPort);
        Map<String, TDisk> backendDisks = new HashMap<String, TDisk>();
        String rootPath = "root_path";
        TDisk disk = new TDisk(rootPath, totalCapacityB, availableCapacityB, true);
        backendDisks.put(rootPath, disk);
        backend.updateDisks(backendDisks);
        return backend;
    }

    public static Method getPrivateMethod(Class c, String methodName, Class[] params) {
        Method method = null;
        try {
            method = c.getDeclaredMethod(methodName, params);
            method.setAccessible(true);
        } catch (NoSuchMethodException e) {
            Assertions.fail(e.getMessage());
        }
        return method;
    }

    public static Class getInnerClass(Class c, String className) {
        Class innerClass = null;
        for (Class tmpClass : c.getDeclaredClasses()) {
            if (tmpClass.getName().equals(className)) {
                innerClass = tmpClass;
                break;
            }
        }
        return innerClass;
    }

    public static View createTestView(long tableId) {
        List<Column> columns = new ArrayList<Column>();
        Column temp = new Column("k1", Type.INT);
        temp.setIsKey(false);
        columns.add(temp);
        temp = new Column("k2", Type.INT);
        temp.setIsKey(false);
        columns.add(temp);

        View view = new View(tableId, VIEW_NAME, columns);
        view.setType(Table.TableType.VIEW);
        return view;
    }

}

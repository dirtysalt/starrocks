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


package com.starrocks.persist;

import com.starrocks.alter.AlterJobMgr;
import com.starrocks.alter.MaterializedViewHandler;
import com.starrocks.alter.SchemaChangeHandler;
import com.starrocks.alter.SystemHandler;
import com.starrocks.catalog.AggregateType;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.DataProperty;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.KeysType;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.PrimitiveType;
import com.starrocks.catalog.RandomDistributionInfo;
import com.starrocks.catalog.ScalarType;
import com.starrocks.catalog.SinglePartitionInfo;
import com.starrocks.common.Config;
import com.starrocks.common.io.Text;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.thrift.TTabletType;
import mockit.Expectations;
import mockit.Injectable;
import mockit.Mocked;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.LinkedList;
import java.util.List;

public class ChangeMaterializedViewRefreshSchemeLogTest {

    private String fileName = "./ChangeMaterializedViewRefreshSchemeLogTest";

    @AfterEach
    public void tearDownDrop() {
        File file = new File(fileName);
        file.delete();
    }

    @Test
    public void testNormal(@Mocked GlobalStateMgr globalStateMgr,
                           @Injectable Database db) throws IOException {
        // 1. Write objects to file
        File file = new File(fileName);
        file.createNewFile();
        DataOutputStream out = new DataOutputStream(Files.newOutputStream(file.toPath()));

        List<Column> columns = new LinkedList<Column>();
        columns.add(new Column("k1", ScalarType.createType(PrimitiveType.TINYINT), true, null, "", ""));
        columns.add(new Column("k2", ScalarType.createType(PrimitiveType.SMALLINT), true, null, "", ""));
        columns.add(new Column("v1", ScalarType.createType(PrimitiveType.INT), false, AggregateType.SUM, "", ""));
        RandomDistributionInfo distributionInfo = new RandomDistributionInfo(10);
        PartitionInfo partitionInfo = new SinglePartitionInfo();
        partitionInfo.setDataProperty(1, DataProperty.DEFAULT_DATA_PROPERTY);
        partitionInfo.setReplicationNum(1, (short) 3);
        partitionInfo.setIsInMemory(1, false);
        partitionInfo.setTabletType(1, TTabletType.TABLET_TYPE_DISK);
        MaterializedView.MvRefreshScheme refreshScheme = new MaterializedView.MvRefreshScheme();
        refreshScheme.setMoment(MaterializedView.RefreshMoment.DEFERRED);
        final MaterializedView.AsyncRefreshContext asyncRefreshContext = refreshScheme.getAsyncRefreshContext();
        asyncRefreshContext.setStartTime(1655732457);
        asyncRefreshContext.setStep(1);
        asyncRefreshContext.setTimeUnit("DAY");
        MaterializedView materializedView = new MaterializedView(1000, 100, "mv_name", columns, KeysType.AGG_KEYS,
                partitionInfo, distributionInfo, refreshScheme);
        ChangeMaterializedViewRefreshSchemeLog changeLog =
                new ChangeMaterializedViewRefreshSchemeLog(materializedView);
        changeLog.write(out);
        out.flush();
        out.close();

        // 2. Read objects from file
        DataInputStream in = new DataInputStream(Files.newInputStream(file.toPath()));
        ChangeMaterializedViewRefreshSchemeLog readChangeLog = ChangeMaterializedViewRefreshSchemeLog.read(in);
        final MaterializedView.AsyncRefreshContext readChangeLogAsyncRefreshContext = readChangeLog.getAsyncRefreshContext();
        Assertions.assertEquals(readChangeLog.getRefreshType().name(), "ASYNC");
        Assertions.assertEquals(readChangeLogAsyncRefreshContext.getStartTime(), 1655732457);
        Assertions.assertEquals(readChangeLogAsyncRefreshContext.getTimeUnit(), "DAY");
        Assertions.assertEquals(readChangeLogAsyncRefreshContext.getStep(), 1);
        in.close();

        new Expectations() {
            {
                globalStateMgr.getCurrentState().getLocalMetastore().getDb(anyLong);
                result = db;

                globalStateMgr.getCurrentState().getLocalMetastore().getTable(anyLong, anyLong);
                result = materializedView;

                db.getId();
                result = anyLong;

                materializedView.getId();
                result = anyLong;
            }
        };
        new AlterJobMgr(null, null, null)
                .replayChangeMaterializedViewRefreshScheme(changeLog);

        Assertions.assertEquals(materializedView.getRefreshScheme().getMoment(), MaterializedView.RefreshMoment.DEFERRED);
    }

    @Test
    public void testFallBack() throws IOException {
        Config.ignore_materialized_view_error = true;
        String str = "bad data";
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        Text.writeString(new DataOutputStream(byteArrayOutputStream), str);
        byteArrayOutputStream.close();
        byte[] data = byteArrayOutputStream.toByteArray();
        DataInputStream in = new DataInputStream(new ByteArrayInputStream(data));
        ChangeMaterializedViewRefreshSchemeLog readChangeLog = ChangeMaterializedViewRefreshSchemeLog.read(in);
        Assertions.assertEquals(0, readChangeLog.getDbId());
        Config.ignore_materialized_view_error = false;
    }

    @Test
    public void testReplayWhenDbIsEmpty() {
        AlterJobMgr alterJobMgr = new AlterJobMgr(
                new SchemaChangeHandler(),
                new MaterializedViewHandler(),
                new SystemHandler());
        alterJobMgr.replayChangeMaterializedViewRefreshScheme(new ChangeMaterializedViewRefreshSchemeLog());
    }

}
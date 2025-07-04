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

package com.starrocks.clone;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Range;
import com.google.common.collect.Sets;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.DistributionInfo;
import com.starrocks.catalog.DynamicPartitionProperty;
import com.starrocks.catalog.ExpressionRangePartitionInfo;
import com.starrocks.catalog.ExpressionRangePartitionInfoV2;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.common.util.DynamicPartitionUtil;
import com.starrocks.common.util.TimeUtils;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.StmtExecutor;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.CreateMaterializedViewStatement;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.parser.SqlParser;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Mock;
import mockit.MockUp;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;

import static org.junit.jupiter.api.Assertions.fail;

public class DynamicPartitionSchedulerTest {
    private static final Logger LOG = LogManager.getLogger(DynamicPartitionSchedulerTest.class);

    private static ConnectContext connectContext;
    private static StarRocksAssert starRocksAssert;
    private static String T1;
    private static String T2;
    private static List<String> LIST_PARTITION_TABLES;

    private static String R1;
    private static String R2;
    private static List<String> RANGE_PARTITION_TABLES;

    @BeforeAll
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        UtFrameUtils.addMockBackend(10002);
        UtFrameUtils.addMockBackend(10003);
        // create connect context
        connectContext = UtFrameUtils.createDefaultCtx();

        // set default config for async mvs
        UtFrameUtils.setDefaultConfigForAsyncMVTest(connectContext);

        T1 = "CREATE TABLE t1 (\n" +
                " id BIGINT,\n" +
                " age SMALLINT,\n" +
                " dt VARCHAR(10) not null,\n" +
                " province VARCHAR(64) not null\n" +
                ")\n" +
                "PARTITION BY (province, dt) \n" +
                "DISTRIBUTED BY RANDOM\n";
        // table whose partitions have multi columns
        T2 = "CREATE TABLE t2 (\n" +
                " id BIGINT,\n" +
                " age SMALLINT,\n" +
                " dt VARCHAR(10) not null,\n" +
                " province VARCHAR(64) not null\n" +
                ")\n" +
                "PARTITION BY LIST (province, dt) (\n" +
                "     PARTITION p1 VALUES IN ((\"beijing\", \"2024-01-31\")),\n" +
                "     PARTITION p2 VALUES IN ((\"guangdong\", \"2024-01-31\")), \n" +
                "     PARTITION p3 VALUES IN ((\"beijing\", \"2024-02-01\")),\n" +
                "     PARTITION p4 VALUES IN ((\"guangdong\", \"2024-02-01\")) \n" +
                ")\n" +
                "DISTRIBUTED BY RANDOM\n";
        LIST_PARTITION_TABLES = ImmutableList.of(T1, T2);

        // range partition table
        R1 = "CREATE TABLE r1 \n" +
                "(\n" +
                "    dt date,\n" +
                "    k2 int,\n" +
                "    v1 int \n" +
                ")\n" +
                "PARTITION BY RANGE(dt)\n" +
                "(\n" +
                "    PARTITION p0 values [('2024-01-29'),('2024-01-30')),\n" +
                "    PARTITION p1 values [('2024-01-30'),('2024-01-31')),\n" +
                "    PARTITION p2 values [('2024-01-31'),('2024-02-01')),\n" +
                "    PARTITION p3 values [('2024-02-01'),('2024-02-02')) \n" +
                ")\n" +
                "DISTRIBUTED BY HASH(k2) BUCKETS 3\n" +
                "PROPERTIES('replication_num' = '1');";
        R2 = "CREATE TABLE r2 \n" +
                "(\n" +
                "    dt date,\n" +
                "    k2 int,\n" +
                "    v1 int \n" +
                ")\n" +
                "PARTITION BY date_trunc('day', dt)\n" +
                "DISTRIBUTED BY HASH(k2) BUCKETS 3\n" +
                "PROPERTIES('replication_num' = '1');";
        RANGE_PARTITION_TABLES = ImmutableList.of(R1, R2);

        starRocksAssert = new StarRocksAssert(connectContext);
        starRocksAssert.withDatabase("test").useDatabase("test");
    }


    public static void executeInsertSql(String sql) throws Exception {
        connectContext.setQueryId(UUIDUtil.genUUID());
        StatementBase statement = SqlParser.parseSingleStatement(sql, connectContext.getSessionVariable().getSqlMode());
        new StmtExecutor(connectContext, statement).execute();
    }

    private String toPartitionVal(String val) {
        return val == null ? "NULL" : String.format("'%s'", val);
    }

    private void addListPartition(String tbl, String pName, String pVal1, String pVal2) {
        String addPartitionSql = String.format("ALTER TABLE %s ADD PARTITION IF NOT EXISTS %s VALUES IN ((%s, %s))",
                tbl, pName, toPartitionVal(pVal1), toPartitionVal(pVal2));
        StatementBase stmt = SqlParser.parseSingleStatement(addPartitionSql, connectContext.getSessionVariable().getSqlMode());
        try {
            new StmtExecutor(connectContext, stmt).execute();
        } catch (Exception e) {
            Assertions.fail("add partition failed:" + e);
        }
    }

    private void withTableListPartitions(String tableName) {
        // Automatic partition creation is not supported in FE UTs
        //String insertSql = String.format("insert into %s values " +
        //        "(1, 1, '2024-01-01', 'beijing'), (1, 1, '2024-01-01', 'guangdong')," +
        //        "(2, 1, '2024-01-02', 'beijing'), (2, 1, '2024-01-02', 'guangdong');", tableName);
        //executeInsertSql(insertSql);
        addListPartition(tableName, "p1", "beijing", "2024-01-31");
        addListPartition(tableName, "p2", "guangdong", "2024-01-31");
        addListPartition(tableName, "p3", "beijing", "2024-02-01");
        addListPartition(tableName, "p4", "guangdong", "2024-02-01");
    }

    private void addRangePartition(String tbl, String pName, String pVal1, String pVal2) {
        // mock the check to ensure test can run
        new MockUp<ExpressionRangePartitionInfo>() {
            @Mock
            public boolean isAutomaticPartition() {
                return false;
            }
        };
        new MockUp<ExpressionRangePartitionInfoV2>() {
            @Mock
            public boolean isAutomaticPartition() {
                return false;
            }
        };
        try {
            String addPartitionSql = String.format("ALTER TABLE %s ADD " +
                    "PARTITION %s VALUES [(%s),(%s))", tbl, pName, toPartitionVal(pVal1), toPartitionVal(pVal2));
            System.out.println(addPartitionSql);
            starRocksAssert.alterTable(addPartitionSql);
        } catch (Exception e) {
            e.printStackTrace();
            LOG.error("Failed to add partition", e);
        }
    }

    private void withTableRangePartitions(String tableName) {
        if (tableName.equalsIgnoreCase("r1")) {
            return;
        }
        addRangePartition(tableName, "p1", "2024-01-29", "2024-01-30");
        addRangePartition(tableName, "p2", "2024-01-30", "2024-01-31");
        addRangePartition(tableName, "p3", "2024-01-31", "2024-02-01");
        addRangePartition(tableName, "p4", "2024-02-01", "2024-02-02");
    }


    @Test
    public void testPartitionTTLProperties() throws Exception {
        starRocksAssert.withDatabase("test").useDatabase("test")
                .withTable("CREATE TABLE test.tbl1\n" +
                        "(\n" +
                        "    k1 date,\n" +
                        "    v1 int \n" +
                        ")\n" +
                        "PARTITION BY RANGE(k1)\n" +
                        "(\n" +
                        "    PARTITION p1 values less than('2020-02-01'),\n" +
                        "    PARTITION p2 values less than('2020-03-01'),\n" +
                        "    PARTITION p3 values less than('2020-04-01'),\n" +
                        "    PARTITION p4 values less than('2020-05-01')\n" +
                        ")\n" +
                        "DISTRIBUTED BY HASH (k1) BUCKETS 3\n" +
                        "PROPERTIES" +
                        "(" +
                        "    'replication_num' = '1'\n" +
                        ");");

        DynamicPartitionScheduler dynamicPartitionScheduler = GlobalStateMgr.getCurrentState()
                .getDynamicPartitionScheduler();
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
        OlapTable tbl = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getFullName(), "tbl1");
        // Now the table does not actually support partition ttl,
        // so in order to simplify the test, it is directly set like this
        tbl.getTableProperty().getProperties().put("partition_ttl_number", "3");
        tbl.getTableProperty().setPartitionTTLNumber(3);

        dynamicPartitionScheduler.registerTtlPartitionTable(db.getId(), tbl.getId());
        dynamicPartitionScheduler.runAfterCatalogReady();

        Assertions.assertEquals(3, tbl.getPartitions().size());
    }

    @Test
    public void testPartitionTTLPropertiesZero() throws Exception {
        starRocksAssert.withDatabase("test").useDatabase("test")
                .withTable("CREATE TABLE test.base\n" +
                        "(\n" +
                        "    k1 date,\n" +
                        "    k2 int,\n" +
                        "    v1 int sum\n" +
                        ")\n" +
                        "PARTITION BY RANGE(k1)\n" +
                        "(\n" +
                        "    PARTITION p1 values less than('2020-02-01'),\n" +
                        "    PARTITION p2 values less than('2020-03-01')\n" +
                        ")\n" +
                        "DISTRIBUTED BY HASH(k2) BUCKETS 3\n" +
                        "PROPERTIES('replication_num' = '1');");
        String sql = "create materialized view mv1 " +
                "partition by k1 " +
                "distributed by hash(k2) " +
                "refresh async START('2122-12-31') EVERY(INTERVAL 1 HOUR) " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"partition_ttl_number\" = \"0\"\n" +
                ") " +
                "as select k1, k2 from test.base;";
        CreateMaterializedViewStatement createMaterializedViewStatement =
                (CreateMaterializedViewStatement) UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        try {
            GlobalStateMgr.getCurrentState().getLocalMetastore().createMaterializedView(createMaterializedViewStatement);
            fail();
        } catch (Exception ex) {
            Assertions.assertTrue(ex.getMessage().contains("Illegal Partition TTL Number"));
        }
    }

    @Test
    public void testAutoPartitionPartitionLiveNumber() throws Exception {
        new MockUp<LocalDateTime>() {
            @Mock
            public LocalDateTime now() {
                return LocalDateTime.of(2023, 3, 30, 1, 1, 1);
            }
        };

        starRocksAssert.withDatabase("test").useDatabase("test")
                .withTable("CREATE TABLE site_access(\n" +
                        "    event_day datetime,\n" +
                        "    site_id INT DEFAULT '10',\n" +
                        "    city_code VARCHAR(100),\n" +
                        "    user_name VARCHAR(32) DEFAULT '',\n" +
                        "    pv BIGINT DEFAULT '0'\n" +
                        ")\n" +
                        "DUPLICATE KEY(event_day, site_id, city_code, user_name)\n" +
                        "PARTITION BY date_trunc('day', event_day)(\n" +
                        " START (\"2023-03-27\") END (\"2023-03-31\") EVERY (INTERVAL 1 day),\n" +
                        " START (\"9999-12-30\") END (\"9999-12-31\") EVERY (INTERVAL 1 day)\n" +
                        ")\n" +
                        "DISTRIBUTED BY HASH(event_day, site_id) BUCKETS 32\n" +
                        "PROPERTIES(\n" +
                        "    \"partition_live_number\" = \"3\",\n" +
                        "    \"replication_num\" = \"1\"\n" +
                        ");");

        DynamicPartitionScheduler dynamicPartitionScheduler = GlobalStateMgr.getCurrentState()
                .getDynamicPartitionScheduler();
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
        OlapTable tbl =
                (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getFullName(), "site_access");
        dynamicPartitionScheduler.registerTtlPartitionTable(db.getId(), tbl.getId());
        dynamicPartitionScheduler.runOnceForTest();

        Map<String, Range<PartitionKey>> rangePartitionMap = tbl.getRangePartitionMap();

        Assertions.assertFalse(rangePartitionMap.containsKey("p20230327"));
        Assertions.assertTrue(rangePartitionMap.containsKey("p20230328"));
        Assertions.assertTrue(rangePartitionMap.containsKey("p20230329"));
        Assertions.assertTrue(rangePartitionMap.containsKey("p20230330"));
        Assertions.assertTrue(rangePartitionMap.containsKey("p99991230"));
    }

    @Test
    public void testAutoRandomPartitionFPartitionLiveNumber() throws Exception {
        new MockUp<LocalDateTime>() {
            @Mock
            public LocalDateTime now() {
                return LocalDateTime.of(2023, 3, 30, 1, 1, 1);
            }
        };

        starRocksAssert.withDatabase("test").useDatabase("test")
                .withTable("CREATE TABLE site_access(\n" +
                        "    event_day datetime,\n" +
                        "    site_id INT DEFAULT '10',\n" +
                        "    city_code VARCHAR(100),\n" +
                        "    user_name VARCHAR(32) DEFAULT '',\n" +
                        "    pv BIGINT DEFAULT '0'\n" +
                        ")\n" +
                        "DUPLICATE KEY(event_day, site_id, city_code, user_name)\n" +
                        "PARTITION BY date_trunc('day', event_day)(\n" +
                        " START (\"2023-03-27\") END (\"2023-03-31\") EVERY (INTERVAL 1 day),\n" +
                        " START (\"9999-12-30\") END (\"9999-12-31\") EVERY (INTERVAL 1 day)\n" +
                        ")\n" +
                        "DISTRIBUTED BY RANDOM BUCKETS 32\n" +
                        "PROPERTIES(\n" +
                        "    \"partition_live_number\" = \"3\",\n" +
                        "    \"replication_num\" = \"1\"\n" +
                        ");");

        DynamicPartitionScheduler dynamicPartitionScheduler = GlobalStateMgr.getCurrentState()
                .getDynamicPartitionScheduler();
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
        OlapTable tbl =
                (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getFullName(), "site_access");
        dynamicPartitionScheduler.registerTtlPartitionTable(db.getId(), tbl.getId());
        dynamicPartitionScheduler.runOnceForTest();

        Map<String, Range<PartitionKey>> rangePartitionMap = tbl.getRangePartitionMap();

        Assertions.assertFalse(rangePartitionMap.containsKey("p20230327"));
        Assertions.assertTrue(rangePartitionMap.containsKey("p20230328"));
        Assertions.assertTrue(rangePartitionMap.containsKey("p20230329"));
        Assertions.assertTrue(rangePartitionMap.containsKey("p20230330"));
        Assertions.assertTrue(rangePartitionMap.containsKey("p99991230"));
    }

    @Test
    public void testRandomDynamicPartitionShouldMatchConfig() throws Exception {
        new MockUp<LocalDateTime>() {
            @Mock
            public LocalDateTime now() {
                return LocalDateTime.of(2023, 3, 30, 1, 1, 1);
            }
        };

        starRocksAssert.withDatabase("test").useDatabase("test")
                .withTable("CREATE TABLE test_random_bucket (\n" +
                        "    uid String,\n" +
                        "    tdbank_imp_date Date\n" +
                        ") ENGINE=OLAP \n" +
                        "DUPLICATE KEY(`uid`) \n" +
                        "PARTITION BY RANGE(`tdbank_imp_date`) ()\n" +
                        "DISTRIBUTED BY RANDOM BUCKETS 1\n" +
                        "PROPERTIES (\n" +
                        "     \"replication_num\" = \"1\", \n" +
                        "     \"dynamic_partition.enable\" = \"true\", \n" +
                        "     \"dynamic_partition.time_unit\" = \"DAY\", \n" +
                        "     \"dynamic_partition.time_zone\" = \"Asia/Shanghai\", \n" +
                        "     \"dynamic_partition.start\" = \"-180\", \n" +
                        "     \"dynamic_partition.end\" = \"3\", \n" +
                        "     \"dynamic_partition.prefix\" = \"p\", \n" +
                        "     \"dynamic_partition.buckets\" = \"4\", \n" +
                        "     \"dynamic_partition.history_partition_num\" = \"0\",\n" +
                        "     \"compression\" = \"LZ4\" );");

        DynamicPartitionScheduler dynamicPartitionScheduler = GlobalStateMgr.getCurrentState()
                .getDynamicPartitionScheduler();
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
        OlapTable tbl = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getTable(db.getFullName(), "test_random_bucket");
        dynamicPartitionScheduler.registerTtlPartitionTable(db.getId(), tbl.getId());
        dynamicPartitionScheduler.runOnceForTest();

        Collection<Partition> partitions = tbl.getPartitions();
        for (Partition partition : partitions) {
            DistributionInfo distributionInfo = partition.getDistributionInfo();
            Assertions.assertEquals(4, distributionInfo.getBucketNum());
        }
    }

    @Test
    public void testPartitionColumnDateUseDynamicHour() throws Exception {
        new MockUp<LocalDateTime>() {
            @Mock
            public LocalDateTime now() {
                return LocalDateTime.of(2023, 3, 30, 1, 1, 1);
            }
        };

        starRocksAssert.withDatabase("test").useDatabase("test")
                .withTable("CREATE TABLE `test_hour_partition2` (\n" +
                        "  `event_day` date NULL COMMENT \"\",\n" +
                        "  `site_id` int(11) NULL DEFAULT \"10\" COMMENT \"\",\n" +
                        "  `city_code` varchar(100) NULL COMMENT \"\",\n" +
                        "  `user_name` varchar(32) NULL DEFAULT \"\" COMMENT \"\",\n" +
                        "  `pv` bigint(20) NULL DEFAULT \"0\" COMMENT \"\"\n" +
                        ") ENGINE=OLAP \n" +
                        "DUPLICATE KEY(`event_day`, `site_id`, `city_code`, `user_name`)\n" +
                        "PARTITION BY RANGE(`event_day`)\n" +
                        "()\n" +
                        "DISTRIBUTED BY HASH(`event_day`, `site_id`) BUCKETS 32 \n" +
                        "PROPERTIES (\n" +
                        "\"replication_num\" = \"1\",\n" +
                        "\"dynamic_partition.enable\" = \"true\",\n" +
                        "\"dynamic_partition.time_unit\" = \"DAY\",\n" +
                        "\"dynamic_partition.time_zone\" = \"Asia/Shanghai\",\n" +
                        "\"dynamic_partition.start\" = \"-1\",\n" +
                        "\"dynamic_partition.end\" = \"10\",\n" +
                        "\"dynamic_partition.prefix\" = \"p\",\n" +
                        "\"dynamic_partition.buckets\" = \"3\",\n" +
                        "\"dynamic_partition.history_partition_num\" = \"0\",\n" +
                        "\"in_memory\" = \"false\",\n" +
                        "\"storage_format\" = \"DEFAULT\",\n" +
                        "\"enable_persistent_index\" = \"true\",\n" +
                        "\"compression\" = \"LZ4\"\n" +
                        ");");

        DynamicPartitionScheduler dynamicPartitionScheduler = GlobalStateMgr.getCurrentState()
                .getDynamicPartitionScheduler();
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
        OlapTable tbl = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getTable(db.getFullName(), "test_hour_partition2");
        DynamicPartitionProperty dynamicPartitionProperty = tbl.getTableProperty().getDynamicPartitionProperty();
        dynamicPartitionProperty.setTimeUnit("HOUR");
        boolean result = dynamicPartitionScheduler.executeDynamicPartitionForTable(db.getId(), tbl.getId());
        Assertions.assertFalse(result);
    }

    @Test
    public void testDuplicatePartitionException() throws Exception {
        // construct a partition when creating table, and it is duplicated with that dynamic partition will create
        TimeZone timeZone = TimeUtils.getOrSystemTimeZone("Asia/Shanghai");
        ZonedDateTime now = ZonedDateTime.now(timeZone.toZoneId());
        String dateStr = DateTimeFormatter.ofPattern("yyyyMMdd")
                .format(now.plusDays(2).withHour(0).withMinute(0).withSecond(0));
        String partitionName = "p" + dateStr;

        starRocksAssert.withDatabase("test").useDatabase("test")
                .withTable("CREATE TABLE test_duplicate_part_exception (\n" +
                        "    uid String,\n" +
                        "    tdbank_imp_date Date\n" +
                        ") ENGINE=OLAP \n" +
                        "DUPLICATE KEY(`uid`) \n" +
                        "PARTITION BY RANGE(`tdbank_imp_date`) (" +
                        "PARTITION " + partitionName + " VALUES LESS THAN (\"2020-03-25\")" +
                        ")\n" +
                        "DISTRIBUTED BY RANDOM BUCKETS 1\n" +
                        "PROPERTIES (\n" +
                        "     \"replication_num\" = \"1\", \n" +
                        "     \"dynamic_partition.enable\" = \"true\", \n" +
                        "     \"dynamic_partition.time_unit\" = \"DAY\", \n" +
                        "     \"dynamic_partition.time_zone\" = \"Asia/Shanghai\", \n" +
                        "     \"dynamic_partition.end\" = \"3\", \n" +
                        "     \"dynamic_partition.prefix\" = \"p\", \n" +
                        "     \"dynamic_partition.buckets\" = \"4\", \n" +
                        "     \"dynamic_partition.history_partition_num\" = \"0\",\n" +
                        "     \"compression\" = \"LZ4\" );");

        DynamicPartitionScheduler dynamicPartitionScheduler = GlobalStateMgr.getCurrentState()
                .getDynamicPartitionScheduler();
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
        OlapTable tbl = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getTable(db.getFullName(), "test_duplicate_part_exception");
        try {
            dynamicPartitionScheduler.executeDynamicPartitionForTable(db.getId(), tbl.getId());
        } catch (Exception e) {
            fail("Should not throw exception: " + e.getMessage());
        }
    }

    @Test
    public void testListPartitionTTLCondition1() {
        for (String t : LIST_PARTITION_TABLES) {
            starRocksAssert.withTable(t,
                    (obj) -> {
                        String tableName = (String) obj;
                        withTableListPartitions(tableName);
                        OlapTable olapTable = (OlapTable) starRocksAssert.getTable("test", tableName);
                        Assertions.assertEquals(4, olapTable.getVisiblePartitions().size());
                        String dropPartitionSql = String.format("alter table %s set ('partition_retention_condition' = " +
                                "'dt >= current_date() - interval 1 month');", tableName);
                        starRocksAssert.alterTable(dropPartitionSql);

                        DynamicPartitionScheduler scheduler = GlobalStateMgr.getCurrentState()
                                .getDynamicPartitionScheduler();
                        scheduler.runOnceForTest();
                        Assertions.assertTrue(olapTable.getVisiblePartitions().size() == 0);
                        // add a new partition and an expired partition
                        LocalDateTime now = LocalDateTime.now();
                        addListPartition(tableName, "p5", "guangdong",
                                now.format(DateTimeFormatter.ofPattern("yyyy-MM-dd")));
                        addListPartition(tableName, "p6", "guangdong",
                                now.minusMonths(1).format(DateTimeFormatter.ofPattern("yyyy-MM-dd")));
                        Assertions.assertTrue(olapTable.getVisiblePartitions().size() == 2);

                        scheduler.runOnceForTest();
                        Assertions.assertTrue(olapTable.getVisiblePartitions().size() == 2);
                    });
        }
    }

    @Test
    public void testListPartitionTTLCondition2() {
        starRocksAssert.withTable("CREATE TABLE t1 (\n" +
                        " id BIGINT,\n" +
                        " age SMALLINT,\n" +
                        " dt datetime not null,\n" +
                        " province VARCHAR(64) not null\n" +
                        ")\n" +
                        "PARTITION BY (province, dt) \n" +
                        "DISTRIBUTED BY RANDOM\n" +
                        "PROPERTIES ('partition_retention_condition' = 'dt > current_date() - interval 1 month')",
                (obj) -> {
                    String tableName = (String) obj;
                    withTableListPartitions(tableName);
                    OlapTable olapTable = (OlapTable) starRocksAssert.getTable("test", tableName);
                    Assertions.assertEquals(4, olapTable.getVisiblePartitions().size());

                    DynamicPartitionScheduler scheduler = GlobalStateMgr.getCurrentState()
                            .getDynamicPartitionScheduler();
                    scheduler.runOnceForTest();
                    Assertions.assertTrue(olapTable.getVisiblePartitions().size() == 0);
                    // add a new partition and an expired partition
                    LocalDateTime now = LocalDateTime.now();
                    addListPartition(tableName, "p5", "guangdong",
                            now.format(DateTimeFormatter.ofPattern("yyyy-MM-dd")));
                    addListPartition(tableName, "p6", "guangdong",
                            now.minusMonths(1).format(DateTimeFormatter.ofPattern("yyyy-MM-dd")));
                    Assertions.assertTrue(olapTable.getVisiblePartitions().size() == 2);

                    scheduler.runOnceForTest();
                    Assertions.assertTrue(olapTable.getVisiblePartitions().size() == 1);
                });
    }

    @Test
    public void testListPartitionTTLCondition3() {
        for (String t : LIST_PARTITION_TABLES) {
            starRocksAssert.withTable(t,
                    (obj) -> {
                        String tableName = (String) obj;
                        withTableListPartitions(tableName);
                        OlapTable olapTable = (OlapTable) starRocksAssert.getTable("test", tableName);
                        Assertions.assertEquals(4, olapTable.getVisiblePartitions().size());

                        String dropPartitionSql = String.format("alter table %s set ('partition_retention_condition' = " +
                                "'date_trunc(\"day\", dt) >= date_sub(current_date(), 2) or " +
                                "date_trunc(\"day\", dt) = (date_trunc(\"month\", dt) " +
                                "+ interval 1 month - interval 1 day)');", tableName);
                        starRocksAssert.alterTable(dropPartitionSql);

                        DynamicPartitionScheduler scheduler = GlobalStateMgr.getCurrentState()
                                .getDynamicPartitionScheduler();
                        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
                        OlapTable tbl = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                                .getTable(db.getFullName(), tableName);
                        scheduler.runOnceForTest();
                        Assertions.assertTrue(tbl.getVisiblePartitions().size() == 2);
                        // add a new partition and an expired partition
                        LocalDateTime now = LocalDateTime.now();
                        addListPartition(tableName, "p5", "guangdong",
                                now.format(DateTimeFormatter.ofPattern("yyyy-MM-dd")));
                        addListPartition(tableName, "p6", "guangdong",
                                now.minusMonths(1).format(DateTimeFormatter.ofPattern("yyyy-MM-dd")));
                        Assertions.assertTrue(tbl.getVisiblePartitions().size() == 4);
                    });
        }
    }

    @Test
    public void testRangePartitionTTLCondition1() {
        for (String t : RANGE_PARTITION_TABLES) {
            starRocksAssert.withTable(t,
                    (obj) -> {
                        String tableName = (String) obj;
                        System.out.println(tableName);
                        withTableRangePartitions(tableName);

                        OlapTable olapTable = (OlapTable) starRocksAssert.getTable("test", tableName);
                        Assertions.assertFalse(DynamicPartitionUtil.isDynamicPartitionTable(olapTable));
                        Assertions.assertFalse(DynamicPartitionUtil.isTTLPartitionTable(olapTable));
                        Assertions.assertEquals(4, olapTable.getVisiblePartitions().size());

                        // set partition retention condition
                        String alterPartitionSql = String.format("alter table %s set ('partition_retention_condition' = " +
                                "'dt >= current_date() - interval 1 month');", tableName);
                        starRocksAssert.alterTable(alterPartitionSql);
                        Assertions.assertFalse(DynamicPartitionUtil.isDynamicPartitionTable(olapTable));
                        Assertions.assertTrue(DynamicPartitionUtil.isTTLPartitionTable(olapTable));

                        DynamicPartitionScheduler scheduler = GlobalStateMgr.getCurrentState()
                                .getDynamicPartitionScheduler();
                        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
                        OlapTable tbl = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                                .getTable(db.getFullName(), tableName);
                        scheduler.runOnceForTest();
                        Assertions.assertTrue(tbl.getVisiblePartitions().size() == 0);
                        // add a new partition and an expired partition
                        LocalDateTime now = LocalDateTime.now();
                        String currentDate = now.format(DateTimeFormatter.ofPattern("yyyy-MM-dd"));
                        String nextDate = now.plusDays(1).format(DateTimeFormatter.ofPattern("yyyy-MM-dd"));
                        addRangePartition(tableName, "p5", currentDate, nextDate);
                        Assertions.assertTrue(tbl.getVisiblePartitions().size() == 1);

                        scheduler.runOnceForTest();
                        Assertions.assertTrue(tbl.getVisiblePartitions().size() == 1);
                    });
        }
    }

    @Test
    public void testRangePartitionTTLCondition2() {
        starRocksAssert.withTable("CREATE TABLE r1 \n" +
                        "(\n" +
                        "    dt date,\n" +
                        "    k2 int,\n" +
                        "    v1 int \n" +
                        ")\n" +
                        "PARTITION BY RANGE(dt)\n" +
                        "(\n" +
                        "    PARTITION p0 values [('2024-01-29'),('2024-01-30')),\n" +
                        "    PARTITION p1 values [('2024-01-30'),('2024-01-31')),\n" +
                        "    PARTITION p2 values [('2024-01-31'),('2024-02-01')),\n" +
                        "    PARTITION p3 values [('2024-02-01'),('2024-02-02')) \n" +
                        ")\n" +
                        "DISTRIBUTED BY HASH(k2) BUCKETS 3\n" +
                        "PROPERTIES ('partition_retention_condition' = 'dt > current_date() - interval 1 month')",
                (obj) -> {
                    String tableName = (String) obj;
                    withTableListPartitions(tableName);
                    OlapTable olapTable = (OlapTable) starRocksAssert.getTable("test", tableName);
                    Assertions.assertEquals(4, olapTable.getVisiblePartitions().size());
                    Assertions.assertFalse(DynamicPartitionUtil.isDynamicPartitionTable(olapTable));
                    Assertions.assertTrue(DynamicPartitionUtil.isTTLPartitionTable(olapTable));

                    DynamicPartitionScheduler scheduler = GlobalStateMgr.getCurrentState()
                            .getDynamicPartitionScheduler();
                    Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
                    OlapTable tbl = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                            .getTable(db.getFullName(), tableName);
                    scheduler.runOnceForTest();
                    Assertions.assertTrue(tbl.getVisiblePartitions().size() == 0);

                    // add a new partition and an expired partition
                    LocalDateTime now = LocalDateTime.now();
                    String currentDate = now.format(DateTimeFormatter.ofPattern("yyyy-MM-dd"));
                    String nextDate = now.plusDays(1).format(DateTimeFormatter.ofPattern("yyyy-MM-dd"));
                    addRangePartition(tableName, "p5", currentDate, nextDate);
                    Assertions.assertTrue(tbl.getVisiblePartitions().size() == 1);

                    scheduler.runOnceForTest();
                    Assertions.assertTrue(tbl.getVisiblePartitions().size() == 1);
                });
    }

    @Test
    public void testRangePartitionTTLCondition3() {
        for (String t : RANGE_PARTITION_TABLES) {
            starRocksAssert.withTable(t,
                    (obj) -> {
                        String tableName = (String) obj;
                        System.out.println(tableName);
                        withTableRangePartitions(tableName);

                        OlapTable olapTable = (OlapTable) starRocksAssert.getTable("test", tableName);
                        Assertions.assertEquals(4, olapTable.getVisiblePartitions().size());
                        Assertions.assertFalse(DynamicPartitionUtil.isDynamicPartitionTable(olapTable));
                        Assertions.assertFalse(DynamicPartitionUtil.isTTLPartitionTable(olapTable));

                        String alterPartitionSql = String.format("alter table %s set ('partition_retention_condition' = " +
                                "'dt >= date_sub(current_date(), 2) or dt != (date_trunc(\"month\", dt) + interval 1 month - " +
                                "interval 1 day)')", tableName);
                        starRocksAssert.alterTable(alterPartitionSql);
                        Assertions.assertFalse(DynamicPartitionUtil.isDynamicPartitionTable(olapTable));
                        Assertions.assertTrue(DynamicPartitionUtil.isTTLPartitionTable(olapTable));

                        DynamicPartitionScheduler scheduler = GlobalStateMgr.getCurrentState()
                                .getDynamicPartitionScheduler();
                        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
                        OlapTable tbl = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                                .getTable(db.getFullName(), tableName);
                        scheduler.runOnceForTest();
                        // cannot beego
                        Assertions.assertTrue(tbl.getVisiblePartitions().size() == 4);
                        // add a new partition and an expired partition
                        LocalDateTime now = LocalDateTime.now();
                        String currentDate = now.format(DateTimeFormatter.ofPattern("yyyy-MM-dd"));
                        String nextDate = now.plusDays(1).format(DateTimeFormatter.ofPattern("yyyy-MM-dd"));
                        addRangePartition(tableName, "p5", currentDate, nextDate);
                        Assertions.assertTrue(tbl.getVisiblePartitions().size() == 5);

                        scheduler.runOnceForTest();
                        Assertions.assertTrue(tbl.getVisiblePartitions().size() == 5);
                    });
        }
    }

    @Test
    public void testPartitionConditionTTL1() throws Exception {
        starRocksAssert.withTable("create table list_par_int(\n" +
                " k1 int,\n" +
                " k2 string)\n" +
                " partition by list(k1)\n" +
                " (partition p1 values in('1','2'),\n" +
                "  partition p2 values in('3','4'),\n" +
                "  partition p3 values in('5','6'),\n" +
                "  partition p4 values in('7','8'),\n" +
                "  partition p5 values in('9','10'),\n" +
                "  partition p6 values in('11','12'),\n" +
                "  partition p7 values in('13','14'),\n" +
                "  partition p8 values in('15','16'),\n" +
                "  partition p9 values in('17','18'),\n" +
                "  partition p10 values in('19','20'))\n" +
                " distributed by hash(k1)\n" +
                "  PROPERTIES (\"partition_retention_condition\" = \"k1 > 5\");");

        final String tableName = "list_par_int";
        OlapTable olapTable = (OlapTable) starRocksAssert.getTable("test", tableName);
        Assertions.assertEquals(10, olapTable.getVisiblePartitions().size());
        Assertions.assertFalse(DynamicPartitionUtil.isDynamicPartitionTable(olapTable));
        Assertions.assertTrue(DynamicPartitionUtil.isTTLPartitionTable(olapTable));

        DynamicPartitionScheduler scheduler = GlobalStateMgr.getCurrentState()
                .getDynamicPartitionScheduler();
        scheduler.runOnceForTest();
        Set<String> visiblePartitionNames = olapTable.getVisiblePartitionNames();
        Assertions.assertEquals(8, visiblePartitionNames.size());
        Set<String> expectedPartitionNames = Sets.newHashSet("p3", "p4", "p5", "p6", "p7", "p8", "p9", "p10");
        System.out.println(visiblePartitionNames);
        Assertions.assertEquals(expectedPartitionNames, visiblePartitionNames);
    }

    @Test
    public void testPartitionConditionTTL2() throws Exception {
        starRocksAssert.withTable("create table range_par_hour(\n" +
                " k1 datetime,\n" +
                " k2 string)\n" +
                " partition by range(k1)\n" +
                "     (partition p1 values less than(\"2025-05-11 16:00:00\"),\n" +
                "      partition p2 values less than(\"2025-05-11 17:00:00\"),\n" +
                "      partition p3 values less than(\"2025-05-11 18:00:00\"),\n" +
                "      partition p4 values less than(\"2025-05-11 19:00:00\"),\n" +
                "      partition p5 values less than(\"2025-05-11 20:00:00\"),\n" +
                "      partition p6 values less than(\"2025-05-12 21:00:00\"),\n" +
                "      partition p7 values less than(\"2025-05-12 22:00:00\"),\n" +
                "      partition p8 values less than(\"2025-05-12 23:00:00\"),\n" +
                "      partition p9 values less than(\"2025-05-13 00:00:00\"),\n" +
                "      partition p10 values less than(\"2025-05-13 01:00:00\"),\n" +
                "      partition p11 values less than(\"2025-05-13 02:00:00\"))\n" +
                " distributed by hash(k1)\n" +
                " PROPERTIES (\"partition_retention_condition\" = \"k1 >= '2025-05-12 00:00:00'\");");

        final String tableName = "range_par_hour";
        OlapTable olapTable = (OlapTable) starRocksAssert.getTable("test", tableName);
        Assertions.assertEquals(11, olapTable.getVisiblePartitions().size());
        Assertions.assertFalse(DynamicPartitionUtil.isDynamicPartitionTable(olapTable));
        Assertions.assertTrue(DynamicPartitionUtil.isTTLPartitionTable(olapTable));

        DynamicPartitionScheduler scheduler = GlobalStateMgr.getCurrentState()
                .getDynamicPartitionScheduler();
        scheduler.runOnceForTest();
        Set<String> visiblePartitionNames = olapTable.getVisiblePartitionNames();
        System.out.println(visiblePartitionNames);
        Assertions.assertEquals(6, visiblePartitionNames.size());
        Set<String> expectedPartitionNames = Sets.newHashSet("p6", "p7", "p8", "p9", "p10", "p11");
        System.out.println(visiblePartitionNames);
        Assertions.assertEquals(expectedPartitionNames, visiblePartitionNames);
    }
}

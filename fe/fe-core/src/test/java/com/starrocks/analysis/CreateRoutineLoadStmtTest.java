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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/test/java/org/apache/doris/analysis/CreateRoutineLoadStmtTest.java

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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.common.Config;
import com.starrocks.common.ExceptionChecker;
import com.starrocks.common.FeConstants;
import com.starrocks.common.Pair;
import com.starrocks.common.StarRocksException;
import com.starrocks.load.routineload.KafkaProgress;
import com.starrocks.load.routineload.LoadDataSourceType;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.analyzer.AstToStringBuilder;
import com.starrocks.sql.analyzer.CreateRoutineLoadAnalyzer;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.ColumnSeparator;
import com.starrocks.sql.ast.CreateRoutineLoadStmt;
import com.starrocks.sql.ast.ImportWhereStmt;
import com.starrocks.sql.ast.LoadStmt;
import com.starrocks.sql.ast.PartitionNames;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.SelectRelation;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.parser.SqlParser;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class CreateRoutineLoadStmtTest {

    private static final Logger LOG = LogManager.getLogger(CreateRoutineLoadStmtTest.class);

    private static ConnectContext connectContext;
    private static StarRocksAssert starRocksAssert;

    @BeforeAll
    public static void beforeClass() throws Exception {
        FeConstants.runningUnitTest = true;
        Config.routine_load_scheduler_interval_millisecond = 100;
        Config.dynamic_partition_enable = true;
        Config.dynamic_partition_check_interval_seconds = 1;
        UtFrameUtils.createMinStarRocksCluster();

        // create connect context
        connectContext = UtFrameUtils.createDefaultCtx();
        starRocksAssert = new StarRocksAssert(connectContext);

        starRocksAssert.withDatabase("test").useDatabase("test");
    }

    @Test
    public void testParser() {
        {
            String sql = "CREATE ROUTINE LOAD testdb.routine_name ON table1\n"
                    + "WHERE k1 > 100 and k2 like \"%starrocks%\",\n"
                    + "COLUMNS(k1, k2, k3 = k1 + k2),\n"
                    + "COLUMNS TERMINATED BY \"\\t\",\n"
                    + "PARTITION(p1,p2) \n"
                    + "PROPERTIES\n"
                    + "(\n"
                    + "\"desired_concurrent_number\"=\"3\",\n"
                    + "\"max_batch_interval\" = \"20\",\n"
                    + "\"max_filter_ratio\" = \"0.12\",\n"
                    + "\"strict_mode\" = \"false\",\n"
                    + "\"timezone\" = \"Asia/Shanghai\"\n"
                    + ")\n"
                    + "FROM KAFKA\n"
                    + "(\n"
                    + "\"kafka_broker_list\" = \"kafkahost1:9092,kafkahost2:9092\",\n"
                    + "\"confluent.schema.registry.url\" = \"https://user:password@confluent.west.us\",\n"
                    + "\"kafka_topic\" = \"topictest\"\n"
                    + ");";
            List<StatementBase> stmts = com.starrocks.sql.parser.SqlParser.parse(sql, 32);
            CreateRoutineLoadStmt createRoutineLoadStmt = (CreateRoutineLoadStmt)stmts.get(0);
            CreateRoutineLoadAnalyzer.analyze(createRoutineLoadStmt, connectContext);
            List<String> partitionNames = new ArrayList<>();
            partitionNames.add("p1");
            partitionNames.add("p2");
            Assertions.assertNotNull(createRoutineLoadStmt.getRoutineLoadDesc());
            Assertions.assertEquals("\t", createRoutineLoadStmt.getRoutineLoadDesc().getColumnSeparator().getColumnSeparator());
            Assertions.assertEquals(partitionNames,
                    createRoutineLoadStmt.getRoutineLoadDesc().getPartitionNames().getPartitionNames());
            Assertions.assertEquals(3, createRoutineLoadStmt.getDesiredConcurrentNum());
            Assertions.assertEquals(20, createRoutineLoadStmt.getMaxBatchIntervalS());
            Assertions.assertEquals("kafkahost1:9092,kafkahost2:9092", createRoutineLoadStmt.getKafkaBrokerList());
            Assertions.assertEquals("topictest", createRoutineLoadStmt.getKafkaTopic());
            Assertions.assertEquals("Asia/Shanghai", createRoutineLoadStmt.getTimezone());
            Assertions.assertEquals("https://user:password@confluent.west.us", createRoutineLoadStmt.getConfluentSchemaRegistryUrl());
            Assertions.assertEquals(0.12, createRoutineLoadStmt.getMaxFilterRatio(), 0.01);
            Assertions.assertFalse(createRoutineLoadStmt.isPauseOnFatalParseError());
        }

        {
            String sql = "CREATE ROUTINE LOAD testdb.routine_name ON table1\n"
                    + "WHERE k1 > 100 and k2 like \"%starrocks%\",\n"
                    + "COLUMNS(k1, k2, k3 = k1 + k2),\n"
                    + "COLUMNS TERMINATED BY \"\\t\",\n"
                    + "PARTITION(p1,p2) \n"
                    + "PROPERTIES\n"
                    + "(\n"
                    + "\"desired_concurrent_number\"=\"3\",\n"
                    + "\"max_batch_interval\" = \"20\",\n"
                    + "\"max_filter_ratio\" = \"0.12\",\n"
                    + "\"strict_mode\" = \"false\",\n"
                    + "\"pause_on_fatal_parse_error\" = \"true\",\n"
                    + "\"timezone\" = \"Asia/Shanghai\"\n"
                    + ")\n"
                    + "FROM KAFKA\n"
                    + "(\n"
                    + "\"kafka_broker_list\" = \"[2001:db8:85a3::8a2e:370:7334]:9092,[2001:0db8:85a3:0000:0000:8a2e:0370:7335]:9092,192.168.164.136:9092\",\n"
                    + "\"confluent.schema.registry.url\" = \"https://user:password@confluent.west.us\",\n"
                    + "\"kafka_topic\" = \"topictest\"\n"
                    + ");";
            List<StatementBase> stmts = com.starrocks.sql.parser.SqlParser.parse(sql, 32);
            CreateRoutineLoadStmt createRoutineLoadStmt = (CreateRoutineLoadStmt)stmts.get(0);
            CreateRoutineLoadAnalyzer.analyze(createRoutineLoadStmt, connectContext);
            List<String> partitionNames = new ArrayList<>();
            partitionNames.add("p1");
            partitionNames.add("p2");
            Assertions.assertNotNull(createRoutineLoadStmt.getRoutineLoadDesc());
            Assertions.assertEquals("\t", createRoutineLoadStmt.getRoutineLoadDesc().getColumnSeparator().getColumnSeparator());
            Assertions.assertEquals(partitionNames,
                    createRoutineLoadStmt.getRoutineLoadDesc().getPartitionNames().getPartitionNames());
            Assertions.assertEquals(3, createRoutineLoadStmt.getDesiredConcurrentNum());
            Assertions.assertEquals(20, createRoutineLoadStmt.getMaxBatchIntervalS());
            Assertions.assertEquals("[2001:db8:85a3::8a2e:370:7334]:9092,[2001:0db8:85a3:0000:0000:8a2e:0370:7335]:9092,192.168.164.136:9092", createRoutineLoadStmt.getKafkaBrokerList());
            Assertions.assertEquals("topictest", createRoutineLoadStmt.getKafkaTopic());
            Assertions.assertEquals("Asia/Shanghai", createRoutineLoadStmt.getTimezone());
            Assertions.assertEquals("https://user:password@confluent.west.us", createRoutineLoadStmt.getConfluentSchemaRegistryUrl());
            Assertions.assertEquals(0.12, createRoutineLoadStmt.getMaxFilterRatio(), 0.01);
            Assertions.assertTrue(createRoutineLoadStmt.isPauseOnFatalParseError());
        }
    }

    @Test
    public void testWhereStmt() throws Exception {
        {
            String sql = "CREATE ROUTINE LOAD job ON tbl " +
            "COLUMNS TERMINATED BY ';', " +
            "ROWS TERMINATED BY '\n', " +
            "COLUMNS(`a`, `b`, `c`=1), " +
            "TEMPORARY PARTITION(`p1`, `p2`), " +
            "WHERE a = 1 " +
            "PROPERTIES (\"desired_concurrent_number\"=\"3\") " +
            "FROM KAFKA\n"
            + "(\n"
            + "\"kafka_broker_list\" = \"kafkahost1:9092,kafkahost2:9092\",\n"
            + "\"kafka_topic\" = \"topictest\"\n"
            + ");";
            List<StatementBase> stmts = com.starrocks.sql.parser.SqlParser.parse(sql, 32);
            CreateRoutineLoadStmt createRoutineLoadStmt = (CreateRoutineLoadStmt)stmts.get(0);
            CreateRoutineLoadAnalyzer.analyze(createRoutineLoadStmt, connectContext);
            ImportWhereStmt whereStmt = createRoutineLoadStmt.getRoutineLoadDesc().getWherePredicate();
            Assertions.assertEquals(false, whereStmt.isContainSubquery());
        }
        

        {
            String sql = "CREATE ROUTINE LOAD job ON tbl " +
            "COLUMNS TERMINATED BY ';', " +
            "ROWS TERMINATED BY '\n', " +
            "COLUMNS(`a`, `b`, `c`=1), " +
            "TEMPORARY PARTITION(`p1`, `p2`), " +
            "WHERE a in (SELECT 1) " +
            "PROPERTIES (\"desired_concurrent_number\"=\"3\") " +
            "FROM KAFKA (\"kafka_topic\" = \"my_topic\")";
            List<StatementBase> stmts = com.starrocks.sql.parser.SqlParser.parse(sql, 32);
            CreateRoutineLoadStmt createRoutineLoadStmt = (CreateRoutineLoadStmt)stmts.get(0);
            try {
                CreateRoutineLoadAnalyzer.analyze(createRoutineLoadStmt, connectContext);
            } catch (Exception e) {
                Assertions.assertEquals(true, e.getMessage().contains("the predicate cannot contain subqueries"));
                return;
            }
            Assertions.assertEquals(true, false);
        }
    }

    @Test
    public void testLoadPropertiesContexts() {
        String sql = "CREATE ROUTINE LOAD testdb.routine_name ON table1"
                + " PROPERTIES( \"desired_concurrent_number\"=\"3\",\n"
                + "\"max_batch_interval\" = \"20\",\n"
                + "\"strict_mode\" = \"false\",\n"
                + "\"timezone\" = \"Asia/Shanghai\"\n"
                + ")\n"
                + "FROM KAFKA\n"
                + "(\n"
                + "\"kafka_broker_list\" = \"kafkahost1:9092,kafkahost2:9092\",\n"
                + "\"kafka_topic\" = \"topictest\"\n"
                + ");";
        List<StatementBase> stmts = com.starrocks.sql.parser.SqlParser.parse(sql, 32);
        CreateRoutineLoadStmt createRoutineLoadStmt = (CreateRoutineLoadStmt)stmts.get(0);
        CreateRoutineLoadAnalyzer.analyze(createRoutineLoadStmt, connectContext);
        Assertions.assertNotNull(createRoutineLoadStmt.getRoutineLoadDesc());
        Assertions.assertEquals(0, createRoutineLoadStmt.getLoadPropertyList().size());
    }

    @Test
    public void testTaskTimeout() {
        {
            String sql = "CREATE ROUTINE LOAD testdb.routine_name ON table1"
                    + " PROPERTIES( \"desired_concurrent_number\"=\"3\",\n"
                    + "\"task_consume_second\" = \"5\",\n"
                    + "\"task_timeout_second\" = \"15\"\n"
                    + ")\n"
                    + "FROM KAFKA\n"
                    + "(\n"
                    + "\"kafka_broker_list\" = \"kafkahost1:9092,kafkahost2:9092\",\n"
                    + "\"kafka_topic\" = \"topictest\"\n"
                    + ");";
            List<StatementBase> stmts = com.starrocks.sql.parser.SqlParser.parse(sql, 32);
            CreateRoutineLoadStmt createRoutineLoadStmt = (CreateRoutineLoadStmt) stmts.get(0);
            CreateRoutineLoadAnalyzer.analyze(createRoutineLoadStmt, connectContext);
            Assertions.assertEquals(15, createRoutineLoadStmt.getTaskTimeoutSecond());
            Assertions.assertEquals(5, createRoutineLoadStmt.getTaskConsumeSecond());
        }

        {
            String sql = "CREATE ROUTINE LOAD testdb.routine_name ON table1"
                    + " PROPERTIES( \"desired_concurrent_number\"=\"3\",\n"
                    + "\"timezone\" = \"Asia/Shanghai\"\n"
                    + ")\n"
                    + "FROM KAFKA\n"
                    + "(\n"
                    + "\"kafka_broker_list\" = \"kafkahost1:9092,kafkahost2:9092\",\n"
                    + "\"kafka_topic\" = \"topictest\"\n"
                    + ");";
            List<StatementBase> stmts = com.starrocks.sql.parser.SqlParser.parse(sql, 32);
            CreateRoutineLoadStmt createRoutineLoadStmt = (CreateRoutineLoadStmt) stmts.get(0);
            CreateRoutineLoadAnalyzer.analyze(createRoutineLoadStmt, connectContext);
            Assertions.assertEquals(Config.routine_load_task_timeout_second, createRoutineLoadStmt.getTaskTimeoutSecond());
            Assertions.assertEquals(Config.routine_load_task_consume_second, createRoutineLoadStmt.getTaskConsumeSecond());
        }

        {
            String sql = "CREATE ROUTINE LOAD testdb.routine_name ON table1"
                    + " PROPERTIES( \"desired_concurrent_number\"=\"3\",\n"
                    + "\"task_timeout_second\" = \"20\"\n"
                    + ")\n"
                    + "FROM KAFKA\n"
                    + "(\n"
                    + "\"kafka_broker_list\" = \"kafkahost1:9092,kafkahost2:9092\",\n"
                    + "\"kafka_topic\" = \"topictest\"\n"
                    + ");";
            List<StatementBase> stmts = com.starrocks.sql.parser.SqlParser.parse(sql, 32);
            CreateRoutineLoadStmt createRoutineLoadStmt = (CreateRoutineLoadStmt) stmts.get(0);
            CreateRoutineLoadAnalyzer.analyze(createRoutineLoadStmt, connectContext);
            Assertions.assertEquals(20, createRoutineLoadStmt.getTaskTimeoutSecond());
            Assertions.assertEquals(5, createRoutineLoadStmt.getTaskConsumeSecond());
        }
    }

    @Test
    public void testLoadColumns() {
        String sql = "CREATE ROUTINE LOAD testdb.routine_name ON table1" +
                " COLUMNS(`k1`, `k2`, `k3`, `k4`, `k5`," +
                " `v1` = to_bitmap(`k1`))" +
                " PROPERTIES (\"desired_concurrent_number\"=\"1\")" +
                " FROM KAFKA (\"kafka_topic\" = \"my_topic\", " +
                "\"kafka_broker_list\" = \"kafkahost1:9092,kafkahost2:9092\"" +
                ")";
        List<StatementBase> stmts = com.starrocks.sql.parser.SqlParser.parse(sql, 32);
        CreateRoutineLoadStmt createRoutineLoadStmt = (CreateRoutineLoadStmt)stmts.get(0);
        CreateRoutineLoadAnalyzer.analyze(createRoutineLoadStmt, connectContext);
        Assertions.assertEquals(6, createRoutineLoadStmt.getRoutineLoadDesc().getColumnsInfo().getColumns().size());

        sql = "CREATE ROUTINE LOAD testdb.routine_name ON table1" +
                " COLUMNS(`k1`, `k2`, `k3`, `k4`, `k5`)" +
                " PROPERTIES (\"desired_concurrent_number\"=\"1\")" +
                " FROM KAFKA (\"kafka_topic\" = \"my_topic\", " +
                "\"kafka_broker_list\" = \"kafkahost1:9092,kafkahost2:9092\"" +
                ")";
        stmts = com.starrocks.sql.parser.SqlParser.parse(sql, 32);
        createRoutineLoadStmt = (CreateRoutineLoadStmt)stmts.get(0);
        CreateRoutineLoadAnalyzer.analyze(createRoutineLoadStmt, connectContext);
        Assertions.assertEquals(5, createRoutineLoadStmt.getRoutineLoadDesc().getColumnsInfo().getColumns().size());

        sql = "CREATE ROUTINE LOAD testdb.routine_name ON table1" +
                " COLUMNS( `v1` = to_bitmap(`k1`)," +
                " `v2` = to_bitmap(`k2`)," +
                " `v3` = to_bitmap(`k3`)," +
                " `v4` = to_bitmap(`k4`)," +
                " `v5` = to_bitmap(`k5`))" +
                " PROPERTIES (\"desired_concurrent_number\"=\"1\")" +
                " FROM KAFKA (\"kafka_topic\" = \"my_topic\", " +
                "\"kafka_broker_list\" = \"kafkahost1:9092,kafkahost2:9092\"" +
                ")";
        stmts = com.starrocks.sql.parser.SqlParser.parse(sql, 32);
        createRoutineLoadStmt = (CreateRoutineLoadStmt)stmts.get(0);
        CreateRoutineLoadAnalyzer.analyze(createRoutineLoadStmt, connectContext);
        Assertions.assertEquals(5, createRoutineLoadStmt.getRoutineLoadDesc().getColumnsInfo().getColumns().size());

        sql = "CREATE ROUTINE LOAD testdb.routine_name ON table1" +
                " COLUMNS( `v1` = to_bitmap(`k1`)," +
                " `v2` = to_bitmap(`k2`)," +
                " `v3` = to_bitmap(`k3`)," +
                " `v4` = to_bitmap(`k4`)," +
                " `v5` = to_bitmap(`k5`)," +
                " `k1`, `k2`, `k3`, `k4`, `k5` )" +
                " PROPERTIES (\"desired_concurrent_number\"=\"1\")" +
                " FROM KAFKA (\"kafka_topic\" = \"my_topic\", " +
                "\"kafka_broker_list\" = \"kafkahost1:9092,kafkahost2:9092\"" +
                ")";
        stmts = com.starrocks.sql.parser.SqlParser.parse(sql, 32);
        createRoutineLoadStmt = (CreateRoutineLoadStmt)stmts.get(0);
        CreateRoutineLoadAnalyzer.analyze(createRoutineLoadStmt, connectContext);
        Assertions.assertEquals(10, createRoutineLoadStmt.getRoutineLoadDesc().getColumnsInfo().getColumns().size());

        sql = "CREATE ROUTINE LOAD testdb.routine_name ON table1" +
                " COLUMNS( `v1` = to_bitmap(`k1`), `k1`," +
                " `v2` = to_bitmap(`k2`), `k2`," +
                " `v3` = to_bitmap(`k3`), `k3`," +
                " `v4` = to_bitmap(`k4`), `k4`," +
                " `v5` = to_bitmap(`k5`), `k5`)" +
                " PROPERTIES (\"desired_concurrent_number\"=\"1\")" +
                " FROM KAFKA (\"kafka_topic\" = \"my_topic\", " +
                "\"kafka_broker_list\" = \"kafkahost1:9092,kafkahost2:9092\"" +
                ")";
        stmts = com.starrocks.sql.parser.SqlParser.parse(sql, 32);
        createRoutineLoadStmt = (CreateRoutineLoadStmt)stmts.get(0);
        CreateRoutineLoadAnalyzer.analyze(createRoutineLoadStmt, connectContext);
        Assertions.assertEquals(10, createRoutineLoadStmt.getRoutineLoadDesc().getColumnsInfo().getColumns().size());

        sql = "CREATE ROUTINE LOAD testdb.routine_name ON table1" +
                " COLUMNS(`k1`, `k2`, `k3`, `k4`, `k5`," +
                " `v1` = to_bitmap(`k1`)," +
                " `v2` = to_bitmap(`k2`)," +
                " `v3` = to_bitmap(`k3`)," +
                " `v4` = to_bitmap(`k4`)," +
                " `v5` = to_bitmap(`k5`))" +
                " PROPERTIES (\"desired_concurrent_number\"=\"1\")" +
                " FROM KAFKA (\"kafka_topic\" = \"my_topic\", " +
                "\"kafka_broker_list\" = \"kafkahost1:9092,kafkahost2:9092\"" +
                ")";
        stmts = com.starrocks.sql.parser.SqlParser.parse(sql, 32);
        createRoutineLoadStmt = (CreateRoutineLoadStmt)stmts.get(0);
        CreateRoutineLoadAnalyzer.analyze(createRoutineLoadStmt, connectContext);
        Assertions.assertEquals(10, createRoutineLoadStmt.getRoutineLoadDesc().getColumnsInfo().getColumns().size());
    }

    @Test
    public void testAnalyzeWithDuplicateProperty() throws StarRocksException {
        String jobName = "job1";
        String dbName = "db1";
        LabelName labelName = new LabelName(dbName, jobName);
        String tableNameString = "table1";
        String topicName = "topic1";
        String serverAddress = "http://127.0.0.1:8080";
        String kafkaPartitionString = "1,2,3";
        List<String> partitionNameString = Lists.newArrayList();
        partitionNameString.add("p1");
        PartitionNames partitionNames = new PartitionNames(false, partitionNameString);
        ColumnSeparator columnSeparator = new ColumnSeparator(",");

        // duplicate load property
        List<ParseNode> loadPropertyList = new ArrayList<>();
        loadPropertyList.add(columnSeparator);
        loadPropertyList.add(columnSeparator);
        Map<String, String> properties = Maps.newHashMap();
        properties.put(CreateRoutineLoadStmt.DESIRED_CONCURRENT_NUMBER_PROPERTY, "2");
        String typeName = LoadDataSourceType.KAFKA.name();
        Map<String, String> customProperties = Maps.newHashMap();

        customProperties.put(CreateRoutineLoadStmt.KAFKA_TOPIC_PROPERTY, topicName);
        customProperties.put(CreateRoutineLoadStmt.KAFKA_BROKER_LIST_PROPERTY, serverAddress);
        customProperties.put(CreateRoutineLoadStmt.KAFKA_PARTITIONS_PROPERTY, kafkaPartitionString);

        CreateRoutineLoadStmt createRoutineLoadStmt = new CreateRoutineLoadStmt(labelName, tableNameString,
                loadPropertyList, properties,
                typeName, customProperties);
        try {
            CreateRoutineLoadAnalyzer.analyze(createRoutineLoadStmt, connectContext);
            Assertions.fail();
        } catch (RuntimeException e) {
            LOG.info(e.getMessage());
        }
    }

    @Test
    public void testAnalyze() {
        String jobName = "job1";
        String dbName = "db1";
        LabelName labelName = new LabelName(dbName, jobName);
        String tableNameString = "table1";
        String topicName = "topic1";
        String serverAddress = "127.0.0.1:8080";
        String kafkaPartitionString = "1,2,3";
        String timeZone = "8:00";
        List<String> partitionNameString = Lists.newArrayList();
        partitionNameString.add("p1");
        PartitionNames partitionNames = new PartitionNames(false, partitionNameString);
        ColumnSeparator columnSeparator = new ColumnSeparator(",");

        // duplicate load property
        TableName tableName = new TableName(dbName, tableNameString);
        List<ParseNode> loadPropertyList = new ArrayList<>();
        loadPropertyList.add(columnSeparator);
        loadPropertyList.add(partitionNames);
        Map<String, String> properties = Maps.newHashMap();
        properties.put(CreateRoutineLoadStmt.DESIRED_CONCURRENT_NUMBER_PROPERTY, "2");
        properties.put(LoadStmt.TIMEZONE, timeZone);
        String typeName = LoadDataSourceType.KAFKA.name();
        Map<String, String> customProperties = Maps.newHashMap();

        customProperties.put(CreateRoutineLoadStmt.KAFKA_TOPIC_PROPERTY, topicName);
        customProperties.put(CreateRoutineLoadStmt.KAFKA_BROKER_LIST_PROPERTY, serverAddress);
        customProperties.put(CreateRoutineLoadStmt.KAFKA_PARTITIONS_PROPERTY, kafkaPartitionString);

        CreateRoutineLoadStmt createRoutineLoadStmt = new CreateRoutineLoadStmt(labelName, tableNameString,
                loadPropertyList, properties,
                typeName, customProperties);

        CreateRoutineLoadAnalyzer.analyze(createRoutineLoadStmt, connectContext);

        Assertions.assertNotNull(createRoutineLoadStmt.getRoutineLoadDesc());
        Assertions.assertEquals(columnSeparator, createRoutineLoadStmt.getRoutineLoadDesc().getColumnSeparator());
        Assertions.assertEquals(partitionNames.getPartitionNames(),
                createRoutineLoadStmt.getRoutineLoadDesc().getPartitionNames().getPartitionNames());
        Assertions.assertEquals(2, createRoutineLoadStmt.getDesiredConcurrentNum());
        Assertions.assertEquals(0, createRoutineLoadStmt.getMaxErrorNum());
        Assertions.assertEquals(serverAddress, createRoutineLoadStmt.getKafkaBrokerList());
        Assertions.assertEquals(topicName, createRoutineLoadStmt.getKafkaTopic());
        Assertions.assertEquals("+08:00", createRoutineLoadStmt.getTimezone());
    }

    @Test
    public void testAnalyzeJsonConfig() throws Exception {
        String createSQL = "CREATE ROUTINE LOAD db0.routine_load_0 ON t1 " +
                "PROPERTIES(\"format\" = \"json\",\"jsonpaths\"=\"[\\\"$.k1\\\",\\\"$.k2.\\\\\\\"k2.1\\\\\\\"\\\"]\") " +
                "FROM KAFKA(\"kafka_broker_list\" = \"xxx.xxx.xxx.xxx:9092\",\"kafka_topic\" = \"topic_0\");";
        ConnectContext ctx = starRocksAssert.getCtx();
        CreateRoutineLoadStmt createRoutineLoadStmt = (CreateRoutineLoadStmt) SqlParser.parse(createSQL, 32).get(0);
        CreateRoutineLoadAnalyzer.analyze(createRoutineLoadStmt, connectContext);
        Assertions.assertEquals(createRoutineLoadStmt.getJsonPaths(), "[\"$.k1\",\"$.k2.\\\"k2.1\\\"\"]");

        String selectSQL = "SELECT \"Pat O\"\"Hanrahan & <Matthew Eldridge]\"\"\";";
        QueryStatement selectStmt = (QueryStatement) UtFrameUtils.parseStmtWithNewParser(selectSQL, ctx);

        Expr expr = ((SelectRelation) (selectStmt.getQueryRelation())).getOutputExpression().get(0);
        Assertions.assertTrue(expr instanceof StringLiteral);
        StringLiteral stringLiteral = (StringLiteral) expr;
        Assertions.assertEquals(stringLiteral.getValue(), "Pat O\"Hanrahan & <Matthew Eldridge]\"");

    }

    @Test
    public void testAnalyzeAvroConfig() throws Exception {
        String createSQL = "CREATE ROUTINE LOAD db0.routine_load_0 ON t1 " +
                "PROPERTIES(\"format\" = \"avro\",\"jsonpaths\"=\"[\\\"$.k1\\\",\\\"$.k2.\\\\\\\"k2.1\\\\\\\"\\\"]\") " +
                "FROM KAFKA(\"kafka_broker_list\" = \"xxx.xxx.xxx.xxx:9092\",\"kafka_topic\" = \"topic_0\"," +
                "\"confluent.schema.registry.url\" = \"https://user:password@confluent.west.us\");";
        ConnectContext ctx = starRocksAssert.getCtx();
        CreateRoutineLoadStmt createRoutineLoadStmt = (CreateRoutineLoadStmt) SqlParser.parse(createSQL, 32).get(0);
        CreateRoutineLoadAnalyzer.analyze(createRoutineLoadStmt, connectContext);
        Assertions.assertEquals(createRoutineLoadStmt.getJsonPaths(), "[\"$.k1\",\"$.k2.\\\"k2.1\\\"\"]");
        Assertions.assertEquals("https://user:password@confluent.west.us", createRoutineLoadStmt.getConfluentSchemaRegistryUrl());
    }

    @Test
    public void testAnalyzeCSVConfig() throws Exception {
        String createSQL = "CREATE ROUTINE LOAD db0.routine_load_1 ON t1 " +
                "PROPERTIES(\"format\" = \"csv\", \"trim_space\"=\"true\", \"enclose\"=\"'\", \"escape\"=\"|\") " +
                "FROM KAFKA(\"kafka_broker_list\" = \"xxx.xxx.xxx.xxx:9092\",\"kafka_topic\" = \"topic_0\");";
        ConnectContext ctx = starRocksAssert.getCtx();
        CreateRoutineLoadStmt createRoutineLoadStmt = (CreateRoutineLoadStmt) SqlParser.parse(createSQL, 32).get(0);
        CreateRoutineLoadAnalyzer.analyze(createRoutineLoadStmt, connectContext);
        Assertions.assertEquals(createRoutineLoadStmt.isTrimspace(), true);
        Assertions.assertEquals(createRoutineLoadStmt.getEnclose(), '\'');
        Assertions.assertEquals(createRoutineLoadStmt.getEscape(), '|');
    }

    @Test
    public void testAnalyzeCSVDefalultValue() throws Exception {
        String createSQL = "CREATE ROUTINE LOAD db0.routine_load_1 ON t1 " +
                "PROPERTIES(\"max_error_number\" = \"10\") " +
                "FROM KAFKA(\"kafka_broker_list\" = \"xxx.xxx.xxx.xxx:9092\",\"kafka_topic\" = \"topic_0\");";
        ConnectContext ctx = starRocksAssert.getCtx();
        CreateRoutineLoadStmt createRoutineLoadStmt = (CreateRoutineLoadStmt) SqlParser.parse(createSQL, 32).get(0);
        CreateRoutineLoadAnalyzer.analyze(createRoutineLoadStmt, connectContext);
        Assertions.assertEquals(createRoutineLoadStmt.getMaxErrorNum(), 10);
        Assertions.assertEquals(createRoutineLoadStmt.getEnclose(), 0);
        Assertions.assertEquals(createRoutineLoadStmt.getEscape(), 0);
        Assertions.assertEquals(createRoutineLoadStmt.isTrimspace(), false);
    }

    @Test
    public void testKafkaOffset() {
        String jobName = "job1";
        String dbName = "db1";
        String tableNameString = "table1";
        String kafkaDefaultOffsetsKey = "property." + CreateRoutineLoadStmt.KAFKA_DEFAULT_OFFSETS;

        // load property
        List<String> partitionNameString = Lists.newArrayList();
        partitionNameString.add("p1");
        PartitionNames partitionNames = new PartitionNames(false, partitionNameString);
        ColumnSeparator columnSeparator = new ColumnSeparator(",");
        List<ParseNode> loadPropertyList = new ArrayList<>();
        loadPropertyList.add(columnSeparator);
        loadPropertyList.add(partitionNames);

        // 1. kafka_offsets
        // 1 -> OFFSET_BEGINNING, 2 -> OFFSET_END
        Map<String, String> customProperties = getCustomProperties();
        customProperties.put(CreateRoutineLoadStmt.KAFKA_PARTITIONS_PROPERTY, " 1 , 2 ");
        customProperties.put(CreateRoutineLoadStmt.KAFKA_OFFSETS_PROPERTY, " OFFSET_BEGINNING , OFFSET_END ");
        LabelName labelName = new LabelName(dbName, jobName);
        CreateRoutineLoadStmt createRoutineLoadStmt = new CreateRoutineLoadStmt(
                labelName, tableNameString, loadPropertyList, Maps.newHashMap(),
                LoadDataSourceType.KAFKA.name(), customProperties);
        CreateRoutineLoadAnalyzer.analyze(createRoutineLoadStmt, connectContext);
        List<Pair<Integer, Long>> partitionOffsets = createRoutineLoadStmt.getKafkaPartitionOffsets();
        Assertions.assertEquals(2, partitionOffsets.size());
        Assertions.assertEquals(KafkaProgress.OFFSET_BEGINNING_VAL, (long) partitionOffsets.get(0).second);
        Assertions.assertEquals(KafkaProgress.OFFSET_END_VAL, (long) partitionOffsets.get(1).second);

        // 2. no kafka_offsets and property.kafka_default_offsets
        // 1,2 -> OFFSET_END
        customProperties = getCustomProperties();
        customProperties.put(CreateRoutineLoadStmt.KAFKA_PARTITIONS_PROPERTY, "1,2");
        labelName = new LabelName(dbName, jobName);
        createRoutineLoadStmt =
                new CreateRoutineLoadStmt(labelName, tableNameString, loadPropertyList, Maps.newHashMap(),
                        LoadDataSourceType.KAFKA.name(), customProperties);
        CreateRoutineLoadAnalyzer.analyze(createRoutineLoadStmt, connectContext);
        partitionOffsets = createRoutineLoadStmt.getKafkaPartitionOffsets();
        Assertions.assertEquals(2, partitionOffsets.size());
        Assertions.assertEquals(KafkaProgress.OFFSET_END_VAL, (long) partitionOffsets.get(0).second);
        Assertions.assertEquals(KafkaProgress.OFFSET_END_VAL, (long) partitionOffsets.get(1).second);

        // 3. property.kafka_default_offsets
        // 1,2 -> 10
        customProperties = getCustomProperties();
        customProperties.put(CreateRoutineLoadStmt.KAFKA_PARTITIONS_PROPERTY, "1,2");
        customProperties.put(kafkaDefaultOffsetsKey, "10");
        labelName = new LabelName(dbName, jobName);
        createRoutineLoadStmt =
                new CreateRoutineLoadStmt(labelName, tableNameString, loadPropertyList, Maps.newHashMap(),
                        LoadDataSourceType.KAFKA.name(), customProperties);
        CreateRoutineLoadAnalyzer.analyze(createRoutineLoadStmt, connectContext);
        partitionOffsets = createRoutineLoadStmt.getKafkaPartitionOffsets();
        Assertions.assertEquals(2, partitionOffsets.size());
        Assertions.assertEquals(10, (long) partitionOffsets.get(0).second);
        Assertions.assertEquals(10, (long) partitionOffsets.get(1).second);

        // 4. both kafka_offsets and property.kafka_default_offsets
        // 1 -> OFFSET_BEGINNING, 2 -> OFFSET_END, 3 -> 11
        customProperties = getCustomProperties();
        customProperties.put(CreateRoutineLoadStmt.KAFKA_PARTITIONS_PROPERTY, "1,2,3");
        customProperties.put(CreateRoutineLoadStmt.KAFKA_OFFSETS_PROPERTY, "OFFSET_BEGINNING,OFFSET_END,11");
        customProperties.put(kafkaDefaultOffsetsKey, "10");
        labelName = new LabelName(dbName, jobName);
        createRoutineLoadStmt =
                new CreateRoutineLoadStmt(labelName, tableNameString, loadPropertyList, Maps.newHashMap(),
                        LoadDataSourceType.KAFKA.name(), customProperties);
        CreateRoutineLoadAnalyzer.analyze(createRoutineLoadStmt, connectContext);
        partitionOffsets = createRoutineLoadStmt.getKafkaPartitionOffsets();
        Assertions.assertEquals(3, partitionOffsets.size());
        Assertions.assertEquals(KafkaProgress.OFFSET_BEGINNING_VAL, (long) partitionOffsets.get(0).second);
        Assertions.assertEquals(KafkaProgress.OFFSET_END_VAL, (long) partitionOffsets.get(1).second);
        Assertions.assertEquals(11, (long) partitionOffsets.get(2).second);

        // 5. invalid partitions " 1 2 3 "
        customProperties = getCustomProperties();
        customProperties.put(CreateRoutineLoadStmt.KAFKA_PARTITIONS_PROPERTY, " 1 2 3 ");
        customProperties.put(CreateRoutineLoadStmt.KAFKA_OFFSETS_PROPERTY, "OFFSET_BEGINNING,OFFSET_END,11");
        customProperties.put(kafkaDefaultOffsetsKey, "10");
        labelName = new LabelName(dbName, jobName);
        createRoutineLoadStmt =
                new CreateRoutineLoadStmt(labelName, tableNameString, loadPropertyList, Maps.newHashMap(),
                        LoadDataSourceType.KAFKA.name(), customProperties);
        CreateRoutineLoadStmt finalCreateRoutineLoadStmt = createRoutineLoadStmt;
        ExceptionChecker.expectThrowsWithMsg(SemanticException.class,
                "Invalid kafka partition: '1 2 3'. Expected values should be an integer",
                () -> CreateRoutineLoadAnalyzer.analyze(finalCreateRoutineLoadStmt, connectContext));

        // 6. invalid offset a
        customProperties = getCustomProperties();
        customProperties.put(CreateRoutineLoadStmt.KAFKA_PARTITIONS_PROPERTY, "1,2,3");
        customProperties.put(CreateRoutineLoadStmt.KAFKA_OFFSETS_PROPERTY, "OFFSET_BEGINNING,OFFSET_END,a");
        customProperties.put(kafkaDefaultOffsetsKey, "10");
        labelName = new LabelName(dbName, jobName);
        createRoutineLoadStmt =
                new CreateRoutineLoadStmt(labelName, tableNameString, loadPropertyList, Maps.newHashMap(),
                        LoadDataSourceType.KAFKA.name(), customProperties);
        CreateRoutineLoadStmt finalCreateRoutineLoadStmt2 = createRoutineLoadStmt;
        ExceptionChecker.expectThrowsWithMsg(SemanticException.class,
                "Invalid kafka offset: 'a'. Expected values should be an integer, OFFSET_BEGINNING, or OFFSET_END",
                () -> CreateRoutineLoadAnalyzer.analyze(finalCreateRoutineLoadStmt2, connectContext));
    }

    @Test
    public void testToStringWithDBName() {
        String sql = "CREATE ROUTINE LOAD testdb.routine_name ON table1\n"
                + "WHERE k1 > 100 and k2 like \"%starrocks%\",\n"
                + "COLUMNS(k1, k2, k3 = k1 + k2),\n"
                + "COLUMNS TERMINATED BY \"\\t\",\n"
                + "PARTITION(p1,p2) \n"
                + "PROPERTIES\n"
                + "(\n"
                + "\"desired_concurrent_number\"=\"3\",\n"
                + "\"max_batch_interval\" = \"20\",\n"
                + "\"strict_mode\" = \"false\",\n"
                + "\"timezone\" = \"Asia/Shanghai\"\n"
                + ")\n"
                + "FROM KAFKA\n"
                + "(\n"
                + "\"kafka_broker_list\" = \"kafkahost1:9092,kafkahost2:9092\",\n"
                + "\"kafka_topic\" = \"topictest\",\n"
                + "\"confluent.schema.registry.url\" = \"https://user:password@confluent.west.us\"\n"
                + ");";
        ConnectContext ctx = starRocksAssert.getCtx();
        CreateRoutineLoadStmt stmt = (CreateRoutineLoadStmt) com.starrocks.sql.parser.SqlParser.parse(sql, ctx.getSessionVariable()).get(0);
        Assertions.assertEquals("CREATE ROUTINE LOAD testdb.routine_name ON table1PROPERTIES ( \"desired_concurrent_number\" = \"3\", \"timezone\" = \"Asia/Shanghai\", \"strict_mode\" = \"false\", \"max_batch_interval\" = \"20\" ) " +
        "FROM KAFKA ( \"kafka_broker_list\" = \"kafkahost1:9092,kafkahost2:9092\", \"kafka_topic\" = \"topictest\", \"confluent.schema.registry.url\" = \"***\" )", AstToStringBuilder.toString(stmt));
    }

    @Test
    public void testToStringWithoutDBName() {
        String sql = "CREATE ROUTINE LOAD routine_name ON table1\n"
                + "WHERE k1 > 100 and k2 like \"%starrocks%\",\n"
                + "COLUMNS(k1, k2, k3 = k1 + k2),\n"
                + "COLUMNS TERMINATED BY \"\\t\",\n"
                + "PARTITION(p1,p2) \n"
                + "PROPERTIES\n"
                + "(\n"
                + "\"desired_concurrent_number\"=\"3\",\n"
                + "\"max_batch_interval\" = \"20\",\n"
                + "\"strict_mode\" = \"false\",\n"
                + "\"timezone\" = \"Asia/Shanghai\"\n"
                + ")\n"
                + "FROM KAFKA\n"
                + "(\n"
                + "\"kafka_broker_list\" = \"kafkahost1:9092,kafkahost2:9092\",\n"
                + "\"kafka_topic\" = \"topictest\",\n"
                + "\"confluent.schema.registry.url\" = \"https://user:password@confluent.west.us\"\n"
                + ");";
        ConnectContext ctx = starRocksAssert.getCtx();
        CreateRoutineLoadStmt stmt = (CreateRoutineLoadStmt) com.starrocks.sql.parser.SqlParser.parse(sql, ctx.getSessionVariable()).get(0);
        Assertions.assertEquals("CREATE ROUTINE LOAD routine_name ON table1PROPERTIES ( \"desired_concurrent_number\" = \"3\", \"timezone\" = \"Asia/Shanghai\", \"strict_mode\" = \"false\", \"max_batch_interval\" = \"20\" ) " +
                "FROM KAFKA ( \"kafka_broker_list\" = \"kafkahost1:9092,kafkahost2:9092\", \"kafka_topic\" = \"topictest\", \"confluent.schema.registry.url\" = \"***\" )", AstToStringBuilder.toString(stmt));
    }

    private Map<String, String> getCustomProperties() {
        Map<String, String> customProperties = Maps.newHashMap();
        customProperties.put(CreateRoutineLoadStmt.KAFKA_TOPIC_PROPERTY, "topic1");
        customProperties.put(CreateRoutineLoadStmt.KAFKA_BROKER_LIST_PROPERTY, "127.0.0.1:8080");
        return customProperties;
    }
}

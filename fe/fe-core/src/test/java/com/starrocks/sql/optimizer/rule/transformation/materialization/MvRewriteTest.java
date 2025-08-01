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

package com.starrocks.sql.optimizer.rule.transformation.materialization;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Range;
import com.starrocks.catalog.BaseTableInfo;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.ColumnId;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.MvPlanContext;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.constraint.ForeignKeyConstraint;
import com.starrocks.catalog.constraint.UniqueConstraint;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.Config;
import com.starrocks.common.FeConstants;
import com.starrocks.qe.SessionVariable;
import com.starrocks.qe.ShowResultSet;
import com.starrocks.schema.MSchema;
import com.starrocks.schema.MTable;
import com.starrocks.sql.optimizer.CachingMvPlanContextBuilder;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.QueryOptimizer;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.base.PhysicalPropertySet;
import com.starrocks.sql.optimizer.operator.logical.LogicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalScanOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalScanOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.plan.PlanTestBase;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer.MethodName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import java.sql.SQLException;
import java.time.LocalDate;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

@TestMethodOrder(MethodName.class)
public class MvRewriteTest extends MVTestBase {

    @BeforeAll
    public static void beforeClass() throws Exception {
        MVTestBase.beforeClass();

        starRocksAssert.withTable(cluster, "depts");
        starRocksAssert.withTable(cluster, "locations");
        starRocksAssert.withTable(cluster, "dependents");
        starRocksAssert.withTable(cluster, "emps");
        starRocksAssert.withTable(cluster, "emps_par");

        starRocksAssert.withTable(cluster, "test_all_type");
        starRocksAssert.withTable(cluster, "t0");
        starRocksAssert.withTable(cluster, "t1");
        starRocksAssert.withTable(cluster, "test10");
        starRocksAssert.withTable(cluster, "test11");

        prepareDatas();
    }

    public static void prepareDatas() throws Exception {
        starRocksAssert.withTable("CREATE TABLE test_partition_tbl1 (\n" +
                " k1 date NOT NULL,\n" +
                " v1 INT,\n" +
                " v2 INT)\n" +
                " DUPLICATE KEY(k1)\n" +
                " PARTITION BY RANGE(k1)\n" +
                " (\n" +
                "   PARTITION p1 VALUES LESS THAN ('2020-01-01'),\n" +
                "   PARTITION p2 VALUES LESS THAN ('2020-02-01'),\n" +
                "   PARTITION p3 VALUES LESS THAN ('2020-03-01'),\n" +
                "   PARTITION p4 VALUES LESS THAN ('2020-04-01'),\n" +
                "   PARTITION p5 VALUES LESS THAN ('2020-05-01'),\n" +
                "   PARTITION p6 VALUES LESS THAN ('2020-06-01')\n" +
                ")\n" +
                "DISTRIBUTED BY HASH(k1);");

        starRocksAssert.withTable("CREATE TABLE test_partition_tbl2 (\n" +
                " k1 date NOT NULL,\n" +
                " v1 INT,\n" +
                " v2 INT)\n" +
                " DUPLICATE KEY(k1)\n" +
                " PARTITION BY RANGE(k1)\n" +
                " (\n" +
                "   PARTITION p1 VALUES LESS THAN ('2020-01-01'),\n" +
                "   PARTITION p2 VALUES LESS THAN ('2020-02-01'),\n" +
                "   PARTITION p3 VALUES LESS THAN ('2020-03-01'),\n" +
                "   PARTITION p4 VALUES LESS THAN ('2020-04-01'),\n" +
                "   PARTITION p5 VALUES LESS THAN ('2020-05-01'),\n" +
                "   PARTITION p6 VALUES LESS THAN ('2020-06-01')\n" +
                ")\n" +
                "DISTRIBUTED BY HASH(k1);");

        starRocksAssert.withTable("CREATE TABLE test_partition_tbl_for_view (\n" +
                " k1 date NOT NULL,\n" +
                " v1 INT,\n" +
                " v2 INT)\n" +
                " DUPLICATE KEY(k1)\n" +
                " PARTITION BY RANGE(k1)\n" +
                " (\n" +
                "   PARTITION p1 VALUES LESS THAN ('2020-01-01'),\n" +
                "   PARTITION p2 VALUES LESS THAN ('2020-02-01'),\n" +
                "   PARTITION p3 VALUES LESS THAN ('2020-03-01'),\n" +
                "   PARTITION p4 VALUES LESS THAN ('2020-04-01'),\n" +
                "   PARTITION p5 VALUES LESS THAN ('2020-05-01'),\n" +
                "   PARTITION p6 VALUES LESS THAN ('2020-06-01')\n" +
                ")\n" +
                "DISTRIBUTED BY HASH(k1);");

        cluster.runSql("test", "insert into test_partition_tbl1 values (\"2019-01-01\",1,1),(\"2019-01-01\",1,2)," +
                "(\"2019-01-01\",2,1),(\"2019-01-01\",2,2),\n" +
                "(\"2020-01-11\",1,1),(\"2020-01-11\",1,2),(\"2020-01-11\",2,1),(\"2020-01-11\",2,2),\n" +
                "(\"2020-02-11\",1,1),(\"2020-02-11\",1,2),(\"2020-02-11\",2,1),(\"2020-02-11\",2,2);");

        cluster.runSql("test",
                "insert into test_partition_tbl_for_view values (\"2019-01-01\",1,1),(\"2019-01-01\",1,2)," +
                        "(\"2019-01-01\",2,1),(\"2019-01-01\",2,2),\n" +
                        "(\"2020-01-11\",1,1),(\"2020-01-11\",1,2),(\"2020-02-11\",2,1),(\"2020-02-11\",2,2),\n" +
                        "(\"2020-03-11\",1,1),(\"2020-04-11\",1,2),(\"2020-05-11\",2,1),(\"2020-05-11\",2,2);");
    }

    @Test
    public void testViewBasedMv() throws Exception {
        {
            starRocksAssert.withView("create view view1 as " +
                    " SELECT t0.v1 as v1, test_all_type.t1d, test_all_type.t1c" +
                    " from t0 join test_all_type" +
                    " on t0.v1 = test_all_type.t1d" +
                    " where t0.v1 < 100");

            createAndRefreshMv("create materialized view join_mv_1" +
                    " distributed by hash(v1)" +
                    " as " +
                    " SELECT * from view1");
            {
                String query = "SELECT (test_all_type.t1d + 1) * 2, test_all_type.t1c" +
                        " from t0 join test_all_type on t0.v1 = test_all_type.t1d where t0.v1 < 100";
                String plan = getFragmentPlan(query);
                PlanTestBase.assertContains(plan, "join_mv_1");
            }

            {
                String query = "SELECT (t1d + 1) * 2, t1c from view1 where v1 < 100";
                String plan = getFragmentPlan(query);
                PlanTestBase.assertContains(plan, "join_mv_1");
            }
            starRocksAssert.dropView("view1");
            dropMv("test", "join_mv_1");
        }

        // nested views
        {
            starRocksAssert.withView("create view view1 as " +
                    " SELECT v1 from t0");

            starRocksAssert.withView("create view view2 as " +
                    " SELECT t1d, t1c from test_all_type");

            starRocksAssert.withView("create view view3 as " +
                    " SELECT view1.v1, view2.t1d, view2.t1c" +
                    " from view1 join view2" +
                    " on view1.v1 = view2.t1d" +
                    " where view1.v1 < 100");

            createAndRefreshMv("create materialized view join_mv_1" +
                    " distributed by hash(v1)" +
                    " as " +
                    " SELECT * from view3");
            {
                String query = "SELECT (test_all_type.t1d + 1) * 2, test_all_type.t1c" +
                        " from t0 join test_all_type on t0.v1 = test_all_type.t1d where t0.v1 < 100";
                String plan = getFragmentPlan(query);
                PlanTestBase.assertContains(plan, "join_mv_1");
            }

            {
                String query = "SELECT (t1d + 1) * 2, t1c" +
                        " from view3 where v1 < 100";
                String plan = getFragmentPlan(query);
                PlanTestBase.assertContains(plan, "join_mv_1");
            }

            starRocksAssert.dropView("view1");
            starRocksAssert.dropView("view2");
            starRocksAssert.dropView("view3");
            dropMv("test", "join_mv_1");
        }

        // duplicate views
        {
            starRocksAssert.withView("create view view1 as " +
                    " SELECT v1 from t0");

            createAndRefreshMv("create materialized view join_mv_1" +
                    " distributed by hash(v11)" +
                    " as " +
                    " SELECT vv1.v1 v11, vv2.v1 v12 from view1 vv1 join view1 vv2 on vv1.v1 = vv2.v1");
            {
                String query = "SELECT vv1.v1, vv2.v1 from view1 vv1 join view1 vv2 on vv1.v1 = vv2.v1";
                String plan = getFragmentPlan(query);
                PlanTestBase.assertContains(plan, "join_mv_1");
            }

            starRocksAssert.dropView("view1");
            dropMv("test", "join_mv_1");
        }

        {
            starRocksAssert.withView("create view view1 as " +
                    " SELECT emps_par.deptno as deptno1, depts.deptno as deptno2, emps_par.empid, emps_par.name" +
                    " from emps_par join depts" +
                    " on emps_par.deptno = depts.deptno");
            createAndRefreshMv("create materialized view join_mv_2" +
                    " distributed by hash(deptno2)" +
                    " partition by deptno1" +
                    " as " +
                    " SELECT deptno1, deptno2, empid, name from view1 union SELECT deptno1, deptno2, empid, name from view1");

            createAndRefreshMv("create materialized view join_mv_1" +
                    " distributed by hash(deptno2)" +
                    " partition by deptno1" +
                    " as " +
                    " SELECT deptno1, deptno2, empid, name from view1");
            {
                String query = "SELECT deptno1, deptno2, empid, name from view1";
                String plan = getFragmentPlan(query);
                PlanTestBase.assertContains(plan, "join_mv_1");
            }

            starRocksAssert.dropView("view1");
            dropMv("test", "join_mv_1");
            dropMv("test", "join_mv_2");
        }
    }

    @Test
    public void testJoinMvRewriteByForceRuleRewrite() throws Exception {
        {
            createAndRefreshMv("create materialized view join_mv_1" +
                    " distributed by hash(v1)" +
                    " as " +
                    " SELECT t0.v1 as v1, test_all_type.t1d, test_all_type.t1c" +
                    " from t0 join test_all_type" +
                    " on t0.v1 = test_all_type.t1d" +
                    " where t0.v1 < 100");
            createAndRefreshMv("create materialized view join_mv_2" +
                    " distributed by hash(v1)" +
                    " as " +
                    " SELECT t0.v1 as v1, test_all_type.t1d, test_all_type.t1c" +
                    " from t0 join test_all_type" +
                    " on t0.v1 = test_all_type.t1d" +
                    " where t0.v1 < 100");

            connectContext.getSessionVariable().setEnableForceRuleBasedMvRewrite(true);
            String query1 = "SELECT (test_all_type.t1d + 1) * 2, test_all_type.t1c" +
                    " from t0 join test_all_type on t0.v1 = test_all_type.t1d where t0.v1 < 100";
            String plan1 = getFragmentPlan(query1);
            PlanTestBase.assertContains(plan1, "join_mv_");

            connectContext.getSessionVariable().setEnableForceRuleBasedMvRewrite(false);
            starRocksAssert.dropMaterializedView("join_mv_1");
            starRocksAssert.dropMaterializedView("join_mv_2");
        }
    }

    @Test
    public void testJoinMvRewrite1() throws Exception {
        createAndRefreshMv("create materialized view join_mv_1" +
                " distributed by hash(v1)" +
                " as " +
                " SELECT t0.v1 as v1, test_all_type.t1d, test_all_type.t1c" +
                " from t0 join test_all_type" +
                " on t0.v1 = test_all_type.t1d" +
                " where t0.v1 < 100");

        String query1 = "SELECT (test_all_type.t1d + 1) * 2, test_all_type.t1c" +
                " from t0 join test_all_type on t0.v1 = test_all_type.t1d where t0.v1 < 100";
        String plan1 = getFragmentPlan(query1);
        PlanTestBase.assertContains(plan1, "join_mv_1");
        OptExpression optExpression1 = getOptimizedPlan(query1, connectContext);
        List<PhysicalScanOperator> scanOperators = getScanOperators(optExpression1, "join_mv_1");
        Assertions.assertEquals(1, scanOperators.size());
        // column prune
        Assertions.assertFalse(scanOperators.get(0).getColRefToColumnMetaMap().keySet().toString().contains("t1d"));

        // t1e is not the output of mv
        String query2 = "SELECT (test_all_type.t1d + 1) * 2, test_all_type.t1c, test_all_type.t1e" +
                " from t0 join test_all_type on t0.v1 = test_all_type.t1d where t0.v1 < 100";
        String plan2 = getFragmentPlan(query2);
        PlanTestBase.assertNotContains(plan2, "join_mv_1");

        String query3 = "SELECT (test_all_type.t1d + 1) * 2, test_all_type.t1c" +
                " from t0 join test_all_type on t0.v1 = test_all_type.t1d where t0.v1 = 99";
        String plan3 = getFragmentPlan(query3);
        PlanTestBase.assertContainsIgnoreColRefs(plan3, "1:Project\n" +
                "  |  <slot 6> : 17: t1c\n" +
                "  |  <slot 14> : 15: v1 + 1 * 2\n" +
                "  |  \n" +
                "  0:OlapScanNode\n" +
                "     TABLE: join_mv_1\n" +
                "     PREAGGREGATION: ON\n" +
                "     PREDICATES: 15: v1 = 99");

        connectContext.getSessionVariable().setEnableMaterializedViewUnionRewrite(false);
        String query4 = "SELECT (test_all_type.t1d + 1) * 2, test_all_type.t1c" +
                " from t0 join test_all_type on t0.v1 = test_all_type.t1d where t0.v1 < 101";
        String plan4 = getFragmentPlan(query4);
        PlanTestBase.assertNotContains(plan4, "join_mv_1");

        String query5 = "SELECT (test_all_type.t1d + 1) * 2, test_all_type.t1c" +
                " from t0 join test_all_type on t0.v1 = test_all_type.t1d where t0.v1 < 100 and t0.v1 > 10";
        String plan5 = getFragmentPlan(query5);
        PlanTestBase.assertContains(plan5, "join_mv_1");

        String query6 = "SELECT (test_all_type.t1d + 1) * 2, test_all_type.t1c" +
                " from t0 join test_all_type on t0.v1 = test_all_type.t1d";
        String plan6 = getFragmentPlan(query6);
        PlanTestBase.assertNotContains(plan6, "join_mv_1");

        connectContext.getSessionVariable().setEnableMaterializedViewUnionRewrite(true);
        dropMv("test", "join_mv_1");
    }

    @Test
    public void testJoinMvRewrite2() throws Exception {
        createAndRefreshMv("create materialized view join_mv_2" +
                " distributed by hash(v1)" +
                " as " +
                " SELECT t0.v1 as v1, test_all_type.t1c" +
                " from t0 join test_all_type" +
                " on t0.v1 = test_all_type.t1d" +
                " where t0.v1 <= 100");

        // test on equivalence classes for output and predicates
        String query7 = "SELECT (test_all_type.t1d + 1) * 2, test_all_type.t1c" +
                " from t0 join test_all_type on t0.v1 = test_all_type.t1d where test_all_type.t1d < 100";
        String plan7 = getFragmentPlan(query7);
        PlanTestBase.assertContains(plan7, "join_mv_2");

        String query8 = "SELECT (test_all_type.t1d + 1) * 2, test_all_type.t1c" +
                " from t0 join test_all_type on t0.v1 = test_all_type.t1d where test_all_type.t1d < 10";
        String plan8 = getFragmentPlan(query8);
        PlanTestBase.assertContainsIgnoreColRefs(plan8, "1:Project\n" +
                "  |  <slot 6> : 16: t1c\n" +
                "  |  <slot 14> : 15: v1 + 1 * 2\n" +
                "  |  \n" +
                "  0:OlapScanNode\n" +
                "     TABLE: join_mv_2\n" +
                "     PREAGGREGATION: ON\n" +
                "     PREDICATES: 15: v1 < 10");
        String query9 = "SELECT (test_all_type.t1d + 1) * 2, test_all_type.t1c" +
                " from t0 join test_all_type on t0.v1 = test_all_type.t1d where test_all_type.t1d = 100";
        String plan9 = getFragmentPlan(query9);
        PlanTestBase.assertContains(plan9, "join_mv_2");

        String query10 = "SELECT (test_all_type.t1d + 1) * 2, test_all_type.t1c" +
                " from t0 join test_all_type on t0.v1 = test_all_type.t1d where t0.v1 between 1 and 10";
        String plan10 = getFragmentPlan(query10);
        PlanTestBase.assertContains(plan10, "join_mv_2");

        dropMv("test", "join_mv_2");
    }

    @Test
    public void testJoinMvRewrite3() throws Exception {
        createAndRefreshMv("create materialized view join_mv_3" +
                " distributed by hash(empid)" +
                " as" +
                " select emps.empid, depts.deptno, depts.name from emps join depts using (deptno)");
        String query11 = "select empid, depts.deptno from emps join depts using (deptno) where empid = 1";
        String plan11 = getFragmentPlan(query11);
        PlanTestBase.assertContains(plan11, "join_mv_3");
        String costPlan2 = getFragmentPlan(query11);
        PlanTestBase.assertContains(costPlan2, "join_mv_3");
        PlanTestBase.assertNotContains(costPlan2, "name-->");
        String newQuery11 = "select depts.deptno from emps join depts using (deptno) where empid = 1";
        String newPlan11 = getFragmentPlan(newQuery11);
        PlanTestBase.assertContains(newPlan11, "join_mv_3");
        OptExpression optExpression11 = getOptimizedPlan(newQuery11, connectContext);
        List<PhysicalScanOperator> scanOperators11 = getScanOperators(optExpression11, "join_mv_3");
        Assertions.assertEquals(1, scanOperators11.size());
        // column prune
        Assertions.assertFalse(scanOperators11.get(0).getColRefToColumnMetaMap().keySet().toString().contains("name"));

        String newQuery12 = "select depts.name from emps join depts using (deptno)";
        OptExpression optExpression12 = getOptimizedPlan(newQuery12, connectContext);
        List<PhysicalScanOperator> scanOperators12 = getScanOperators(optExpression12, "join_mv_3");
        Assertions.assertEquals(1, scanOperators12.size());
        // deptno is not projected
        Assertions.assertFalse(scanOperators12.get(0).getColRefToColumnMetaMap().keySet().toString().contains("deptno"));

        // join to scan with projection
        String newQuery13 = "select upper(depts.name) from emps join depts using (deptno)";
        OptExpression optExpression13 = getOptimizedPlan(newQuery13, connectContext);
        List<PhysicalScanOperator> scanOperators13 = getScanOperators(optExpression13, "join_mv_3");
        Assertions.assertEquals(1, scanOperators13.size());
        // deptno is not projected
        Assertions.assertFalse(scanOperators13.get(0).getColRefToColumnMetaMap().keySet().toString().contains("deptno"));

        // output on equivalence classes
        String query12 = "select empid, emps.deptno from emps join depts using (deptno) where empid = 1";
        String plan12 = getFragmentPlan(query12);
        PlanTestBase.assertContains(plan12, "join_mv_3");

        String query13 = "select empid, emps.deptno from emps join depts using (deptno) where empid > 1";
        String plan13 = getFragmentPlan(query13);
        PlanTestBase.assertContains(plan13, "join_mv_3");

        String query14 = "select empid, emps.deptno from emps join depts using (deptno) where empid < 1";
        String plan14 = getFragmentPlan(query14);
        PlanTestBase.assertContains(plan14, "join_mv_3");

        // query delta(query has three tables and view has two tabels) is supported
        // depts.name should be in the output of mv
        String query15 = "select emps.empid from emps join depts using (deptno)" +
                " join dependents on (depts.name = dependents.name)";
        String plan15 = getFragmentPlan(query15);
        PlanTestBase.assertContains(plan15, "join_mv_3");
        OptExpression optExpression15 = getOptimizedPlan(query15, connectContext);
        List<PhysicalScanOperator> scanOperators15 = getScanOperators(optExpression15, "join_mv_3");
        Assertions.assertEquals(1, scanOperators15.size());
        // column prune
        Assertions.assertFalse(scanOperators15.get(0).getColRefToColumnMetaMap().keySet().toString().contains("deptno"));

        // query delta depends on join reorder
        String query16 = "select dependents.empid from depts join dependents on (depts.name = dependents.name)" +
                " join emps on (emps.deptno = depts.deptno)";
        String plan16 = getFragmentPlan(query16, "MV");
        PlanTestBase.assertContains(plan16, "join_mv_3");
        OptExpression optExpression16 = getOptimizedPlan(query16, connectContext);
        List<PhysicalScanOperator> scanOperators16 = getScanOperators(optExpression16, "join_mv_3");
        Assertions.assertEquals(1, scanOperators16.size());
        // column prune
        Assertions.assertFalse(scanOperators16.get(0).getColRefToColumnMetaMap().keySet().toString().contains("deptno"));

        String query23 = "select dependents.empid from depts join dependents on (depts.name = dependents.name)" +
                " join emps on (emps.deptno = depts.deptno) where emps.deptno = 1";
        String plan23 = getFragmentPlan(query23);
        PlanTestBase.assertContains(plan23, "join_mv_3");

        // more tables
        String query17 = "select dependents.empid from depts join dependents on (depts.name = dependents.name)" +
                " join locations on (locations.name = dependents.name) join emps on (emps.deptno = depts.deptno)";
        String plan17 = getFragmentPlan(query17);
        PlanTestBase.assertContains(plan17, "join_mv_3");

        dropMv("test", "join_mv_3");
    }

    @Test
    public void testJoinMvRewrite4() throws Exception {
        createAndRefreshMv("create materialized view join_mv_4" +
                " distributed by hash(empid)" +
                " as" +
                " select emps.empid, emps.name as name1, emps.deptno, depts.name as name2 from emps join depts using (deptno)" +
                " where (depts.name is not null and emps.name ='a')" +
                " or (depts.name is not null and emps.name = 'b')" +
                " or (depts.name is not null and emps.name = 'c')");

        String query18 = "select depts.deptno, depts.name from emps join depts using (deptno)" +
                " where (depts.name is not null and emps.name = 'a')" +
                " or (depts.name is not null and emps.name = 'b')";
        String plan18 = getFragmentPlan(query18);
        PlanTestBase.assertContains(plan18, "join_mv_4");
        dropMv("test", "join_mv_4");
    }

    @Test
    public void testJoinMvRewrite5() throws Exception {
        createAndRefreshMv("create materialized view join_mv_5" +
                " distributed by hash(empid)" +
                " as" +
                " select emps.empid, emps.name as name1, emps.deptno, depts.name as name2 from emps join depts using (deptno)" +
                " where emps.name = 'a'");

        createAndRefreshMv("create materialized view join_mv_6" +
                " distributed by hash(empid)" +
                " as " +
                " select empid, deptno, name2 from join_mv_5 where name2 like \"%abc%\"");

        String query19 = "select emps.deptno, depts.name from emps join depts using (deptno)" +
                " where emps.name = 'a' and depts.name like \"%abc%\"";
        String plan19 = getFragmentPlan(query19);
        // the nested rewrite succeed, but the result depends on cost
        PlanTestBase.assertContains(plan19, "join_mv_");

        dropMv("test", "join_mv_5");
        dropMv("test", "join_mv_6");
    }

    @Test
    public void testJoinMvRewrite6() throws Exception {
        createAndRefreshMv("create materialized view join_mv_7" +
                " distributed by hash(empid)" +
                " as" +
                " select emps.empid from emps join depts using (deptno)");

        // TODO: rewrite on subquery
        String query20 = "select emps.empid from emps where deptno in (select deptno from depts)";
        String plan20 = getFragmentPlan(query20);
        PlanTestBase.assertContains(plan20, "join_mv_7");
        dropMv("test", "join_mv_7");
    }

    @Test
    public void testJoinMvRewrite7() throws Exception {
        // multi relations test
        createAndRefreshMv("create materialized view join_mv_8" +
                " distributed by hash(empid)" +
                " as" +
                " select emps1.empid, emps2.name from emps emps1 join emps emps2 on (emps1.empid = emps2.empid)");
        String query21 =
                "select emps1.name, emps2.empid from emps emps1 join emps emps2 on (emps1.empid = emps2.empid)";
        String plan21 = getFragmentPlan(query21);
        PlanTestBase.assertContains(plan21, "join_mv_8");
        dropMv("test", "join_mv_8");
    }

    @Test
    public void testJoinMvRewrite8() throws Exception {
        createAndRefreshMv("create materialized view join_mv_9" +
                " distributed by hash(empid)" +
                " as" +
                " select emps1.empid, emps2.name as name1, depts.name as name2 from emps emps1 join depts using (deptno)" +
                " join emps emps2 on (emps1.empid = emps2.empid)");
        String query22 = "select emps2.empid, emps1.name as name1, depts.name as name2" +
                " from emps emps2 join depts using (deptno)" +
                " join emps emps1 on (emps1.empid = emps2.empid)";
        String plan22 = getFragmentPlan(query22);
        PlanTestBase.assertContains(plan22, "join_mv_9");
        dropMv("test", "join_mv_9");

    }

    @Test
    public void testCrossJoin() throws Exception {
        createAndRefreshMv("create materialized view cross_join_mv1" +
                " distributed by hash(v1)" +
                " as " +
                " SELECT t0.v1 as v1, test_all_type.t1d, test_all_type.t1c" +
                " from t0 join test_all_type" +
                " where t0.v1 < 100");

        String query1 = "SELECT (test_all_type.t1d + 1) * 2, test_all_type.t1c" +
                " from t0 join test_all_type on t0.v1 = test_all_type.t1d where t0.v1 < 100";
        String plan1 = getFragmentPlan(query1);
        PlanTestBase.assertContains(plan1, "cross_join_mv1");
        dropMv("test", "cross_join_mv1");

        createAndRefreshMv("create materialized view cross_join_mv2" +
                " distributed by hash(empid)" +
                " as" +
                " select emps1.empid, emps2.name as name1, depts.name as name2 from emps emps1 join depts" +
                " join emps emps2 on (emps1.empid = emps2.empid)");
        String query22 = "select depts.name as name2" +
                " from emps emps2 join depts" +
                " join emps emps1 on (emps1.empid = emps2.empid)";
        String plan22 = getFragmentPlan(query22);
        PlanTestBase.assertContains(plan22, "cross_join_mv2");
        dropMv("test", "cross_join_mv2");
    }

    @Test
    public void testAggregateMvRewrite() throws Exception {
        createAndRefreshMv("create materialized view agg_join_mv_1" +
                " distributed by hash(v1) as SELECT t0.v1 as v1," +
                " test_all_type.t1d, sum(test_all_type.t1c) as total_sum, count(test_all_type.t1c) as total_num" +
                " from t0 join test_all_type on t0.v1 = test_all_type.t1d" +
                " where t0.v1 < 100" +
                " group by v1, test_all_type.t1d");

        String query1 = "SELECT t0.v1 as v1, test_all_type.t1d," +
                " sum(test_all_type.t1c) as total_sum, count(test_all_type.t1c) as total_num" +
                " from t0 join test_all_type" +
                " on t0.v1 = test_all_type.t1d" +
                " where t0.v1 = 1" +
                " group by v1, test_all_type.t1d";
        String plan1 = getFragmentPlan(query1);
        PlanTestBase.assertContains(plan1, "1:Project\n" +
                "  |  <slot 1> : clone(16: v1)\n" +
                "  |  <slot 7> : 16: v1\n" +
                "  |  <slot 14> : 18: total_sum\n" +
                "  |  <slot 15> : 19: total_num\n" +
                "  |  \n" +
                "  0:OlapScanNode\n" +
                "     TABLE: agg_join_mv_1\n" +
                "     PREAGGREGATION: ON\n" +
                "     PREDICATES: 16: v1 = 1");

        String query2 = "SELECT t0.v1 as v1, test_all_type.t1d," +
                " sum(test_all_type.t1c) as total_sum, count(test_all_type.t1c) as total_num" +
                " from t0 join test_all_type" +
                " on t0.v1 = test_all_type.t1d" +
                " where t0.v1 < 100" +
                " group by v1, test_all_type.t1d";
        starRocksAssert.getCtx().getSessionVariable().setEnableMaterializedViewPlanCache(false);
        String plan2 = getFragmentPlan(query2);
        PlanTestBase.assertContains(plan2, "1:Project\n" +
                "  |  <slot 1> : clone(16: v1)\n" +
                "  |  <slot 7> : 16: v1\n" +
                "  |  <slot 14> : 18: total_sum\n" +
                "  |  <slot 15> : 19: total_num\n" +
                "  |  \n" +
                "  0:OlapScanNode\n" +
                "     TABLE: agg_join_mv_1");

        String query3 = "SELECT t0.v1 as v1, test_all_type.t1d," +
                " sum(test_all_type.t1c) as total_sum, count(test_all_type.t1c) as total_num" +
                " from t0 join test_all_type" +
                " on t0.v1 = test_all_type.t1d" +
                " where t0.v1 < 99" +
                " group by v1, test_all_type.t1d";
        starRocksAssert.getCtx().getSessionVariable().setEnableMaterializedViewPlanCache(true);
        String plan3 = getFragmentPlan(query3);
        PlanTestBase.assertContains(plan3, "1:Project\n" +
                "  |  <slot 1> : clone(16: v1)\n" +
                "  |  <slot 7> : 16: v1\n" +
                "  |  <slot 14> : 18: total_sum\n" +
                "  |  <slot 15> : 19: total_num\n" +
                "  |  \n" +
                "  0:OlapScanNode\n" +
                "     TABLE: agg_join_mv_1\n" +
                "     PREAGGREGATION: ON\n" +
                "     PREDICATES: 16: v1 < 99");

        String query4 = "SELECT t0.v1 as v1, " +
                " sum(test_all_type.t1c) as total_sum, count(test_all_type.t1c) as total_num" +
                " from t0 join test_all_type" +
                " on t0.v1 = test_all_type.t1d" +
                " where t0.v1 < 99" +
                " group by v1";
        String plan4 = getFragmentPlan(query4);
        PlanTestBase.assertContains(plan4, "1:Project\n" +
                "  |  <slot 1> : 16: v1\n" +
                "  |  <slot 14> : 18: total_sum\n" +
                "  |  <slot 15> : 19: total_num\n" +
                "  |  \n" +
                "  0:OlapScanNode\n" +
                "     TABLE: agg_join_mv_1\n" +
                "     PREAGGREGATION: ON\n" +
                "     PREDICATES: 16: v1 < 99");

        // test group key not equal
        String query5 = "SELECT t0.v1 + 1 as alias, test_all_type.t1d," +
                " sum(test_all_type.t1c) as total_sum, count(test_all_type.t1c) as total_num" +
                " from t0 join test_all_type" +
                " on t0.v1 = test_all_type.t1d" +
                " where t0.v1 < 100" +
                " group by alias, test_all_type.t1d";
        String plan5 = getFragmentPlan(query5);
        PlanTestBase.assertContains(plan5, "agg_join_mv_1");
        PlanTestBase.assertContains(plan5, "  2:AGGREGATE (update finalize)\n" +
                "  |  output: sum(19: total_sum), sum(20: total_num)\n" +
                "  |  group by: 23: add, 17: v1");
        PlanTestBase.assertContains(plan5, "  1:Project\n" +
                "  |  <slot 17> : 17: v1\n" +
                "  |  <slot 19> : 19: total_sum\n" +
                "  |  <slot 20> : 20: total_num\n" +
                "  |  <slot 23> : 17: v1 + 1");

        MaterializedView mv1 = getMv("test", "agg_join_mv_1");
        dropMv("test", "agg_join_mv_1");
        Assertions.assertFalse(CachingMvPlanContextBuilder.getInstance().contains(mv1));

        createAndRefreshMv("create materialized view agg_join_mv_2" +
                " distributed by hash(v1) as SELECT t0.v1 as v1," +
                " test_all_type.t1b, sum(test_all_type.t1c) as total_sum, count(test_all_type.t1c) as total_num" +
                " from t0 join test_all_type on t0.v1 = test_all_type.t1d" +
                " where t0.v1 < 100" +
                " group by v1, test_all_type.t1b");
        String query6 = "SELECT t0.v1 as v1, " +
                " sum(test_all_type.t1c) as total_sum, count(test_all_type.t1c) as total_num" +
                " from t0 join test_all_type" +
                " on t0.v1 = test_all_type.t1d" +
                " where t0.v1 < 99" +
                " group by v1";
        // rollup test
        String plan6 = getFragmentPlan(query6);
        PlanTestBase.assertContains(plan6, "1:AGGREGATE (update finalize)\n" +
                "  |  output: sum(18: total_sum), sum(19: total_num)\n" +
                "  |  group by: 16: v1\n" +
                "  |  \n" +
                "  0:OlapScanNode\n" +
                "     TABLE: agg_join_mv_2\n" +
                "     PREAGGREGATION: ON\n" +
                "     PREDICATES: 16: v1 < 99");
        dropMv("test", "agg_join_mv_2");

        createAndRefreshMv("create materialized view agg_join_mv_3" +
                " distributed by hash(v1) as SELECT t0.v1 as v1," +
                " test_all_type.t1b, sum(test_all_type.t1c) * 2 as total_sum," +
                " count(distinct test_all_type.t1c) + 1 as total_num" +
                " from t0 join test_all_type on t0.v1 = test_all_type.t1d" +
                " where t0.v1 < 100" +
                " group by v1, test_all_type.t1b");

        // rollup with distinct
        String query7 = "SELECT t0.v1 as v1, " +
                " (sum(test_all_type.t1c)  * 2) + (count(distinct test_all_type.t1c) + 1) as total_sum," +
                " (count(distinct test_all_type.t1c) + 1) * 2 as total_num" +
                " from t0 join test_all_type" +
                " on t0.v1 = test_all_type.t1d" +
                " where t0.v1 < 99" +
                " group by v1";
        String plan7 = getFragmentPlan(query7);
        PlanTestBase.assertNotContains(plan7, "agg_join_mv_3");

        // distinct rewrite without rollup
        String query8 = "SELECT t0.v1, test_all_type.t1b," +
                " (sum(test_all_type.t1c) * 2) + 1 as total_sum, (count(distinct test_all_type.t1c) + 1) * 2 as total_num" +
                " from t0 join test_all_type" +
                " on t0.v1 = test_all_type.t1d" +
                " where t0.v1 < 99" +
                " group by v1, test_all_type.t1b";
        String plan8 = getFragmentPlan(query8);
        PlanTestBase.assertContains(plan8, "agg_join_mv_3");

        // test group by keys order change
        String query9 = "SELECT t0.v1, test_all_type.t1b," +
                " (sum(test_all_type.t1c) * 2) + 1 as total_sum, (count(distinct test_all_type.t1c) + 1) * 2 as total_num" +
                " from t0 join test_all_type" +
                " on t0.v1 = test_all_type.t1d" +
                " where t0.v1 < 99" +
                " group by test_all_type.t1b, v1";
        String plan9 = getFragmentPlan(query9);
        PlanTestBase.assertContains(plan9, "agg_join_mv_3");

        dropMv("test", "agg_join_mv_3");

        createAndRefreshMv("create materialized view agg_join_mv_4" +
                " distributed by hash(`deptno`) as SELECT deptno, count(*) as num from emps group by deptno");
        String query10 = "select deptno, count(*) from emps group by deptno";
        String plan10 = getFragmentPlan(query10);
        PlanTestBase.assertContains(plan10, "agg_join_mv_4");

        String query11 = "select count(*) from emps";
        String plan11 = getFragmentPlan(query11);
        PlanTestBase.assertContains(plan11, "agg_join_mv_4");
        OptExpression optExpression11 = getOptimizedPlan(query11, connectContext);
        List<PhysicalScanOperator> scanOperators11 = getScanOperators(optExpression11, "agg_join_mv_4");
        Assertions.assertEquals(1, scanOperators11.size());
        // column prune
        Assertions.assertFalse(scanOperators11.get(0).getColRefToColumnMetaMap().keySet().toString().contains("deptno"));
        dropMv("test", "agg_join_mv_4");

        createAndRefreshMv("create materialized view agg_join_mv_5" +
                " distributed by hash(`deptno`) as SELECT deptno, count(1) as num from emps group by deptno");
        String query12 = "select deptno, count(1) from emps group by deptno";
        String plan12 = getFragmentPlan(query12);
        PlanTestBase.assertContains(plan12, "agg_join_mv_5");

        dropMv("test", "agg_join_mv_5");

        // test aggregate with projection
        createAndRefreshMv("create materialized view agg_mv_6" +
                " distributed by hash(`empid`) as select empid, abs(empid) as abs_empid, avg(salary) as total" +
                " from emps group by empid");

        MaterializedView mv = getMv("test", "agg_mv_6");
        Column idColumn = mv.getColumn("empid");
        Assertions.assertFalse(idColumn.isAllowNull());
        Column totalColumn = mv.getColumn("total");
        Assertions.assertTrue(totalColumn.isAllowNull());

        String query13 = "select empid, abs(empid), avg(salary) from emps group by empid";
        String plan13 = getFragmentPlan(query13);
        PlanTestBase.assertContains(plan13, "agg_mv_6");

        String query14 = "select empid, avg(salary) from emps group by empid";
        OptExpression optExpression14 = getOptimizedPlan(query14, connectContext);
        List<PhysicalScanOperator> scanOperators14 = getScanOperators(optExpression14, "agg_mv_6");
        Assertions.assertEquals(1, scanOperators14.size());
        // column prune
        Assertions.assertFalse(scanOperators14.get(0).getColRefToColumnMetaMap().keySet().toString().contains("abs_empid"));

        String query15 = "select abs(empid), avg(salary) from emps group by empid";
        String plan15 = getFragmentPlan(query15);
        PlanTestBase.assertContains(plan15, "agg_mv_6");

        // avg can not be rolled up
        String query16 = "select avg(salary) from emps";
        String plan16 = getFragmentPlan(query16);
        PlanTestBase.assertNotContains(plan16, "agg_mv_6");
        dropMv("test", "agg_mv_6");

        createAndRefreshMv("create materialized view agg_mv_7" +
                " distributed by hash(`empid`) as select empid, abs(empid) as abs_empid," +
                " sum(salary) as total, count(salary) as cnt" +
                " from emps group by empid");

        String query17 = "select empid, abs(empid), sum(salary), count(salary) from emps group by empid";
        String plan17 = getFragmentPlan(query17);
        PlanTestBase.assertContains(plan17, "agg_mv_7");

        String query19 = "select abs(empid), sum(salary), count(salary) from emps group by empid";
        String plan19 = getFragmentPlan(query19);
        PlanTestBase.assertContains(plan19, "agg_mv_7");

        String query20 = "select sum(salary), count(salary) from emps";
        OptExpression optExpression20 = getOptimizedPlan(query20, connectContext);
        List<PhysicalScanOperator> scanOperators20 = getScanOperators(optExpression20, "agg_mv_7");
        Assertions.assertEquals(1, scanOperators20.size());
        // column prune
        Assertions.assertFalse(scanOperators20.get(0).getColRefToColumnMetaMap().keySet().toString().contains("empid"));
        Assertions.assertFalse(scanOperators20.get(0).getColRefToColumnMetaMap().keySet().toString().contains("abs_empid"));

        String query27 = "select sum(salary), count(salary) from emps";
        OptExpression optExpression27 = getOptimizedPlan(query27, connectContext);
        List<PhysicalScanOperator> scanOperators27 = getScanOperators(optExpression27, "agg_mv_7");
        Assertions.assertEquals(1, scanOperators27.size());
        // column prune
        Assertions.assertFalse(scanOperators27.get(0).getColRefToColumnMetaMap().keySet().toString().contains("empid"));
        Assertions.assertFalse(scanOperators27.get(0).getColRefToColumnMetaMap().keySet().toString().contains("abs_empid"));

        dropMv("test", "agg_mv_7");

        createAndRefreshMv("create materialized view agg_mv_8" +
                " distributed by hash(`empid`) as select empid, deptno," +
                " sum(salary) as total, count(salary) + 1 as cnt" +
                " from emps group by empid, deptno");

        // abs(empid) can not be rewritten
        String query21 = "select abs(empid), sum(salary) from emps group by empid";
        String plan21 = getFragmentPlan(query21);
        PlanTestBase.assertContains(plan21, "agg_mv_8");

        // count(salary) + 1 cannot be rewritten
        String query22 = "select sum(salary), count(salary) + 1 from emps";
        String plan22 = getFragmentPlan(query22);
        PlanTestBase.assertNotContains(plan22, "agg_mv_8");

        String query23 = "select sum(salary) from emps";
        String plan23 = getFragmentPlan(query23);
        PlanTestBase.assertContains(plan23, "agg_mv_8");

        String query24 = "select empid, sum(salary) from emps group by empid";
        String plan24 = getFragmentPlan(query24);
        PlanTestBase.assertContains(plan24, "agg_mv_8");

        dropMv("test", "agg_mv_8");

        createAndRefreshMv("create materialized view agg_mv_9" +
                " distributed by hash(`deptno`) as select deptno," +
                " count(distinct empid) as num" +
                " from emps group by deptno");

        String query25 = "select deptno, count(distinct empid) from emps group by deptno";
        String plan25 = getFragmentPlan(query25);
        PlanTestBase.assertContains(plan25, "agg_mv_9");
        dropMv("test", "agg_mv_9");

        starRocksAssert.withTable("CREATE TABLE `test_table_1` (\n" +
                "  `dt` date NULL COMMENT \"\",\n" +
                "  `experiment_id` bigint(20) NULL COMMENT \"\",\n" +
                "  `hour` varchar(65533) NULL COMMENT \"\",\n" +
                "  `player_id` varchar(65533) NULL COMMENT \"\",\n" +
                "  `metric_value` double NULL COMMENT \"\"\n" +
                ") ENGINE=OLAP \n" +
                "DUPLICATE KEY(`dt`, `experiment_id`, `hour`)\n" +
                "COMMENT \"OLAP\"\n" +
                "PARTITION BY RANGE(`dt`)\n" +
                "(PARTITION p202207 VALUES [(\"2022-07-01\"), (\"2022-08-01\")),\n" +
                "PARTITION p202208 VALUES [(\"2022-08-01\"), (\"2022-09-01\")),\n" +
                "PARTITION p202209 VALUES [(\"2022-09-01\"), (\"2022-10-01\")),\n" +
                "PARTITION p202210 VALUES [(\"2022-10-01\"), (\"2022-11-01\")))\n" +
                "DISTRIBUTED BY HASH(`player_id`) BUCKETS 160 \n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\",\n" +
                "\"enable_persistent_index\" = \"true\",\n" +
                "\"compression\" = \"LZ4\"\n" +
                ");");
        cluster.runSql("test", "insert into test_table_1 values('2022-07-01', 1, '08:00:00', 'player_id_1', 20.0)");
        cluster.runSql("test", "insert into test_table_1 values('2022-08-01', 1, '08:00:00', 'player_id_1', 20.0)");
        cluster.runSql("test", "insert into test_table_1 values('2022-09-01', 1, '08:00:00', 'player_id_1', 20.0)");
        cluster.runSql("test", "insert into test_table_1 values('2022-10-01', 1, '08:00:00', 'player_id_1', 20.0)");

        createAndRefreshMv("create materialized view agg_mv_10" +
                " distributed by hash(experiment_id)\n" +
                " refresh manual\n" +
                " as\n" +
                " SELECT `dt`, `experiment_id`," +
                "       count(DISTINCT `player_id`) AS `count_distinct_player_id`\n" +
                " FROM `test_table_1`\n" +
                " GROUP BY `dt`, `experiment_id`;");

        String query26 = "SELECT `dt`, `experiment_id`," +
                " count(DISTINCT `player_id`) AS `count_distinct_player_id`\n" +
                "FROM `test_table_1`\n" +
                "GROUP BY `dt`, `experiment_id`";
        String plan26 = getFragmentPlan(query26);
        PlanTestBase.assertContains(plan26, "agg_mv_10");
        dropMv("test", "agg_mv_10");
        starRocksAssert.dropTable("test_table_1");
    }

    @Test
    public void testAggExprRewrite() throws Exception {
        // Group by Cast Expr
        starRocksAssert.withMTable("json_tbl",
                () -> {
                    {
                        String mvName = "mv_q15";
                        createAndRefreshMv("CREATE MATERIALIZED VIEW `mv_q15`\n" +
                                "DISTRIBUTED BY HASH(`gender`) BUCKETS 2\n" +
                                "REFRESH ASYNC\n" +
                                "AS \n" +
                                "SELECT \n" +
                                "    CAST((`d_user`->'gender') AS string) AS `gender`, \n" +
                                "    count(d_user) AS `cnt`\n" +
                                "FROM `json_tbl`\n" +
                                "GROUP BY `gender`");
                        String query =
                                "SELECT \n" +
                                        "    CAST((`d_user`->'gender') AS string) AS `gender`, \n" +
                                        "    count(d_user) AS `cnt`\n" +
                                        "FROM `json_tbl`\n" +
                                        "GROUP BY `gender`;";
                        PlanTestBase.assertContains(getFragmentPlan(query), mvName);
                    }

                    {
                        // Agg with Cast Expr
                        String mvName = "mv_q16";
                        createAndRefreshMv("CREATE MATERIALIZED VIEW `mv_q16`\n" +
                                "DISTRIBUTED BY HASH(`gender`) BUCKETS 2\n" +
                                "REFRESH ASYNC\n" +
                                "AS \n" +
                                "SELECT \n" +
                                "    CAST((`d_user`->'gender') AS string) AS `gender`, \n" +
                                "    sum(cast(d_user->'age' as int)) AS `sum`\n" +
                                "FROM `json_tbl`\n" +
                                "GROUP BY `gender`");
                        String query =
                                "SELECT \n" +
                                        "    CAST((`d_user`->'gender') AS string) AS `gender`, \n" +
                                        "    sum(cast(d_user->'age' as int)) AS `sum`\n" +
                                        "FROM `json_tbl`\n" +
                                        "GROUP BY `gender`;";
                        PlanTestBase.assertContains(getFragmentPlan(query), mvName);
                    }
                });
    }

    @Test
    public void testCardinality() throws Exception {
        try {
            FeConstants.USE_MOCK_DICT_MANAGER = true;
            createAndRefreshMv("CREATE MATERIALIZED VIEW emp_lowcard_sum" +
                    " DISTRIBUTED BY HASH(empid) AS SELECT empid, name, sum(salary) as sum_sal from emps group by " +
                    "empid, name;");
            String sql = "select name from emp_lowcard_sum group by name";
            String plan = getFragmentPlan(sql);
            Assertions.assertTrue(plan.contains("Decode"));
        } finally {
            dropMv("test", "emp_lowcard_sum");
            FeConstants.USE_MOCK_DICT_MANAGER = false;
        }
    }

    @Test
    public void testPkFk() throws SQLException {
        starRocksAssert.withMTables(List.of(
                        new MTable("parent_table1", "k1",
                                List.of(
                                        "k1 INT",
                                        "k2 VARCHAR(20)",
                                        "k3 INT",
                                        "k4 VARCHAR(20)"
                                )
                        ).withProperties(
                                "'unique_constraints' = 'k1,k2'"
                        ),
                        new MTable("parent_table2", "k1",
                                List.of(
                                        "k1 INT",
                                        "k2 VARCHAR(20)",
                                        "k3 INT",
                                        "k4 VARCHAR(20)"
                                )
                        ).withProperties(
                                "'unique_constraints' = 'k1,k2'"
                        ),
                        new MTable(
                                "base_table1", "k1",
                                List.of(
                                        "k1 INT",
                                        "k2 VARCHAR(20)",
                                        "k3 INT",
                                        "k4 VARCHAR(20)",
                                        "k5 INT",
                                        "k6 VARCHAR(20)",
                                        "k7 INT",
                                        "k8 VARCHAR(20)",
                                        "k9 INT",
                                        "k10 VARCHAR(20)"
                                )
                        ).withProperties(
                                "'foreign_key_constraints' = '(k3,k4) REFERENCES parent_table1(k1, k2)'"
                        )
                ),
                () -> {
                    OlapTable olapTable = (OlapTable) getTable("test", "parent_table1");
                    Assertions.assertNotNull(olapTable.getUniqueConstraints());
                    Assertions.assertEquals(1, olapTable.getUniqueConstraints().size());
                    UniqueConstraint uniqueConstraint = olapTable.getUniqueConstraints().get(0);
                    Assertions.assertEquals(2, uniqueConstraint.getUniqueColumnNames(olapTable).size());
                    Assertions.assertEquals("k1", uniqueConstraint.getUniqueColumnNames(olapTable).get(0));
                    Assertions.assertEquals("k2", uniqueConstraint.getUniqueColumnNames(olapTable).get(1));

                    cluster.runSql("test", "alter table parent_table1 set(\"unique_constraints\"=\"k1, k2; k3; k4\")");
                    Assertions.assertNotNull(olapTable.getUniqueConstraints());
                    Assertions.assertEquals(3, olapTable.getUniqueConstraints().size());
                    UniqueConstraint uniqueConstraint2 = olapTable.getUniqueConstraints().get(0);
                    Assertions.assertEquals(2, uniqueConstraint2.getUniqueColumnNames(olapTable).size());
                    Assertions.assertEquals("k1", uniqueConstraint2.getUniqueColumnNames(olapTable).get(0));
                    Assertions.assertEquals("k2", uniqueConstraint2.getUniqueColumnNames(olapTable).get(1));

                    UniqueConstraint uniqueConstraint3 = olapTable.getUniqueConstraints().get(1);
                    Assertions.assertEquals(1, uniqueConstraint3.getUniqueColumnNames(olapTable).size());
                    Assertions.assertEquals("k3", uniqueConstraint3.getUniqueColumnNames(olapTable).get(0));

                    UniqueConstraint uniqueConstraint4 = olapTable.getUniqueConstraints().get(2);
                    Assertions.assertEquals(1, uniqueConstraint4.getUniqueColumnNames(olapTable).size());
                    Assertions.assertEquals("k4", uniqueConstraint4.getUniqueColumnNames(olapTable).get(0));

                    cluster.runSql("test", "alter table parent_table1 set(\"unique_constraints\"=\"\")");
                    Assertions.assertTrue(olapTable.getUniqueConstraints().isEmpty());

                    cluster.runSql("test", "alter table parent_table1 set(\"unique_constraints\"=\"k1, k2\")");

                    OlapTable baseTable = (OlapTable) getTable("test", "base_table1");
                    Assertions.assertNotNull(baseTable.getForeignKeyConstraints());
                    List<ForeignKeyConstraint> foreignKeyConstraints = baseTable.getForeignKeyConstraints();
                    Assertions.assertEquals(1, foreignKeyConstraints.size());
                    BaseTableInfo parentTable = foreignKeyConstraints.get(0).getParentTableInfo();
                    Assertions.assertEquals(olapTable.getId(), parentTable.getTableId());
                    Assertions.assertEquals(2, foreignKeyConstraints.get(0).getColumnRefPairs().size());
                    Assertions.assertEquals(ColumnId.create("k3"), foreignKeyConstraints.get(0).getColumnRefPairs().get(0).first);
                    Assertions.assertEquals(ColumnId.create("k1"),
                            foreignKeyConstraints.get(0).getColumnRefPairs().get(0).second);
                    Assertions.assertEquals(ColumnId.create("k4"), foreignKeyConstraints.get(0).getColumnRefPairs().get(1).first);
                    Assertions.assertEquals(ColumnId.create("k2"),
                            foreignKeyConstraints.get(0).getColumnRefPairs().get(1).second);

                    cluster.runSql("test", "alter table base_table1 set(" +
                            "\"foreign_key_constraints\"=\"(k3,k4) references parent_table1(k1, k2);" +
                            "(k5,k6) REFERENCES parent_table2(k1, k2)\")");

                    List<ForeignKeyConstraint> foreignKeyConstraints2 = baseTable.getForeignKeyConstraints();
                    Assertions.assertEquals(2, foreignKeyConstraints2.size());
                    BaseTableInfo parentTableInfo2 = foreignKeyConstraints2.get(1).getParentTableInfo();
                    OlapTable parentTable2 = (OlapTable) getTable("test", "parent_table2");
                    Assertions.assertEquals(parentTable2.getId(), parentTableInfo2.getTableId());
                    Assertions.assertEquals(2, foreignKeyConstraints2.get(1).getColumnRefPairs().size());
                    Assertions.assertEquals(ColumnId.create("k5"),
                            foreignKeyConstraints2.get(1).getColumnRefPairs().get(0).first);
                    Assertions.assertEquals(ColumnId.create("k1"),
                            foreignKeyConstraints2.get(1).getColumnRefPairs().get(0).second);
                    Assertions.assertEquals(ColumnId.create("k6"),
                            foreignKeyConstraints2.get(1).getColumnRefPairs().get(1).first);
                    Assertions.assertEquals(ColumnId.create("k2"),
                            foreignKeyConstraints2.get(1).getColumnRefPairs().get(1).second);

                    cluster.runSql("test", "show create table base_table1");
                    cluster.runSql("test", "alter table base_table1 set(" +
                            "\"foreign_key_constraints\"=\"\")");
                    List<ForeignKeyConstraint> foreignKeyConstraints3 = baseTable.getForeignKeyConstraints();
                    Assertions.assertNull(foreignKeyConstraints3);
                }
        );
    }

    @Test
    public void testTabletHintForbidMvRewrite() throws Exception {
        createAndRefreshMv("create materialized view forbid_mv_1" +
                " distributed by hash(t1d) as SELECT " +
                " test_all_type.t1d, sum(test_all_type.t1c) as total_sum, count(test_all_type.t1c) as total_num" +
                " from test_all_type" +
                " group by test_all_type.t1d");

        String query1 = "SELECT test_all_type.t1d," +
                " sum(test_all_type.t1c) as total_sum, count(test_all_type.t1c) as total_num" +
                " from test_all_type" +
                " group by test_all_type.t1d";
        String plan1 = getFragmentPlan(query1);
        PlanTestBase.assertContains(plan1, "  0:OlapScanNode\n" +
                "     TABLE: forbid_mv_1\n" +
                "     PREAGGREGATION: ON");
        ShowResultSet tablets = starRocksAssert.showTablet("test", "test_all_type");
        List<String> tabletIds = tablets.getResultRows().stream().map(r -> r.get(0)).collect(Collectors.toList());
        String tabletHint = String.format("tablet(%s)", tabletIds.get(0));
        String query2 = "SELECT test_all_type.t1d," +
                " sum(test_all_type.t1c) as total_sum, count(test_all_type.t1c) as total_num" +
                " from test_all_type " + tabletHint +
                " group by test_all_type.t1d";

        String plan2 = getFragmentPlan(query2);
        PlanTestBase.assertContains(plan2, "  0:OlapScanNode\n" +
                "     TABLE: test_all_type\n" +
                "     PREAGGREGATION: ON");
        dropMv("test", "forbid_mv_1");
    }

    @Test
    public void testJoinPredicatePushdown() throws Exception {
        List<String> tables = ImmutableList.of("pushdown_t1", "pushdown_t2");
        cluster.runSql("test", "CREATE TABLE pushdown_t1 (\n" +
                "    `c0` string,\n" +
                "    `c1` string,\n" +
                "    `c2` string,\n" +
                "    `c3` string,\n" +
                "    `c4` string,\n" +
                "    `c5` string ,\n" +
                "    `c6` string,\n" +
                "    `c7`  date\n" +
                ") \n" +
                "DUPLICATE KEY (c0)\n" +
                "DISTRIBUTED BY HASH(c0)\n" +
                "properties('replication_num' = '1');");
        cluster.runSql("test", "CREATE TABLE `pushdown_t2` (\n" +
                "  `c0` varchar(65533) NULL ,\n" +
                "  `c1` varchar(65533) NULL ,\n" +
                "  `c2` varchar(65533) NULL ,\n" +
                "  `c3` varchar(65533) NULL ,\n" +
                "  `c4` varchar(65533) NULL ,\n" +
                "  `c5` varchar(65533) NULL ,\n" +
                "  `c6` varchar(65533) NULL ,\n" +
                "  `c7` varchar(65533) NULL ,\n" +
                "  `c8` varchar(65533) NULL ,\n" +
                "  `c9` varchar(65533) NULL ,\n" +
                "  `c10` varchar(65533) NULL ,\n" +
                "  `c11` date NULL ,\n" +
                "  `c12` datetime NULL \n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY (c0)\n" +
                "DISTRIBUTED BY HASH(c0)\n" +
                "PROPERTIES ( \"replication_num\" = \"1\");");

        // With null-rejecting predicate
        createAndRefreshMv("CREATE MATERIALIZED VIEW `_pushdown_predicate_join_mv1`  \n" +
                "DISTRIBUTED BY HASH(c12) BUCKETS 18 \n" +
                "REFRESH MANUAL \n" +
                "PROPERTIES ( \"replication_num\" = \"1\", \"storage_medium\" = \"HDD\") \n" +
                "AS\n" +
                "SELECT t1.c0, t1.c1, t2.c7, t2.c12\n" +
                "FROM\n" +
                "    ( SELECT `c0`, `c7`, `c12` FROM `pushdown_t2`) t2\n" +
                "    LEFT OUTER JOIN \n" +
                "    ( SELECT c0, c1, c7 FROM pushdown_t1 ) t1\n" +
                "    ON `t2`.`c0` = `t1`.`c0`\n" +
                "    AND t2.c0 IS NOT NULL " +
                "    AND date(t2.`c12`) = `t1`.`c7`\n" +
                "   ;");

        String query = "SELECT t1.c0, t1.c1, t2.c7, t2.c12\n" +
                "FROM\n" +
                "    ( SELECT `c0`, `c7`, `c12` FROM `pushdown_t2`) t2\n" +
                "    LEFT OUTER JOIN \n" +
                "    ( SELECT c0, c1, c7 FROM pushdown_t1 ) t1\n" +
                "    ON `t2`.`c0` = `t1`.`c0`\n" +
                "    AND t2.c0 IS NOT NULL " +
                "    AND date(t2.`c12`) = `t1`.`c7`\n" +
                "   ;";
        String plan = getFragmentPlan(query);
        PlanTestBase.assertContains(plan, "_pushdown_predicate_join_mv1");
        starRocksAssert.dropTables(tables);
    }

    @Test
    public void testJoinPredicatePushdown1() throws Exception {
        List<String> tables = ImmutableList.of("pushdown_t1", "pushdown_t2");
        cluster.runSql("test", "CREATE TABLE pushdown_t1 (\n" +
                "    `c0` string,\n" +
                "    `c1` string,\n" +
                "    `c2` string,\n" +
                "    `c3` string,\n" +
                "    `c4` string,\n" +
                "    `c5` string ,\n" +
                "    `c6` string,\n" +
                "    `c7`  date\n" +
                ") \n" +
                "DUPLICATE KEY (c0)\n" +
                "DISTRIBUTED BY HASH(c0)\n" +
                "properties('replication_num' = '1');");
        cluster.runSql("test", "CREATE TABLE `pushdown_t2` (\n" +
                "  `c0` varchar(65533) NULL ,\n" +
                "  `c1` varchar(65533) NULL ,\n" +
                "  `c2` varchar(65533) NULL ,\n" +
                "  `c3` varchar(65533) NULL ,\n" +
                "  `c4` varchar(65533) NULL ,\n" +
                "  `c5` varchar(65533) NULL ,\n" +
                "  `c6` varchar(65533) NULL ,\n" +
                "  `c7` varchar(65533) NULL ,\n" +
                "  `c8` varchar(65533) NULL ,\n" +
                "  `c9` varchar(65533) NULL ,\n" +
                "  `c10` varchar(65533) NULL ,\n" +
                "  `c11` date NULL ,\n" +
                "  `c12` datetime NULL \n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY (c0)\n" +
                "DISTRIBUTED BY HASH(c0)\n" +
                "PROPERTIES ( \"replication_num\" = \"1\");");

        // Without null-rejecting predicate
        createAndRefreshMv("CREATE MATERIALIZED VIEW `_pushdown_predicate_join_mv2`  \n" +
                "DISTRIBUTED BY HASH(c12) BUCKETS 18 \n" +
                "REFRESH MANUAL \n" +
                "PROPERTIES ( \"replication_num\" = \"1\", \"storage_medium\" = \"HDD\") \n" +
                "AS\n" +
                "SELECT t1.c0, t1.c1, t2.c7, t2.c12\n" +
                "FROM\n" +
                "    ( SELECT `c0`, `c7`, `c12` FROM `pushdown_t2`) t2\n" +
                "    LEFT OUTER JOIN \n" +
                "    ( SELECT c0, c1, c7 FROM pushdown_t1 ) t1\n" +
                "    ON `t2`.`c0` = `t1`.`c0`\n" +
                "    AND date(t2.`c12`) = `t1`.`c7`\n" +
                "   ;");

        String query = "SELECT t1.c0, t1.c1, t2.c7, t2.c12\n" +
                "FROM\n" +
                "    ( SELECT `c0`, `c7`, `c12` FROM `pushdown_t2`) t2\n" +
                "    LEFT OUTER JOIN \n" +
                "    ( SELECT c0, c1, c7 FROM pushdown_t1 ) t1\n" +
                "    ON `t2`.`c0` = `t1`.`c0`\n" +
                "    AND date(t2.`c12`) = `t1`.`c7`\n" +
                "   ;";

        String plan = getFragmentPlan(query);
        PlanTestBase.assertContains(plan, "_pushdown_predicate_join_mv2");
        starRocksAssert.dropTables(tables);
    }

    @Test
    public void testNonpartitionedMvWithPartitionPredicate() throws Exception {
        createAndRefreshMv("create materialized view mv_with_partition_predicate_1 distributed by hash(`k1`)" +
                " as select k1, v1 from t1 where k1 = 3;");
        String query = "select k1, v1 from t1 where k1 = 3;";
        String plan = getFragmentPlan(query);
        PlanTestBase.assertContains(plan, "mv_with_partition_predicate_1");
        starRocksAssert.dropMaterializedView("mv_with_partition_predicate_1");
    }

    @Test
    public void testJoinWithTypeCast() throws Exception {
        String sql1 = "create table test.dim_tbl1 (\n" +
                "    col1 string,\n" +
                "    col1_name string\n" +
                ")DISTRIBUTED BY HASH(col1)" +
                ";\n";
        String sql2 = "CREATE TABLE test.fact_tbl1( \n" +
                "          fdate  int,\n" +
                "          fqqid STRING ,\n" +
                "          col1 BIGINT  ,\n" +
                "          flcnt BIGINT\n" +
                " )PARTITION BY range(fdate) (\n" +
                "    PARTITION p1 VALUES [ (\"20230702\"),(\"20230703\")),\n" +
                "    PARTITION p2 VALUES [ (\"20230703\"),(\"20230704\")),\n" +
                "    PARTITION p3 VALUES [ (\"20230705\"),(\"20230706\"))\n" +
                " )\n" +
                " DISTRIBUTED BY HASH(fqqid);";
        String mv = "create MATERIALIZED VIEW test.test_mv1\n" +
                "DISTRIBUTED BY HASH(fdate,col1_name)\n" +
                "REFRESH MANUAL\n" +
                "AS \n" +
                "    select t1.fdate, t2.col1_name,  count(DISTINCT t1.fqqid) AS index_0_8228, sum(t1.flcnt)as index_xxx\n" +
                "    FROM test.fact_tbl1 t1 \n" +
                "    LEFT JOIN test.dim_tbl1 t2\n" +
                "    ON t1.`col1` = t2.`col1`\n" +
                "    WHERE t1.`fdate` >= 20230701 and t1.fdate <= 20230705\n" +
                "    GROUP BY  fdate, `col1_name`;";
        starRocksAssert.withTable(sql1);
        starRocksAssert.withTable(sql2);
        starRocksAssert.withMaterializedView(mv);
        String sql =
                "select t1.fdate, t2.col1_name,  count(DISTINCT t1.fqqid) AS index_0_8228, sum(t1.flcnt)as index_xxx\n" +
                        "    FROM test.fact_tbl1 t1 \n" +
                        "    LEFT JOIN test.dim_tbl1 t2\n" +
                        "    ON t1.`col1` = t2.`col1`\n" +
                        "    WHERE t1.`fdate` >= 20230702 and t1.fdate <= 20230705\n" +
                        "    GROUP BY  fdate, `col1_name`;";
        String plan = getFragmentPlan(sql);
        PlanTestBase.assertContains(plan, "test_mv1");

        sql = "select t2.col1_name,  count(DISTINCT t1.fqqid) AS index_0_8228, sum(t1.flcnt)as index_xxx\n" +
                "    FROM test.fact_tbl1 t1 \n" +
                "    LEFT JOIN test.dim_tbl1 t2\n" +
                "    ON t1.`col1` = t2.`col1`\n" +
                "    WHERE t1.`fdate` = 20230705\n" +
                "    GROUP BY   `col1_name`;";
        plan = getFragmentPlan(sql);
        PlanTestBase.assertContains(plan, "test_mv1");

        sql = "select t2.col1_name,  sum(t1.flcnt)as index_xxx\n" +
                "    FROM test.fact_tbl1 t1 \n" +
                "    LEFT JOIN test.dim_tbl1 t2\n" +
                "    ON t1.`col1` = t2.`col1`\n" +
                "    WHERE t1.`fdate` = 20230705\n" +
                "    GROUP BY   `col1_name`;";
        plan = getFragmentPlan(sql);
        PlanTestBase.assertContains(plan, "test_mv1");

        sql = "select t2.col1_name,  sum(t1.flcnt)as index_xxx\n" +
                "    FROM test.fact_tbl1 t1 \n" +
                "    LEFT JOIN test.dim_tbl1 t2\n" +
                "    ON t1.`col1` = t2.`col1`\n" +
                "    WHERE t1.`fdate` >= 20230702 and t1.fdate <= 20230705\n" +
                "    GROUP BY   `col1_name`;";
        plan = getFragmentPlan(sql);
        PlanTestBase.assertContains(plan, "test_mv1");
    }

    @Test
    public void testPartitionPrune1() throws Exception {
        createAndRefreshMv("CREATE MATERIALIZED VIEW test_partition_tbl_mv1\n" +
                "               PARTITION BY k1\n" +
                "               DISTRIBUTED BY HASH(k1) BUCKETS 10\n" +
                "               REFRESH ASYNC\n" +
                "               PROPERTIES(\n" +
                "               \"partition_ttl_number\"=\"4\",\n" +
                "               \"auto_refresh_partitions_limit\"=\"4\"\n" +
                "               )\n" +
                "               AS SELECT k1, sum(v1) as sum_v1 FROM test_partition_tbl1 group by k1;");
        {
            String query = "select k1, sum(v1) FROM test_partition_tbl1 where k1>='2020-02-11' group by k1;";
            String plan = getFragmentPlan(query);
            PlanTestBase.assertContains(plan, "test_partition_tbl_mv1");
            PlanTestBase.assertContains(plan, "PREDICATES: 5: k1 >= '2020-02-11'");
            PlanTestBase.assertContains(plan, "partitions=4/4");
        }
        {
            String query = "select k1, sum(v1) FROM test_partition_tbl1 where k1>='2020-02-01' group by k1;";
            String plan = getFragmentPlan(query);
            PlanTestBase.assertContains(plan, "test_partition_tbl_mv1");
            PlanTestBase.assertContains(plan, "partitions=4/4\n" +
                    "     rollup: test_partition_tbl_mv1");
        }
    }

    @Test
    public void testMapArrayRewrite() throws Exception {
        starRocksAssert.withTable("CREATE TABLE test_map_array (\n" +
                "                            k1 date,\n" +
                "                            k2 varchar(20),\n" +
                "                            v1 map<string, string>,\n" +
                "                            v2 array<int>, \n" +
                "                            v3 int)\n" +
                "                        DUPLICATE KEY(k1)\n" +
                "                        DISTRIBUTED BY HASH(k1);");

        // MV1: MAP MV
        {
            String mvName = "mv_test_map_element";
            createAndRefreshMv("CREATE MATERIALIZED VIEW \n" + mvName +
                    "\nDISTRIBUTED BY HASH(k1) BUCKETS 10\n" +
                    "REFRESH ASYNC \n" +
                    "AS SELECT k1, element_at(v1, 'k1') as col1, sum(v3) as sum_v3 \n" +
                    "FROM test_map_array\n" +
                    "GROUP BY k1, element_at(v1, 'k1') ");

            // query1: exactly-same aggregation
            {
                String query = "SELECT k1, element_at(v1, 'k1'), sum(v3) as sum_v1 " +
                        " FROM test_map_array group by k1, element_at(v1, 'k1')";
                String plan = getFragmentPlan(query);
                PlanTestBase.assertContains(plan, mvName);
            }
            // query2: rollup aggregation
            {
                String query = "SELECT element_at(v1, 'k1'), sum(v3) as sum_v1 " +
                        " FROM test_map_array group by element_at(v1, 'k1')";
                String plan = getFragmentPlan(query);
                PlanTestBase.assertContains(plan, mvName);
            }
            dropMv("test", mvName);
        }

        // MV2: Array Element MV
        {
            String mvName = "mv_test_array_element";
            createAndRefreshMv("CREATE MATERIALIZED VIEW \n" + mvName +
                    "\nDISTRIBUTED BY HASH(k1) BUCKETS 10\n" +
                    "REFRESH ASYNC \n" +
                    "AS SELECT k1, element_at(v2, 1) as col1, sum(v3) as sum_v3 \n" +
                    "FROM test_map_array\n" +
                    "GROUP BY k1, element_at(v2, 1) ");

            // query1: exactly-same aggregation
            {
                String query = "SELECT k1, element_at(v2, 1), sum(v3) as sum_v1 " +
                        " FROM test_map_array group by k1, element_at(v2, 1)";
                String plan = getFragmentPlan(query);
                PlanTestBase.assertContains(plan, mvName);
            }
            // query2: rollup aggregation
            {
                String query = "SELECT element_at(v2, 1), sum(v3) as sum_v1 " +
                        " FROM test_map_array group by element_at(v2, 1)";
                String plan = getFragmentPlan(query);
                PlanTestBase.assertContains(plan, mvName);
            }
            dropMv("test", mvName);
        }
        // MV3: Array Slice MV
        {
            String mvName = "mv_test_array_slice";
            createAndRefreshMv("CREATE MATERIALIZED VIEW \n" + mvName +
                    "\nDISTRIBUTED BY HASH(k1) BUCKETS 10\n" +
                    "REFRESH ASYNC \n" +
                    "AS SELECT k1, array_slice(v2, 1, 1) as col1, sum(v3) as sum_v3 \n" +
                    "FROM test_map_array\n" +
                    "GROUP BY k1, array_slice(v2, 1, 1) ");

            // query1: exactly-same aggregation
            {
                String query = "SELECT k1, array_slice(v2, 1, 1), sum(v3) as sum_v1 " +
                        " FROM test_map_array group by k1, array_slice(v2, 1, 1)";
                String plan = getFragmentPlan(query);
                PlanTestBase.assertContains(plan, mvName);
            }
        }
    }

    @Test
    public void testProjectConstant() throws Exception {
        String tableName = "test_tbl_project_constant";
        starRocksAssert.withTable(String.format("CREATE TABLE %s(\n" +
                "                            k1 date,\n" +
                "                            k2 varchar(20),\n" +
                "                            v3 int)\n" +
                "                        DUPLICATE KEY(k1)\n" +
                "                        DISTRIBUTED BY HASH(k1);", tableName));

        // MV1: Projection MV
        {
            String mvName = "mv_projection_const";
            createAndRefreshMv("CREATE MATERIALIZED VIEW \n" + mvName +
                    "\nDISTRIBUTED BY HASH(k1) BUCKETS 10\n" +
                    "REFRESH ASYNC \n" +
                    "AS SELECT k1, k2, v3 " +
                    "FROM " + tableName);
            {
                String query = "SELECT 'hehe', k1, k2" +
                        " FROM " + tableName;
                String plan = getFragmentPlan(query);
                PlanTestBase.assertContains(plan, mvName);
            }
        }
        // MV2: Aggregation MV
        {
            String mvName = "mv_aggregation_projection_const";
            createAndRefreshMv("CREATE MATERIALIZED VIEW \n" + mvName +
                    "\nDISTRIBUTED BY HASH(k1) BUCKETS 10\n" +
                    "REFRESH ASYNC \n" +
                    "AS SELECT k1, sum(v3) as sum_v3 \n" +
                    "FROM " + tableName + "\n" +
                    "GROUP BY k1");

            {
                String query = String.format("SELECT 'hehe', k1, sum(v3) as sum_v1 " +
                        " FROM %s group by 'hehe', k1", tableName);
                String plan = getFragmentPlan(query);
                PlanTestBase.assertContains(plan, mvName);
            }
        }
    }

    @Test
    public void testMVWithAutomaticPartitionBaseTable() throws Exception {
        starRocksAssert.withTable("\n" +
                "CREATE TABLE test_empty_partition_tbl(\n" +
                "  `dt` datetime DEFAULT NULL,\n" +
                "  `col1` bigint(20) DEFAULT NULL,\n" +
                "  `col2` bigint(20) DEFAULT NULL,\n" +
                "  `col3` bigint(20) DEFAULT NULL,\n" +
                "  `error_code` varchar(1048576) DEFAULT NULL\n" +
                ")\n" +
                "DUPLICATE KEY (dt, col1)\n" +
                "PARTITION BY date_trunc('day', dt)\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ");");
        starRocksAssert.withMaterializedView("CREATE MATERIALIZED VIEW  test_empty_partition_mv1 \n" +
                "DISTRIBUTED BY HASH(col1, dt) BUCKETS 32\n" +
                "--DISTRIBUTED BY RANDOM BUCKETS 32\n" +
                "partition by date_trunc('day', dt)\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ")\n" +
                "AS select\n" +
                "      col1,\n" +
                "        dt,\n" +
                "        sum(col2) AS sum_col2,\n" +
                "        sum(if(error_code = 'TIMEOUT', col3, 0)) AS sum_col3\n" +
                "    FROM\n" +
                "        test_empty_partition_tbl AS f\n" +
                "    GROUP BY\n" +
                "        col1,\n" +
                "        dt;");
        String insertSql = "insert into test_empty_partition_tbl values('2022-08-16', 1, 1, 1, 'a')";
        cluster.runSql("test", insertSql);
        refreshMaterializedView("test", "test_empty_partition_mv1");

        String sql = "select\n" +
                "      col1,\n" +
                "        sum(col2) AS sum_col2,\n" +
                "        sum(if(error_code = 'TIMEOUT', col3, 0)) AS sum_col3\n" +
                "    FROM\n" +
                "        test_empty_partition_tbl AS f\n" +
                "    WHERE (dt >= STR_TO_DATE('2023-08-15 00:00:00', '%Y-%m-%d %H:%i:%s'))\n" +
                "        AND (dt <= STR_TO_DATE('2023-08-15 00:00:00', '%Y-%m-%d %H:%i:%s'))\n" +
                "    GROUP BY col1;";
        String plan = getFragmentPlan(sql);
        PlanTestBase.assertContains(plan, "test_empty_partition_mv1");
    }

    @Test
    public void testJoinWithConstExprs1() throws Exception {
        // a.k1/b.k1 are both output
        createAndRefreshMv("CREATE MATERIALIZED VIEW test_partition_tbl_mv3\n" +
                "PARTITION BY k1\n" +
                "DISTRIBUTED BY HASH(v1) BUCKETS 10\n" +
                "REFRESH ASYNC\n" +
                "AS SELECT a.k1, a.v1,sum(a.v1) as sum_v1 \n" +
                "FROM test_partition_tbl1 as a \n" +
                "join test_partition_tbl2 as b " +
                "on a.k1=b.k1 and a.v1=b.v1\n" +
                "group by a.k1, a.v1;");
        // should not be rollup
        {
            // if a.k1=b.k1
            String query = "select a.k1, a.v1, sum(a.v1) " +
                    "FROM test_partition_tbl1 as a join test_partition_tbl2 as b " +
                    "on a.v1=b.v1 " +
                    "where a.k1 = '2020-01-01' and b.k1 = '2020-01-01' " +
                    "group by a.k1, a.v1;";
            String plan = getFragmentPlan(query);
            PlanTestBase.assertNotContains(plan, "AGGREGATE");
            PlanTestBase.assertContains(plan, "PREDICATES: 8: k1 = '2020-01-01'\n" +
                    "     partitions=1/6\n" +
                    "     rollup: test_partition_tbl_mv3");
        }

        // should not be rollup
        {
            // if a.k1=b.k1
            String query = "select a.k1, a.v1, sum(a.v1) " +
                    "FROM test_partition_tbl1 as a join test_partition_tbl2 as b " +
                    "on a.v1=b.v1 and a.k1=b.k1 " +
                    "where a.k1 = '2020-01-01' and b.k1 = '2020-01-01' " +
                    "group by a.k1, a.v1;";
            String plan = getFragmentPlan(query);
            PlanTestBase.assertNotContains(plan, "AGGREGATE");
            PlanTestBase.assertContains(plan, "PREDICATES: 8: k1 = '2020-01-01'\n" +
                    "     partitions=1/6\n" +
                    "     rollup: test_partition_tbl_mv3");
        }

        // should rollup
        {
            String query = "select a.k1, sum(a.v1) " +
                    "FROM test_partition_tbl1 as a join test_partition_tbl2 as b " +
                    "on a.v1=b.v1 " +
                    "where a.k1='2020-01-01' and b.k1='2020-01-01' and a.v1=1 " +
                    "group by a.k1;";
            String plan = getFragmentPlan(query);
            PlanTestBase.assertNotContains(plan, "AGGREGATE");
            PlanTestBase.assertContains(plan, "PREDICATES: 8: k1 = '2020-01-01', 9: v1 = 1\n" +
                    "     partitions=1/6\n" +
                    "     rollup: test_partition_tbl_mv3");
        }
        starRocksAssert.dropMaterializedView("test_partition_tbl_mv3");
    }

    @Test
    public void testJoinWithConstExprs2() throws Exception {
        // a.k1/b.k1 are both output
        createAndRefreshMv("CREATE MATERIALIZED VIEW test_partition_tbl_mv3\n" +
                "PARTITION BY a_k1\n" +
                "DISTRIBUTED BY HASH(a_v1) BUCKETS 10\n" +
                "REFRESH ASYNC\n" +
                "AS SELECT a.k1 as a_k1, b.k1 as b_k1, " +
                "a.v1 as a_v1, b.v1 as b_v1,sum(a.v1) as sum_v1 \n" +
                "FROM test_partition_tbl1 as a \n" +
                "left join test_partition_tbl2 as b " +
                "on a.k1=b.k1 and a.v1=b.v1\n" +
                "group by a.k1, b.k1, a.v1, b.v1;");
        {
            String query = "SELECT a.k1 as a_k1, b.k1 as b_k1, " +
                    "a.v1 as a_v1, b.v1 as b_v1,sum(a.v1) as sum_v1 \n" +
                    "FROM test_partition_tbl1 as a \n" +
                    "left join test_partition_tbl2 as b " +
                    "on a.k1=b.k1 and a.v1=b.v1 and a.k1=b.k1 and b.v1=a.v1 \n" +
                    "group by a.k1, b.k1, a.v1, b.v1";
            String plan = getFragmentPlan(query);
            PlanTestBase.assertContains(plan, "partitions=6/6\n" +
                    "     rollup: test_partition_tbl_mv3");
        }

        // should not be rollup
        {
            String query = "select a.k1, b.k1, a.v1, b.v1, sum(a.v1) " +
                    "FROM test_partition_tbl1 as a left join test_partition_tbl2 as b " +
                    "on a.v1=b.v1 " +
                    "where a.k1='2020-01-01' and b.k1 = '2020-01-01' " +
                    "group by a.k1, b.k1, a.v1,b.v1;";
            String plan = getFragmentPlan(query);
            PlanTestBase.assertNotContains(plan, "AGGREGATE");
            PlanTestBase.assertContains(plan, "PREDICATES: 8: a_k1 = '2020-01-01', 9: b_k1 = '2020-01-01'\n" +
                    "     partitions=1/6\n" +
                    "     rollup: test_partition_tbl_mv3");
        }

        // should be rollup
        {
            String query = "select a.k1,a.v1, sum(a.v1) " +
                    "FROM test_partition_tbl1 as a left join test_partition_tbl2 as b " +
                    "on a.v1=b.v1 " +
                    "where a.k1='2020-01-01' and b.k1 = '2020-01-01' " +
                    "group by a.k1, a.v1 ;";
            String plan = getFragmentPlan(query);
            PlanTestBase.assertNotContains(plan, "AGGREGATE");
            PlanTestBase.assertContains(plan, "PREDICATES: 8: a_k1 = '2020-01-01', 9: b_k1 = '2020-01-01'\n" +
                    "     partitions=1/6\n" +
                    "     rollup: test_partition_tbl_mv3");
        }

        // should be rollup
        {
            String query = "select a.k1,a.v1, sum(a.v1) " +
                    "FROM test_partition_tbl1 as a left join test_partition_tbl2 as b " +
                    "on a.v1=b.v1 and a.k1=b.k1 " +
                    "where a.k1='2020-01-01' and b.k1 = '2020-01-01' " +
                    "group by a.k1, a.v1 ;";
            String plan = getFragmentPlan(query);
            PlanTestBase.assertNotContains(plan, "AGGREGATE");
            PlanTestBase.assertContains(plan, "PREDICATES: 8: a_k1 = '2020-01-01', " +
                    "9: b_k1 IS NOT NULL\n" +
                    "     partitions=1/6\n" +
                    "     rollup: test_partition_tbl_mv3");
        }

        // should be rollup
        {
            String query = "select a.k1,a.v1, sum(a.v1) " +
                    "FROM test_partition_tbl1 as a left join test_partition_tbl2 as b " +
                    "on a.v1=b.v1 and a.v1=b.v1 and a.v1=b.v1 " +
                    "where a.k1='2020-01-01' and b.k1 = '2020-01-01' " +
                    "group by a.k1, a.v1 ;";
            String plan = getFragmentPlan(query);
            PlanTestBase.assertNotContains(plan, "AGGREGATE");
            PlanTestBase.assertContains(plan, "PREDICATES: 8: a_k1 = '2020-01-01', 9: b_k1 = '2020-01-01'\n" +
                    "     partitions=1/6\n" +
                    "     rollup: test_partition_tbl_mv3");
        }
        starRocksAssert.dropMaterializedView("test_partition_tbl_mv3");
    }

    @Test
    public void testJoinWithConstExprs3() throws Exception {
        // a.k1/b.k1 are both output
        createAndRefreshMv("CREATE MATERIALIZED VIEW test_partition_tbl_mv3\n" +
                "PARTITION BY a_k1\n" +
                "DISTRIBUTED BY HASH(a_v1) BUCKETS 10\n" +
                "REFRESH ASYNC\n" +
                "AS SELECT a.k1 as a_k1, a.v1 as a_v1, a.v2 as a_v2, " +
                "b.k1 as b_k1, b.v1 as b_v1, b.v2 as b_v2 \n" +
                "FROM test_partition_tbl1 as a \n" +
                "join test_partition_tbl2 as b " +
                "on a.v1=b.v1 and a.v2=b.v2 \n");
        // should not be rollup
        {
            // if a.k1=b.k1
            String query = "select a.k1, a.v1 " +
                    "FROM test_partition_tbl1 as a join test_partition_tbl2 as b " +
                    "on a.v1=b.v1 " +
                    "where a.v1=1 and b.v2=1 ;";
            String plan = getFragmentPlan(query);
            PlanTestBase.assertNotContains(plan, "test_partition_tbl_mv3");
        }

        // should not be rollup
        {
            // if a.k1=b.k1
            String query = "select a.k1, a.v1 " +
                    "FROM test_partition_tbl1 as a join test_partition_tbl2 as b " +
                    "on a.v1=b.v1 and a.v2=b.v2 " +
                    "where a.v1=1 and b.v2=1 ;";
            String plan = getFragmentPlan(query);
            PlanTestBase.assertContains(plan, "PREDICATES: 8: a_v1 = 1, 9: a_v2 = 1\n" +
                    "     partitions=6/6\n" +
                    "     rollup: test_partition_tbl_mv3");
        }
        starRocksAssert.dropMaterializedView("test_partition_tbl_mv3");
    }

    @Test
    public void testJoinWithConstExprs4() throws Exception {
        // a.k1/b.k1 are both output
        createAndRefreshMv("CREATE MATERIALIZED VIEW test_partition_tbl_mv3\n" +
                "PARTITION BY a_k1\n" +
                "DISTRIBUTED BY HASH(a_v1) BUCKETS 10\n" +
                "REFRESH ASYNC\n" +
                "AS SELECT a.k1 as a_k1, a.v1 as a_v1, a.v2 as a_v2, " +
                "b.k1 as b_k1, b.v1 as b_v1, b.v2 as b_v2 \n" +
                "FROM test_partition_tbl1 as a \n" +
                "join test_partition_tbl2 as b " +
                "on a.v1=b.v1 \n");
        // should not be rollup
        {
            // if a.k1=b.k1
            String query = "select a.k1, a.v1 " +
                    "FROM test_partition_tbl1 as a join test_partition_tbl2 as b " +
                    "on a.v1=b.v1 " +
                    "where a.v1=1 and b.v2=1 ;";
            String plan = getFragmentPlan(query);
            System.out.println(plan);
            PlanTestBase.assertContains(plan, " TABLE: test_partition_tbl_mv3\n" +
                    "     PREAGGREGATION: ON\n" +
                    "     PREDICATES: 8: a_v1 = 1, 12: b_v2 = 1\n" +
                    "     partitions=6/6");
        }

        starRocksAssert.dropMaterializedView("test_partition_tbl_mv3");
    }

    @Test
    public void testJoinWithConstExprs5() throws Exception {
        // a.k1/b.k1 are both output
        createAndRefreshMv("CREATE MATERIALIZED VIEW test_partition_tbl_mv3\n" +
                "PARTITION BY a_k1\n" +
                "DISTRIBUTED BY HASH(a_v1) BUCKETS 10\n" +
                "REFRESH ASYNC\n" +
                "AS SELECT a.k1 as a_k1, a.v1 as a_v1, a.v2 as a_v2, " +
                "b.k1 as b_k1, b.v1 as b_v1, b.v2 as b_v2 \n" +
                "FROM test_partition_tbl1 as a \n" +
                "join test_partition_tbl2 as b " +
                "on a.v1=b.v1 and a.v1=b.v2 \n");
        // should not be rollup
        {
            // if a.k1=b.k1
            String query = "select a.k1, a.v1 " +
                    "FROM test_partition_tbl1 as a join test_partition_tbl2 as b " +
                    "on a.v1=b.v1 " +
                    "where a.v1=1 and b.v2=1 ;";
            String plan = getFragmentPlan(query);
            System.out.println(plan);
            PlanTestBase.assertNotContains(plan, "PREDICATES: 8: a_v1 = 1, 11: b_v1 = 1, 12: b_v2 = 1\n" +
                    "     partitions=6/6\n" +
                    "     rollup: test_partition_tbl_mv3");
        }

        starRocksAssert.dropMaterializedView("test_partition_tbl_mv3");
    }

    private List<MvPlanContext> getPlanContext(MaterializedView mv, boolean useCache) {
        boolean prev = connectContext.getSessionVariable().isEnableMaterializedViewPlanCache();
        connectContext.getSessionVariable().setEnableMaterializedViewPlanCache(useCache);
        List<MvPlanContext> result = CachingMvPlanContextBuilder.getInstance().getPlanContext(
                connectContext.getSessionVariable(), mv);
        connectContext.getSessionVariable().setEnableMaterializedViewPlanCache(prev);
        return result;
    }

    @Test
    public void testPlanCache1() throws Exception {
        CachingMvPlanContextBuilder instance = CachingMvPlanContextBuilder.getInstance();
        String mvSql = "create materialized view agg_join_mv_1" +
                " distributed by hash(v1) as SELECT t0.v1 as v1," +
                " test_all_type.t1d, sum(test_all_type.t1c) as total_sum, count(test_all_type.t1c) as total_num" +
                " from t0 join test_all_type on t0.v1 = test_all_type.t1d" +
                " where t0.v1 < 100" +
                " group by v1, test_all_type.t1d";
        starRocksAssert.withMaterializedView(mvSql);

        MaterializedView mv = getMv("test", "agg_join_mv_1");
        instance.evictMaterializedViewCache(mv);

        List<MvPlanContext> planContexts = getPlanContext(mv, false);
        Assertions.assertNotNull(planContexts);
        Assertions.assertTrue(planContexts.size() == 1);
        Assertions.assertFalse(CachingMvPlanContextBuilder.getInstance().contains(mv));
        planContexts = getPlanContext(mv, true);
        Assertions.assertNotNull(planContexts);
        Assertions.assertNotNull(planContexts.size() == 1);
        Assertions.assertTrue(CachingMvPlanContextBuilder.getInstance().contains(mv));
        planContexts = getPlanContext(mv, false);
        Assertions.assertNotNull(planContexts);
        Assertions.assertNotNull(planContexts.size() == 1);
        starRocksAssert.dropMaterializedView("agg_join_mv_1");
    }

    @Test
    public void testPlanCache2() throws Exception {
        String mvSql = "create materialized view mv_with_window" +
                " distributed by hash(t1d) as" +
                " SELECT test_all_type.t1d, row_number() over (partition by t1c)" +
                " from test_all_type";
        starRocksAssert.withMaterializedView(mvSql);

        MaterializedView mv = getMv("test", "mv_with_window");
        List<MvPlanContext> planContexts = getPlanContext(mv, true);
        Assertions.assertNotNull(planContexts);
        Assertions.assertNotNull(planContexts.size() == 1);
        Assertions.assertTrue(CachingMvPlanContextBuilder.getInstance().contains(mv));
        starRocksAssert.dropMaterializedView("mv_with_window");
    }

    @Test
    public void testPlanCache3() throws Exception {
        long testSize = Config.mv_plan_cache_max_size + 1;
        for (int i = 0; i < testSize; i++) {
            String mvName = "plan_cache_mv_" + i;
            String mvSql = String.format("create materialized view %s" +
                    " distributed by hash(v1) as SELECT t0.v1 as v1," +
                    " test_all_type.t1d, sum(test_all_type.t1c) as total_sum, count(test_all_type.t1c) as total_num" +
                    " from t0 join test_all_type on t0.v1 = test_all_type.t1d" +
                    " where t0.v1 < 100" +
                    " group by v1, test_all_type.t1d", mvName);
            starRocksAssert.withMaterializedView(mvSql);

            MaterializedView mv = getMv("test", mvName);
            List<MvPlanContext> planContexts = getPlanContext(mv, true);
            Assertions.assertNotNull(planContexts);
            Assertions.assertNotNull(planContexts.size() == 1);
        }
        for (int i = 0; i < testSize; i++) {
            String mvName = "plan_cache_mv_" + i;
            starRocksAssert.dropMaterializedView(mvName);
        }
    }

    @Test
    public void testPlanCacheWithException() throws Exception {
        CachingMvPlanContextBuilder instance = CachingMvPlanContextBuilder.getInstance();
        // Planner exception
        String mvSql = "create materialized view mv_with_window" +
                " refresh deferred async " +
                " as SELECT test_all_type.t1d, row_number() over (partition by t1c) from test_all_type";
        starRocksAssert.withMaterializedView(mvSql);

        MaterializedView mv = getMv("test", "mv_with_window");
        instance.evictMaterializedViewCache(mv);
        
        new MockUp<QueryOptimizer>() {
            @Mock
            public OptExpression optimize(OptExpression logicOperatorTree,
                                          PhysicalPropertySet requiredProperty,
                                          ColumnRefSet requiredColumns) {
                throw new RuntimeException("optimize failed");
            }
        };
        // build cache
        List<MvPlanContext> planContexts = getPlanContext(mv, true);
        Assertions.assertEquals(Lists.newArrayList(), planContexts);
    }

    @Test
    public void testWithSqlSelectLimit() throws Exception {
        starRocksAssert.getCtx().getSessionVariable().setSqlSelectLimit(1000);
        createAndRefreshMv("CREATE MATERIALIZED VIEW mv_with_select_limit " +
                " distributed by hash(empid) " +
                "AS " +
                "SELECT /*+set_var(sql_select_limit=1000)*/ empid, sum(salary) as total " +
                "FROM emps " +
                "GROUP BY empid");
        starRocksAssert.query("SELECT empid, sum(salary) as total " +
                "FROM emps " +
                "GROUP BY empid").explainContains("mv_with_select_limit");
        starRocksAssert.getCtx().getSessionVariable().setSqlSelectLimit(SessionVariable.DEFAULT_SELECT_LIMIT);
    }

    @Test
    public void testQueryIncludingExcludingMVNames() throws Exception {
        starRocksAssert.getCtx().getSessionVariable().setOptimizerExecuteTimeout(3000000);
        createAndRefreshMv("CREATE MATERIALIZED VIEW mv_agg_1 " +
                " distributed by hash(empid) " +
                "AS " +
                "SELECT empid, sum(salary) as total " +
                "FROM emps " +
                "GROUP BY empid");
        createAndRefreshMv("CREATE MATERIALIZED VIEW mv_agg_2 " +
                " distributed by hash(empid) " +
                "AS " +
                "SELECT empid, sum(salary) as total " +
                "FROM emps " +
                "GROUP BY empid");
        {
            starRocksAssert.getCtx().getSessionVariable().setQueryIncludingMVNames("mv_agg_1");
            String query = "SELECT empid, sum(salary) as total " +
                    "FROM emps " +
                    "GROUP BY empid";
            String plan = getFragmentPlan(query);
            PlanTestBase.assertContains(plan, "mv_agg_1");
            starRocksAssert.getCtx().getSessionVariable().setQueryIncludingMVNames("");
        }
        {
            starRocksAssert.getCtx().getSessionVariable().setQueryIncludingMVNames("mv_agg_2");
            String query = "SELECT empid, sum(salary) as total " +
                    "FROM emps " +
                    "GROUP BY empid";
            String plan = getFragmentPlan(query);
            PlanTestBase.assertContains(plan, "mv_agg_2");
            starRocksAssert.getCtx().getSessionVariable().setQueryIncludingMVNames("");
        }
        {
            starRocksAssert.getCtx().getSessionVariable().setQueryIncludingMVNames("mv_agg_1, mv_agg_2");
            String query = "SELECT empid, sum(salary) as total " +
                    "FROM emps " +
                    "GROUP BY empid";
            String plan = getFragmentPlan(query);
            PlanTestBase.assertContains(plan, "mv_agg_");
            starRocksAssert.getCtx().getSessionVariable().setQueryIncludingMVNames("");
        }
        {
            starRocksAssert.getCtx().getSessionVariable().setQueryExcludingMVNames("mv_agg_1");
            String query = "SELECT empid, sum(salary) as total " +
                    "FROM emps " +
                    "GROUP BY empid";
            String plan = getFragmentPlan(query);
            PlanTestBase.assertContains(plan, "mv_agg_2");
            starRocksAssert.getCtx().getSessionVariable().setQueryExcludingMVNames("");
        }
        {
            starRocksAssert.getCtx().getSessionVariable().setQueryExcludingMVNames("mv_agg_2");
            String query = "SELECT empid, sum(salary) as total " +
                    "FROM emps " +
                    "GROUP BY empid";
            String plan = getFragmentPlan(query);
            PlanTestBase.assertContains(plan, "mv_agg_1");
            starRocksAssert.getCtx().getSessionVariable().setQueryExcludingMVNames("");
        }
        {
            starRocksAssert.getCtx().getSessionVariable().setQueryExcludingMVNames("mv_agg_1, mv_agg_2");
            String query = "SELECT empid, sum(salary) as total " +
                    "FROM emps " +
                    "GROUP BY empid";
            String plan = getFragmentPlan(query);
            PlanTestBase.assertNotContains(plan, "mv_agg_");
            starRocksAssert.getCtx().getSessionVariable().setQueryExcludingMVNames("");
        }
        starRocksAssert.dropMaterializedView("mv_agg_1");
        starRocksAssert.dropMaterializedView("mv_agg_2");
    }

    @Test
    public void testIsDateRange() throws AnalysisException {
        {
            LocalDate date1 = LocalDate.now();
            PartitionKey upper = PartitionKey.ofDate(date1);
            Range<PartitionKey> upRange = Range.atMost(upper);
            Assertions.assertTrue(MvUtils.isDateRange(upRange));
        }
        {
            LocalDate date1 = LocalDate.now();
            PartitionKey low = PartitionKey.ofDate(date1);
            Range<PartitionKey> lowRange = Range.atLeast(low);
            Assertions.assertTrue(MvUtils.isDateRange(lowRange));
        }
        {
            LocalDate date1 = LocalDate.of(2023, 10, 1);
            PartitionKey upper = PartitionKey.ofDate(date1);
            Range<PartitionKey> upRange = Range.atMost(upper);
            LocalDate date2 = LocalDate.of(2023, 9, 1);
            PartitionKey low = PartitionKey.ofDate(date2);
            Range<PartitionKey> lowRange = Range.atLeast(low);
            Range<PartitionKey> range = upRange.intersection(lowRange);
            Assertions.assertTrue(MvUtils.isDateRange(range));
        }
        {
            Range<PartitionKey> unboundRange = Range.all();
            Assertions.assertFalse(MvUtils.isDateRange(unboundRange));
        }
    }

    @Test
    public void testConvertToVarcharRange() throws AnalysisException {
        {
            LocalDate date1 = LocalDate.of(2023, 10, 10);
            PartitionKey upper = PartitionKey.ofDate(date1);
            Range<PartitionKey> upRange = Range.atMost(upper);
            Range<PartitionKey> range = MvUtils.convertToVarcharRange(upRange, "%Y-%m-%d");
            Assertions.assertTrue(range.hasUpperBound());
            Assertions.assertTrue(range.upperEndpoint().getTypes().get(0).isStringType());
            Assertions.assertEquals("2023-10-10", range.upperEndpoint().getKeys().get(0).getStringValue());
        }
        {
            LocalDate date1 = LocalDate.of(2023, 10, 10);
            PartitionKey low = PartitionKey.ofDate(date1);
            Range<PartitionKey> lowRange = Range.atLeast(low);
            Range<PartitionKey> range = MvUtils.convertToVarcharRange(lowRange, "%Y-%m-%d");
            Assertions.assertTrue(range.hasLowerBound());
            Assertions.assertTrue(range.lowerEndpoint().getTypes().get(0).isStringType());
            Assertions.assertEquals("2023-10-10", range.lowerEndpoint().getKeys().get(0).getStringValue());
        }
        {
            LocalDate date1 = LocalDate.of(2023, 10, 10);
            PartitionKey upper = PartitionKey.ofDate(date1);
            Range<PartitionKey> upRange = Range.atMost(upper);
            LocalDate date2 = LocalDate.of(2023, 9, 10);
            PartitionKey low = PartitionKey.ofDate(date2);
            Range<PartitionKey> lowRange = Range.atLeast(low);
            Range<PartitionKey> dateRange = upRange.intersection(lowRange);
            Range<PartitionKey> range = MvUtils.convertToVarcharRange(dateRange, "%Y-%m-%d");
            Assertions.assertTrue(range.hasUpperBound());
            Assertions.assertTrue(range.upperEndpoint().getTypes().get(0).isStringType());
            Assertions.assertEquals("2023-10-10", range.upperEndpoint().getKeys().get(0).getStringValue());
            Assertions.assertTrue(range.hasLowerBound());
            Assertions.assertTrue(range.lowerEndpoint().getTypes().get(0).isStringType());
            Assertions.assertEquals("2023-09-10", range.lowerEndpoint().getKeys().get(0).getStringValue());
        }

        {
            LocalDate date1 = LocalDate.of(2023, 10, 10);
            PartitionKey upper = PartitionKey.ofDate(date1);
            Range<PartitionKey> upRange = Range.atMost(upper);
            Range<PartitionKey> range = MvUtils.convertToVarcharRange(upRange, "%Y%m%d");
            Assertions.assertTrue(range.hasUpperBound());
            Assertions.assertTrue(range.upperEndpoint().getTypes().get(0).isStringType());
            Assertions.assertEquals("20231010", range.upperEndpoint().getKeys().get(0).getStringValue());
        }
        {
            LocalDate date1 = LocalDate.of(2023, 10, 10);
            PartitionKey low = PartitionKey.ofDate(date1);
            Range<PartitionKey> lowRange = Range.atLeast(low);
            Range<PartitionKey> range = MvUtils.convertToVarcharRange(lowRange, "%Y%m%d");
            Assertions.assertTrue(range.hasLowerBound());
            Assertions.assertTrue(range.lowerEndpoint().getTypes().get(0).isStringType());
            Assertions.assertEquals("20231010", range.lowerEndpoint().getKeys().get(0).getStringValue());
        }
        {
            LocalDate date1 = LocalDate.of(2023, 10, 10);
            PartitionKey upper = PartitionKey.ofDate(date1);
            Range<PartitionKey> upRange = Range.atMost(upper);
            LocalDate date2 = LocalDate.of(2023, 9, 10);
            PartitionKey low = PartitionKey.ofDate(date2);
            Range<PartitionKey> lowRange = Range.atLeast(low);
            Range<PartitionKey> dateRange = upRange.intersection(lowRange);
            Range<PartitionKey> range = MvUtils.convertToVarcharRange(dateRange, "%Y%m%d");
            Assertions.assertTrue(range.hasUpperBound());
            Assertions.assertTrue(range.upperEndpoint().getTypes().get(0).isStringType());
            Assertions.assertEquals("20231010", range.upperEndpoint().getKeys().get(0).getStringValue());
            Assertions.assertTrue(range.hasLowerBound());
            Assertions.assertTrue(range.lowerEndpoint().getTypes().get(0).isStringType());
            Assertions.assertEquals("20230910", range.lowerEndpoint().getKeys().get(0).getStringValue());
        }
    }

    @Test
    public void testInsertMV() throws Exception {
        String mvName = "mv_insert";
        createAndRefreshMv("create materialized view " + mvName +
                " distributed by hash(v1) " +
                "refresh async as " +
                "select * from t0");
        String sql = "insert into t0 select * from t0";

        // enable
        {
            starRocksAssert.getCtx().getSessionVariable().setEnableMaterializedViewRewriteForInsert(true);
            starRocksAssert.query(sql).explainContains(mvName);
        }

        // disable
        {
            starRocksAssert.getCtx().getSessionVariable().setEnableMaterializedViewRewriteForInsert(false);
            starRocksAssert.query(sql).explainWithout(mvName);
        }

        starRocksAssert.getCtx().getSessionVariable().setEnableMaterializedViewRewriteForInsert(
                SessionVariable.DEFAULT_SESSION_VARIABLE.isEnableMaterializedViewRewriteForInsert());
        starRocksAssert.dropMaterializedView("mv_insert");
    }

    /**
     * With many Agg MV candidates, the rewrite should prefer the one with fewer data rows
     */
    @Test
    public void testCandidateOrdering_HierarchyAgg() throws Exception {
        starRocksAssert.withTable(cluster, MSchema.T_METRICS.getTableName());

        List<String> dimensions = Lists.newArrayList(
                " c2",
                " c2, c3",
                " c2, c3, c4",
                " c2, c3, c4, c5",
                " c2, c3, c4, c5, c6"
        );

        Function<Integer, String> mvNameBuilder = (i) -> ("mv_agg_metric_" + i);
        for (int i = 0; i < dimensions.size(); i++) {
            String name = mvNameBuilder.apply(i);
            starRocksAssert.withRefreshedMaterializedView("create materialized view " + name +
                    " refresh async as " +
                    " select sum(c1) from t_metrics group by " +
                    dimensions.get(i));
            MaterializedView mv = starRocksAssert.getMv("test", name);

            int mockRows = i + 1;
            mv.getPartitions().forEach(p -> p.getDefaultPhysicalPartition().getBaseIndex().setRowCount(mockRows));
        }

        for (int i = 0; i < dimensions.size(); i++) {
            String query = "select sum(c1) from t_metrics group by " + dimensions.get(i);
            String target = mvNameBuilder.apply(i);

            // With candidate limit
            starRocksAssert.getCtx().getSessionVariable().setCboMaterializedViewRewriteCandidateLimit(10);
            starRocksAssert.query(query).explainContains(target);

            // Without candidate limit
            starRocksAssert.getCtx().getSessionVariable().setCboMaterializedViewRewriteCandidateLimit(0);
            starRocksAssert.query(query).explainContains(target);
        }
    }

    /**
     * Many dimensions on a table, create many MVs on it
     */
    @Test
    public void testCandidateOrdering_ManyDimensions() throws Exception {
        final int numDimensions = connectContext.getSessionVariable().getCboMaterializedViewRewriteRelatedMVsLimit();
        StringBuilder createTableBuilder = new StringBuilder("create table t_many_dimensions ( ");
        Function<Integer, String> columnNameGen = (i) -> "c" + i;
        for (int i = 0; i < numDimensions; i++) {
            if (i != 0) {
                createTableBuilder.append("\n,");
            }
            createTableBuilder.append(columnNameGen.apply(i)).append(" int");
        }
        createTableBuilder.append(") distributed by hash(c0) ");
        starRocksAssert.withTable(createTableBuilder.toString());

        Function<Integer, String> mvNameGen = (i) -> "mv_dimension_" + i;
        for (int i = 1; i < numDimensions; i++) {
            String dimension = columnNameGen.apply(i);
            String mvName = mvNameGen.apply(i);
            starRocksAssert.withMaterializedView("create materialized view " + mvName + "\n" +
                    "refresh async " +
                    "properties('query_rewrite_consistency'='nocheck') " +
                    "as select " + dimension + ", sum(c0) from t_many_dimensions group by " + dimension);
        }

        for (int i = 1; i < numDimensions; i++) {
            String target = mvNameGen.apply(i);
            String dimension = columnNameGen.apply(i);

            // 1 candidate
            starRocksAssert.getCtx().getSessionVariable().setCboMaterializedViewRewriteCandidateLimit(1);
            starRocksAssert.query("select " + dimension + ", sum(c0) from t_many_dimensions group by " + dimension)
                    .explainContains(target);
            starRocksAssert.query("select sum(c0) from t_many_dimensions where " + dimension + "=123")
                    .explainContains(target);

            // all candidates
            starRocksAssert.getCtx().getSessionVariable().setCboMaterializedViewRewriteCandidateLimit(0);
            starRocksAssert.query("select " + dimension + ", sum(c0) from t_many_dimensions group by " + dimension)
                    .explainContains(target);
            starRocksAssert.query("select sum(c0) from t_many_dimensions where " + dimension + "=123")
                    .explainContains(target);
        }
    }

    @Test
    public void testOuterJoinRewrite() throws Exception {
        starRocksAssert.withMaterializedView(
                "CREATE MATERIALIZED VIEW `mv1` (`event_id`, `event_time`, `event_time1`)\n" +
                        "PARTITION BY (`event_time`)\n" +
                        "DISTRIBUTED BY HASH(`event_id`) BUCKETS 1\n" +
                        "REFRESH ASYNC\n" +
                        "PROPERTIES (\n" +
                        "\"replicated_storage\" = \"true\",\n" +
                        "\"partition_refresh_number\" = \"2\",\n" +
                        "\"force_external_table_query_rewrite\" = \"CHECKED\",\n" +
                        "\"replication_num\" = \"1\",\n" +
                        "\"storage_medium\" = \"HDD\"\n" +
                        ")\n" +
                        "AS SELECT `a`.`event_id`, `a`.`event_time`, `b`.`event_time1`\n" +
                        "FROM `test`.`test10` AS `a` LEFT OUTER JOIN `test`.`test11` AS `b`" +
                        " ON (`a`.`event_id` = `b`.`event_id1`) AND (`a`.`event_time` = `b`.`event_time1`);");

        connectContext.executeSql("refresh materialized view mv1 with sync mode");
        {
            String query = "select * from (select event_id, event_time, event_time1" +
                    " from test10 a left outer join test11 b" +
                    " on a.event_id = b.event_id1 and a.event_time = b.event_time1 ) xx" +
                    " where xx.event_time >= \"2023-01-05 00:00:00\" and xx.event_time < \"2023-01-06 00:00:00\";";
            String plan = getFragmentPlan(query);
            PlanTestBase.assertContains(plan, "mv1");
            PlanTestBase.assertNotContains(plan, "event_time1 >= '2023-01-05 00:00:00'");
        }
    }

    @Test
    public void test() throws Exception {
        connectContext.executeSql("drop table if exists t11");
        starRocksAssert.withTable("create table t11(\n" +
                "shop_id int,\n" +
                "region int,\n" +
                "shop_type string,\n" +
                "shop_flag string,\n" +
                "store_id String,\n" +
                "store_qty Double\n" +
                ") DUPLICATE key(shop_id) distributed by hash(shop_id) buckets 1 " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ");");
        cluster.runSql("test", "insert into\n" +
                "t11\n" +
                "values\n" +
                "(1, 1, 's', 'o', '1', null),\n" +
                "(1, 1, 'm', 'o', '2', 2),\n" +
                "(1, 1, 'b', 'c', '3', 1);");
        connectContext.executeSql("drop materialized view if exists mv11");
        starRocksAssert.withMaterializedView("create MATERIALIZED VIEW mv11 (region, ct) " +
                "DISTRIBUTED BY RANDOM buckets 1 REFRESH MANUAL as\n" +
                "select region,\n" +
                "count(\n" +
                "distinct (\n" +
                "case\n" +
                "when store_qty > 0 then store_id\n" +
                "else null\n" +
                "end\n" +
                ")\n" +
                ")\n" +
                "from t11\n" +
                "group by region;");
        cluster.runSql("test", "refresh materialized view mv11 with sync mode");
        {
            String query = "select region,\n" +
                    "count(\n" +
                    "distinct (\n" +
                    "case\n" +
                    "when store_qty > 0.0 then store_id\n" +
                    "else null\n" +
                    "end\n" +
                    ")\n" +
                    ") as ct\n" +
                    "from t11\n" +
                    "group by region\n" +
                    "having\n" +
                    "count(\n" +
                    "distinct (\n" +
                    "case\n" +
                    "when store_qty > 0.0 then store_id\n" +
                    "else null\n" +
                    "end\n" +
                    ")\n" +
                    ") > 0\n";
            String plan = getFragmentPlan(query);
            PlanTestBase.assertContains(plan, "mv11", "PREDICATES: 10: ct > 0");
        }
    }

    @Test
    public void testMvRewriteWithSortKey() throws Exception {
        connectContext.getSessionVariable().setOptimizerExecuteTimeout(3000000);
        {
            starRocksAssert.withMaterializedView("create MATERIALIZED VIEW if not exists mv_order_by_v1 " +
                    "DISTRIBUTED BY RANDOM buckets 1 " +
                    "order by (v1) " +
                    "REFRESH MANUAL " +
                    "as\n" +
                    "select v1, v2, sum(v3) from t0 group by v1, v2");
            cluster.runSql("test", "refresh materialized view mv_order_by_v1 with sync mode");
            starRocksAssert.withMaterializedView("create MATERIALIZED VIEW if not exists mv_order_by_v2 " +
                    "DISTRIBUTED BY RANDOM buckets 1 " +
                    "order by (v2) " +
                    "REFRESH MANUAL " +
                    "as\n" +
                    "select v1, v2, sum(v3) from t0 group by v1, v2");
            cluster.runSql("test", "refresh materialized view mv_order_by_v2 with sync mode");
            {
                // in predicate
                String query = "select v1, v2, sum(v3) from t0 where v1 in (1, 2, 3) group by v1, v2;";
                String plan = getFragmentPlan(query);
                PlanTestBase.assertContains(plan, "mv_order_by_v1");
            }
            {
                // equal predicate
                String query = "select v1, v2, sum(v3) from t0 where v1 = 1 group by v1, v2;";
                String plan = getFragmentPlan(query);
                PlanTestBase.assertContains(plan, "mv_order_by_v1");
            }
            starRocksAssert.dropMaterializedView("mv_order_by_v1");
            starRocksAssert.dropMaterializedView("mv_order_by_v2");
        }
        {
            starRocksAssert.withMaterializedView("create MATERIALIZED VIEW if not exists mv_order_by_v1 " +
                    "DISTRIBUTED BY RANDOM buckets 1 " +
                    "order by (v1) " +
                    "REFRESH MANUAL " +
                    "as\n" +
                    "select v1, v2, v3 from t0");
            cluster.runSql("test", "refresh materialized view mv_order_by_v1 with sync mode");
            starRocksAssert.withMaterializedView("create MATERIALIZED VIEW if not exists mv_order_by_v2 " +
                    "DISTRIBUTED BY RANDOM buckets 1 " +
                    "order by (v2) " +
                    "REFRESH MANUAL " +
                    "as\n" +
                    "select v1, v2, v3 from t0");
            cluster.runSql("test", "refresh materialized view mv_order_by_v2 with sync mode");
            {
                String query = "select v1, v2, v3 from t0 where v1 in (1, 2, 3);";
                String plan = getFragmentPlan(query);
                PlanTestBase.assertContains(plan, "mv_order_by_v1");
            }
            {
                String query = "select v1, v2, v3 from t0 where v1 = 1;";
                String plan = getFragmentPlan(query);
                PlanTestBase.assertContains(plan, "mv_order_by_v1");
            }
            starRocksAssert.dropMaterializedView("mv_order_by_v1");
            starRocksAssert.dropMaterializedView("mv_order_by_v2");
        }
    }

    @Test
    public void testRefreshMVWithListPartition1() throws Exception {
        starRocksAssert.withTable("CREATE TABLE s1 (\n" +
                "   dt varchar(30),\n" +
                "   id int\n" +
                ")\n" +
                "PARTITION BY RANGE(str2date(dt, \"%Y-%m-%d\"))(\n" +
                "   START (\"2021-01-01\") END (\"2021-01-10\") EVERY (INTERVAL 1 DAY)\n" +
                ");");
        executeInsertSql(connectContext, "insert into s1 values(\"2021-01-01\",1),(\"2021-01-02\",2)," +
                "(\"2021-01-03\",3), (\"2021-01-04\",4),(\"2021-01-05\",5),\n" +
                "(\"2021-01-06\",6),(\"2021-01-07\",7),(\"2021-01-08\",8),(\"2021-01-09\",9),(\"2021-01-09\",10);");
        try {
            starRocksAssert.withMaterializedView("create materialized view mv1 partition by dt " +
                    "refresh manual as select dt,id from s1;\n");
            Assertions.fail();
        } catch (Exception e) {
            Assertions.assertTrue(
                    e.getMessage().contains("Materialized view is partitioned by string type column dt but ref base " +
                    "table s1 is range partitioned"));
        }
        starRocksAssert.dropMaterializedView("mv1");
        starRocksAssert.dropTable("s1");
    }

    @Test
    public void testRefreshMVWithListPartition2() throws Exception {
        starRocksAssert.withTable("CREATE TABLE s1 (\n" +
                "   dt varchar(30),\n" +
                "   id int\n" +
                ")\n" +
                "PARTITION BY RANGE(str2date(dt, \"%Y-%m-%d\"))(\n" +
                "   START (\"2021-01-01\") END (\"2021-01-10\") EVERY (INTERVAL 1 DAY)\n" +
                ");");
        executeInsertSql(connectContext, "insert into s1 values(\"2021-01-01\",1),(\"2021-01-02\",2)," +
                "(\"2021-01-03\",3), (\"2021-01-04\",4),(\"2021-01-05\",5),\n" +
                "(\"2021-01-06\",6),(\"2021-01-07\",7),(\"2021-01-08\",8),(\"2021-01-09\",9),(\"2021-01-09\",10);");
        starRocksAssert.withMaterializedView("create materialized view mv1 " +
                "PARTITION BY str2date(dt, \"%Y-%m-%d\")\n" +
                "refresh manual as select dt,id from s1;\n");
        refreshMaterializedView("test", "mv1");
        MaterializedView mv = getMv("test", "mv1");
        Assertions.assertTrue(mv.getPartitionInfo().isRangePartition());
        String plan = getFragmentPlan("select * from s1");
        PlanTestBase.assertContains(plan, "mv1");
        starRocksAssert.dropMaterializedView("mv1");
        starRocksAssert.dropTable("s1");
    }

    private void checkMVRewritePlanWithUniqueColumnRefs(String mvQuery,
                                                        List<String> queries) throws Exception {
        createAndRefreshMv(mvQuery);
        for (String query : queries) {
            final OptExpression plan = getOptimizedPlan(query);
            final List<LogicalScanOperator> scanOperators = MvUtils.getScanOperator(plan);
            final Set<ColumnRefOperator> mvColumnRefSet = new HashSet<>();
            for (LogicalScanOperator scanOperator : scanOperators) {
                Assertions.assertTrue(scanOperator instanceof LogicalOlapScanOperator);
                Assertions.assertTrue(scanOperator.getTable().getName().equalsIgnoreCase("test_mv1"));
                final LogicalOlapScanOperator olapScanOperator = (LogicalOlapScanOperator) scanOperator;
                for (ColumnRefOperator colRef : olapScanOperator.getColumnMetaToColRefMap().values()) {
                    Assertions.assertTrue(!mvColumnRefSet.contains(colRef));
                    mvColumnRefSet.add(colRef);
                }
            }
        }
    }

    @Test
    public void testMVWriteWithDuplicatedMVs1() throws Exception {
        connectContext.getSessionVariable().setCboCTERuseRatio(-1);
        checkMVRewritePlanWithUniqueColumnRefs("create materialized view test_mv1 " +
                        " distributed by random" +
                        " as select emps.empid from emps join depts using (deptno)",
                ImmutableList.of(
                        "with cte1 as (select emps.empid from emps join depts using(deptno)) " +
                                " select * from cte1 union all select * from cte1 " +
                                "union all select * from cte1 union all select * from cte1",
                        "with cte1 as (select emps.empid from emps join depts using(deptno)) " +
                                " select * from cte1 a join cte1 b on a.empid=b.empid"
                ));
        connectContext.getSessionVariable().setCboCTERuseRatio(0);
    }

    @Test
    public void testMVWriteWithDuplicatedMVs2() throws Exception {
        connectContext.getSessionVariable().setCboCTERuseRatio(-1);
        checkMVRewritePlanWithUniqueColumnRefs("create materialized view test_mv1 " +
                        " distributed by random" +
                        " as select a.empid, a.name, sum(b.deptno) from emps a join depts b " +
                        " on a.deptno=b.deptno group by a.empid, a.name",
                ImmutableList.of(
                        "with cte1 as (select a.empid, sum(b.deptno) from emps a join depts b " +
                                " on a.deptno=b.deptno group by a.empid)" +
                                " select * from cte1 union all select * from cte1 " +
                                "union all select * from cte1 union all select * from cte1",
                        "with cte1 as (select a.empid, a.name, sum(b.deptno) from emps a join depts b " +
                                " on a.deptno=b.deptno group by a.empid, a.name)" +
                                " select * from cte1 union all select * from cte1 " +
                                "union all select * from cte1 union all select * from cte1",
                        "with cte1 as (select a.empid, a.name, sum(b.deptno) from emps a join depts b " +
                                " on a.deptno=b.deptno group by a.empid, a.name)" +
                                " select * from cte1 a join cte1 b on a.empid=b.empid"
                ));
        connectContext.getSessionVariable().setCboCTERuseRatio(0);
    }

    @Test
    public void testMVWriteWithDuplicatedMVs3() throws Exception {
        connectContext.getSessionVariable().setCboCTERuseRatio(-1);
        checkMVRewritePlanWithUniqueColumnRefs("create materialized view test_mv1 " +
                        " distributed by random" +
                        " as select empid, name from emps ",
                ImmutableList.of(
                        "with cte1 as (select empid, name from emps) " +
                                " select * from cte1 union all select * from cte1 " +
                                "union all select * from cte1 union all select * from cte1",
                        "with cte1 as (select empid, name from emps) " +
                                " select * from cte1 a join cte1 b on a.empid=b.empid"
                ));
        connectContext.getSessionVariable().setCboCTERuseRatio(0);
    }

    @Test
    public void testMVUnionRewriteWithDuplicateMVs1() throws Exception {
        starRocksAssert.withTable("CREATE TABLE s1 (\n" +
                "  num int,\n" +
                "  dt date\n" +
                ")\n" +
                "DUPLICATE KEY(`num`)\n" +
                "PARTITION BY RANGE(`dt`)\n" +
                "(\n" +
                "  PARTITION p20200615 VALUES [(\"2020-06-15 00:00:00\"), (\"2020-06-16 00:00:00\")),\n" +
                "  PARTITION p20200618 VALUES [(\"2020-06-18 00:00:00\"), (\"2020-06-19 00:00:00\")),\n" +
                "  PARTITION p20200621 VALUES [(\"2020-06-21 00:00:00\"), (\"2020-06-22 00:00:00\")),\n" +
                "  PARTITION p20200624 VALUES [(\"2020-06-24 00:00:00\"), (\"2020-06-25 00:00:00\")),\n" +
                "  PARTITION p20200702 VALUES [(\"2020-07-02 00:00:00\"), (\"2020-07-03 00:00:00\")),\n" +
                "  PARTITION p20200705 VALUES [(\"2020-07-05 00:00:00\"), (\"2020-07-06 00:00:00\")),\n" +
                "  PARTITION p20200708 VALUES [(\"2020-07-08 00:00:00\"), (\"2020-07-09 00:00:00\")),\n" +
                "  PARTITION p20200716 VALUES [(\"2020-07-16 00:00:00\"), (\"2020-07-17 00:00:00\")),\n" +
                "  PARTITION p20200719 VALUES [(\"2020-07-19 00:00:00\"), (\"2020-07-20 00:00:00\")),\n" +
                "  PARTITION p20200722 VALUES [(\"2020-07-22 00:00:00\"), (\"2020-07-23 00:00:00\")),\n" +
                "  PARTITION p20200725 VALUES [(\"2020-07-25 00:00:00\"), (\"2020-07-26 00:00:00\")),\n" +
                "  PARTITION p20200711 VALUES [(\"2020-07-11 00:00:00\"), (\"2020-07-12 00:00:00\"))\n" +
                ")\n" +
                "DISTRIBUTED BY HASH(`num`);");
        executeInsertSql("INSERT INTO s1 VALUES \n" +
                "  (1,\"2020-06-15\"),(2,\"2020-06-18\"),(3,\"2020-06-21\"),(4,\"2020-06-24\"),\n" +
                "  (1,\"2020-07-02\"),(2,\"2020-07-05\"),(3,\"2020-07-08\"),(4,\"2020-07-11\"),\n" +
                "  (1,\"2020-07-16\"),(2,\"2020-07-19\"),(3,\"2020-07-22\"),(4,\"2020-07-25\"),\n" +
                "  (2,\"2020-06-15\"),(3,\"2020-06-18\"),(4,\"2020-06-21\"),(5,\"2020-06-24\"),\n" +
                "  (2,\"2020-07-02\"),(3,\"2020-07-05\"),(4,\"2020-07-08\"),(5,\"2020-07-11\");");
        withRefreshedMV("CREATE MATERIALIZED VIEW test_mv1 \n" +
                        "PARTITION BY dt \n" +
                        "REFRESH DEFERRED MANUAL \n" +
                        "AS SELECT * FROM s1 where dt > '2020-07-01';",
                () -> {
                    String query = "SELECT * FROM (SELECT * FROM s1 where num > 3 " +
                            "UNION ALL SELECT * FROM s1 where num > 3) t order by 1, 2 limit 3;";
                    connectContext.getSessionVariable().setMaterializedViewUnionRewriteMode(2);
                    final String plan = getFragmentPlan(query);
                    PlanTestBase.assertContains(plan, "test_mv1");
                    connectContext.getSessionVariable().setMaterializedViewUnionRewriteMode(0);
                });
    }
}
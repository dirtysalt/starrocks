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

package com.starrocks.load;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.starrocks.analysis.BinaryPredicate;
import com.starrocks.analysis.BinaryType;
import com.starrocks.analysis.IntLiteral;
import com.starrocks.analysis.SlotRef;
import com.starrocks.analysis.TableName;
import com.starrocks.backup.CatalogMocker;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.Replica;
import com.starrocks.catalog.TabletInvertedIndex;
import com.starrocks.catalog.TabletMeta;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReportException;
import com.starrocks.common.FeConstants;
import com.starrocks.common.StarRocksException;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.common.util.concurrent.MarkedCountDownLatch;
import com.starrocks.common.util.concurrent.lock.LockException;
import com.starrocks.common.util.concurrent.lock.LockType;
import com.starrocks.common.util.concurrent.lock.Locker;
import com.starrocks.common.util.concurrent.lock.NotSupportLockException;
import com.starrocks.load.DeleteJob.DeleteState;
import com.starrocks.persist.EditLog;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.QueryStateException;
import com.starrocks.qe.VariableMgr;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.LocalMetastore;
import com.starrocks.sql.analyzer.Analyzer;
import com.starrocks.sql.ast.DeleteStmt;
import com.starrocks.sql.ast.PartitionNames;
import com.starrocks.system.SystemInfoService;
import com.starrocks.task.AgentBatchTask;
import com.starrocks.task.AgentTask;
import com.starrocks.task.AgentTaskExecutor;
import com.starrocks.task.AgentTaskQueue;
import com.starrocks.transaction.GlobalTransactionMgr;
import com.starrocks.transaction.TabletCommitInfo;
import com.starrocks.transaction.TabletFailInfo;
import com.starrocks.transaction.TransactionState;
import com.starrocks.transaction.TransactionStatus;
import com.starrocks.transaction.TxnCommitAttachment;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertThrows;

public class DeleteHandlerTest {

    private DeleteMgr deleteHandler;

    private static final long BACKEND_ID_1 = 10000L;
    private static final long BACKEND_ID_2 = 10001L;
    private static final long BACKEND_ID_3 = 10002L;
    private static final long REPLICA_ID_1 = 70000L;
    private static final long REPLICA_ID_2 = 70001L;
    private static final long REPLICA_ID_3 = 70002L;
    private static final long TABLET_ID = 60000L;
    private static final long PARTITION_ID = 40000L;
    private static final long PH_PARTITION_ID = 40011L;
    private static final long TBL_ID = 30000L;
    private static final long DB_ID = 20000L;

    @Mocked
    private GlobalStateMgr globalStateMgr;
    @Mocked
    private EditLog editLog;
    @Mocked
    private AgentTaskQueue agentTaskQueue;
    @Mocked
    private AgentTaskExecutor executor;

    private Database db;

    private GlobalTransactionMgr globalTransactionMgr;
    private TabletInvertedIndex invertedIndex = new TabletInvertedIndex();
    private ConnectContext connectContext = new ConnectContext();
    private VariableMgr variableMgr = new VariableMgr();

    @BeforeEach
    public void setUp() {
        FeConstants.runningUnitTest = true;

        globalTransactionMgr = new GlobalTransactionMgr(globalStateMgr);
        connectContext.setGlobalStateMgr(globalStateMgr);
        connectContext.setSessionVariable(variableMgr.newSessionVariable());
        deleteHandler = new DeleteMgr();
        try {
            db = CatalogMocker.mockDb();
        } catch (AnalysisException e) {
            e.printStackTrace();
            Assertions.fail();
        }
        TabletMeta tabletMeta = new TabletMeta(DB_ID, TBL_ID, PH_PARTITION_ID, TBL_ID, null);
        invertedIndex.addTablet(TABLET_ID, tabletMeta);
        invertedIndex.addReplica(TABLET_ID, new Replica(REPLICA_ID_1, BACKEND_ID_1, 0, Replica.ReplicaState.NORMAL));
        invertedIndex.addReplica(TABLET_ID, new Replica(REPLICA_ID_2, BACKEND_ID_2, 0, Replica.ReplicaState.NORMAL));
        invertedIndex.addReplica(TABLET_ID, new Replica(REPLICA_ID_3, BACKEND_ID_3, 0, Replica.ReplicaState.NORMAL));

        new MockUp<EditLog>() {
            @Mock
            public void logSaveTransactionId(long transactionId) {
            }

            @Mock
            public void logInsertTransactionState(TransactionState transactionState) {
            }
        };

        Analyzer analyzer = new Analyzer(Analyzer.AnalyzerVisitor.getInstance());

        new Expectations() {
            {
                GlobalStateMgr.getCurrentState();
                minTimes = 0;
                result = globalStateMgr;

                globalStateMgr.getLocalMetastore().getDb(anyString);
                minTimes = 0;
                result = db;

                globalStateMgr.getLocalMetastore().getDb(anyLong);
                minTimes = 0;
                result = db;

                globalStateMgr.getLocalMetastore().getTable("test_db", "test_tbl");
                minTimes = 0;
                result = db.getTable("test_tbl");

                globalStateMgr.getLocalMetastore().getTable(CatalogMocker.TEST_DB_ID, CatalogMocker.TEST_TBL_ID);
                minTimes = 0;
                result = db.getTable("test_tbl");

                globalStateMgr.getEditLog();
                minTimes = 0;
                result = editLog;

                globalStateMgr.getNextId();
                minTimes = 0;
                result = 10L;

                globalStateMgr.getTabletInvertedIndex();
                minTimes = 0;
                result = invertedIndex;

                globalStateMgr.getEditLog();
                minTimes = 0;
                result = editLog;

                globalStateMgr.getVariableMgr();
                minTimes = 0;
                result = variableMgr;
            }
        };
        globalTransactionMgr.addDatabaseTransactionMgr(db.getId());

        SystemInfoService systemInfoService = new SystemInfoService();
        new Expectations() {
            {
                GlobalStateMgr.getCurrentState();
                minTimes = 0;
                result = globalStateMgr;

                GlobalStateMgr.getCurrentState().getTabletInvertedIndex();
                minTimes = 0;
                result = invertedIndex;

                GlobalStateMgr.getCurrentState().getGlobalTransactionMgr();
                minTimes = 0;
                result = globalTransactionMgr;

                AgentTaskExecutor.submit((AgentBatchTask) any);
                minTimes = 0;

                AgentTaskQueue.addTask((AgentTask) any);
                minTimes = 0;
                result = true;

                globalStateMgr.getAnalyzer();
                result = analyzer;
                minTimes = 0;
            }
        };

        new MockUp<LocalMetastore>() {
            @Mock
            public Database getDb(String dbName) {
                return db;
            }
        };
    }

    @Test
    public void testUnQuorumTimeout() {
        assertThrows(DdlException.class, () -> {
            BinaryPredicate binaryPredicate = new BinaryPredicate(BinaryType.GT, new SlotRef(null, "k1"),
                    new IntLiteral(3));

            DeleteStmt deleteStmt = new DeleteStmt(new TableName("test_db", "test_tbl"),
                    new PartitionNames(false, Lists.newArrayList("test_tbl")), binaryPredicate);

            new Expectations(globalTransactionMgr) {
                {
                    try {
                        globalTransactionMgr.abortTransaction(db.getId(), anyLong, anyString);
                    } catch (StarRocksException e) {
                    }
                    minTimes = 0;
                }
            };
            try {
                com.starrocks.sql.analyzer.Analyzer.analyze(deleteStmt, connectContext);
            } catch (Exception e) {
                Assertions.fail();
            }
            deleteHandler.process(deleteStmt);
            Assertions.fail();
        });
    }

    @Test
    public void testQuorumTimeout() throws DdlException, QueryStateException {
        BinaryPredicate binaryPredicate = new BinaryPredicate(BinaryType.GT, new SlotRef(null, "k1"),
                new IntLiteral(3));

        DeleteStmt deleteStmt = new DeleteStmt(new TableName("test_db", "test_tbl"),
                new PartitionNames(false, Lists.newArrayList("test_tbl")), binaryPredicate);

        Set<Replica> finishedReplica = Sets.newHashSet();
        finishedReplica.add(new Replica(REPLICA_ID_1, BACKEND_ID_1, 0, Replica.ReplicaState.NORMAL));
        finishedReplica.add(new Replica(REPLICA_ID_2, BACKEND_ID_2, 0, Replica.ReplicaState.NORMAL));
        TabletDeleteInfo tabletDeleteInfo = new TabletDeleteInfo(PH_PARTITION_ID, TABLET_ID);
        tabletDeleteInfo.getFinishedReplicas().addAll(finishedReplica);

        new MockUp<OlapDeleteJob>() {
            @Mock
            public Collection<TabletDeleteInfo> getTabletDeleteInfo() {
                return Lists.newArrayList(tabletDeleteInfo);
            }
        };

        new MockUp<GlobalTransactionMgr>() {
            @Mock
            public TransactionState getTransactionState(long transactionId) {
                TransactionState transactionState = new TransactionState();
                transactionState.setTransactionStatus(TransactionStatus.VISIBLE);
                return transactionState;
            }
        };

        try {
            com.starrocks.sql.analyzer.Analyzer.analyze(deleteStmt, connectContext);
        } catch (Exception e) {
            Assertions.fail();
        }
        try {
            deleteHandler.process(deleteStmt);
        } catch (QueryStateException e) {
        }

        Map<Long, DeleteJob> idToDeleteJob = Deencapsulation.getField(deleteHandler, "idToDeleteJob");
        Collection<DeleteJob> jobs = idToDeleteJob.values();
        Assertions.assertEquals(1, jobs.size());
        for (DeleteJob job : jobs) {
            Assertions.assertEquals(job.getState(), DeleteState.QUORUM_FINISHED);
        }
    }

    @Test
    public void testNormalTimeout() throws DdlException, QueryStateException {
        BinaryPredicate binaryPredicate = new BinaryPredicate(BinaryType.GT, new SlotRef(null, "k1"),
                new IntLiteral(3));

        DeleteStmt deleteStmt = new DeleteStmt(new TableName("test_db", "test_tbl"),
                new PartitionNames(false, Lists.newArrayList("test_tbl")), binaryPredicate);

        Set<Replica> finishedReplica = Sets.newHashSet();
        finishedReplica.add(new Replica(REPLICA_ID_1, BACKEND_ID_1, 0, Replica.ReplicaState.NORMAL));
        finishedReplica.add(new Replica(REPLICA_ID_2, BACKEND_ID_2, 0, Replica.ReplicaState.NORMAL));
        finishedReplica.add(new Replica(REPLICA_ID_3, BACKEND_ID_3, 0, Replica.ReplicaState.NORMAL));
        TabletDeleteInfo tabletDeleteInfo = new TabletDeleteInfo(PH_PARTITION_ID, TABLET_ID);
        tabletDeleteInfo.getFinishedReplicas().addAll(finishedReplica);

        new MockUp<OlapDeleteJob>() {
            @Mock
            public Collection<TabletDeleteInfo> getTabletDeleteInfo() {
                return Lists.newArrayList(tabletDeleteInfo);
            }
        };

        new MockUp<GlobalTransactionMgr>() {
            @Mock
            public TransactionState getTransactionState(long transactionId) {
                TransactionState transactionState = new TransactionState();
                transactionState.setTransactionStatus(TransactionStatus.VISIBLE);
                return transactionState;
            }
        };

        try {
            com.starrocks.sql.analyzer.Analyzer.analyze(deleteStmt, connectContext);
        } catch (Exception e) {
            Assertions.fail();
        }

        try {
            deleteHandler.process(deleteStmt);
        } catch (QueryStateException e) {
        }

        Map<Long, DeleteJob> idToDeleteJob = Deencapsulation.getField(deleteHandler, "idToDeleteJob");
        Collection<DeleteJob> jobs = idToDeleteJob.values();
        Assertions.assertEquals(1, jobs.size());
        for (DeleteJob job : jobs) {
            Assertions.assertEquals(job.getState(), DeleteState.FINISHED);
        }
    }

    @Test
    public void testCommitFail(@Mocked MarkedCountDownLatch countDownLatch) {
        assertThrows(DdlException.class, () -> {
            BinaryPredicate binaryPredicate = new BinaryPredicate(BinaryType.GT, new SlotRef(null, "k1"),
                    new IntLiteral(3));

            DeleteStmt deleteStmt = new DeleteStmt(new TableName("test_db", "test_tbl"),
                    new PartitionNames(false, Lists.newArrayList("test_tbl")), binaryPredicate);

            Set<Replica> finishedReplica = Sets.newHashSet();
            finishedReplica.add(new Replica(REPLICA_ID_1, BACKEND_ID_1, 0, Replica.ReplicaState.NORMAL));
            finishedReplica.add(new Replica(REPLICA_ID_2, BACKEND_ID_2, 0, Replica.ReplicaState.NORMAL));
            finishedReplica.add(new Replica(REPLICA_ID_3, BACKEND_ID_3, 0, Replica.ReplicaState.NORMAL));
            TabletDeleteInfo tabletDeleteInfo = new TabletDeleteInfo(PH_PARTITION_ID, TABLET_ID);
            tabletDeleteInfo.getFinishedReplicas().addAll(finishedReplica);

            new MockUp<OlapDeleteJob>() {
                @Mock
                public Collection<TabletDeleteInfo> getTabletDeleteInfo() {
                    return Lists.newArrayList(tabletDeleteInfo);
                }
            };

            new Expectations() {
                {
                    try {
                        countDownLatch.await(anyLong, (TimeUnit) any);
                    } catch (InterruptedException e) {
                    }
                    result = false;
                }
            };

            new Expectations(globalTransactionMgr) {
                {
                    try {
                        globalTransactionMgr.commitTransaction(anyLong, anyLong, (List<TabletCommitInfo>) any,
                                (List<TabletFailInfo>) any,
                                (TxnCommitAttachment) any);
                    } catch (StarRocksException e) {
                    }
                    result = new StarRocksException("commit fail");
                }
            };

            try {
                com.starrocks.sql.analyzer.Analyzer.analyze(deleteStmt, connectContext);
            } catch (Exception e) {
                Assertions.fail();
            }
            try {
                deleteHandler.process(deleteStmt);
            } catch (DdlException e) {
                Map<Long, DeleteJob> idToDeleteJob = Deencapsulation.getField(deleteHandler, "idToDeleteJob");
                Collection<DeleteJob> jobs = idToDeleteJob.values();
                Assertions.assertEquals(1, jobs.size());
                for (DeleteJob job : jobs) {
                    Assertions.assertEquals(job.getState(), DeleteState.FINISHED);
                }
                throw e;
            } catch (QueryStateException e) {
            }
            Assertions.fail();
        });
    }

    @Test
    public void testPublishFail(@Mocked MarkedCountDownLatch countDownLatch, @Mocked AgentTaskExecutor taskExecutor)
            throws DdlException, QueryStateException {
        BinaryPredicate binaryPredicate = new BinaryPredicate(BinaryType.GT, new SlotRef(null, "k1"),
                new IntLiteral(3));

        DeleteStmt deleteStmt = new DeleteStmt(new TableName("test_db", "test_tbl"),
                new PartitionNames(false, Lists.newArrayList("test_tbl")), binaryPredicate);

        Set<Replica> finishedReplica = Sets.newHashSet();
        finishedReplica.add(new Replica(REPLICA_ID_1, BACKEND_ID_1, 0, Replica.ReplicaState.NORMAL));
        finishedReplica.add(new Replica(REPLICA_ID_2, BACKEND_ID_2, 0, Replica.ReplicaState.NORMAL));
        finishedReplica.add(new Replica(REPLICA_ID_3, BACKEND_ID_3, 0, Replica.ReplicaState.NORMAL));
        TabletDeleteInfo tabletDeleteInfo = new TabletDeleteInfo(PH_PARTITION_ID, TABLET_ID);
        tabletDeleteInfo.getFinishedReplicas().addAll(finishedReplica);

        new MockUp<OlapDeleteJob>() {
            @Mock
            public Collection<TabletDeleteInfo> getTabletDeleteInfo() {
                return Lists.newArrayList(tabletDeleteInfo);
            }
        };

        new Expectations() {
            {
                try {
                    countDownLatch.await(anyLong, (TimeUnit) any);
                } catch (InterruptedException e) {
                }
                result = false;
            }
        };

        new Expectations() {
            {
                AgentTaskExecutor.submit((AgentBatchTask) any);
                minTimes = 0;
            }
        };

        try {
            com.starrocks.sql.analyzer.Analyzer.analyze(deleteStmt, connectContext);
        } catch (Exception e) {
            Assertions.fail();
        }
        try {
            deleteHandler.process(deleteStmt);
        } catch (QueryStateException e) {
        }

        Map<Long, DeleteJob> idToDeleteJob = Deencapsulation.getField(deleteHandler, "idToDeleteJob");
        Collection<DeleteJob> jobs = idToDeleteJob.values();
        Assertions.assertEquals(1, jobs.size());
        for (DeleteJob job : jobs) {
            Assertions.assertEquals(job.getState(), DeleteState.FINISHED);
        }
    }

    @Test
    public void testNormal(@Mocked MarkedCountDownLatch countDownLatch) throws DdlException, QueryStateException {
        BinaryPredicate binaryPredicate = new BinaryPredicate(BinaryType.GT, new SlotRef(null, "k1"),
                new IntLiteral(3));

        DeleteStmt deleteStmt = new DeleteStmt(new TableName("test_db", "test_tbl"),
                new PartitionNames(false, Lists.newArrayList("test_tbl")), binaryPredicate);

        Set<Replica> finishedReplica = Sets.newHashSet();
        finishedReplica.add(new Replica(REPLICA_ID_1, BACKEND_ID_1, 0, Replica.ReplicaState.NORMAL));
        finishedReplica.add(new Replica(REPLICA_ID_2, BACKEND_ID_2, 0, Replica.ReplicaState.NORMAL));
        finishedReplica.add(new Replica(REPLICA_ID_3, BACKEND_ID_3, 0, Replica.ReplicaState.NORMAL));
        TabletDeleteInfo tabletDeleteInfo = new TabletDeleteInfo(PH_PARTITION_ID, TABLET_ID);
        tabletDeleteInfo.getFinishedReplicas().addAll(finishedReplica);

        new MockUp<OlapDeleteJob>() {
            @Mock
            public Collection<TabletDeleteInfo> getTabletDeleteInfo() {
                return Lists.newArrayList(tabletDeleteInfo);
            }
        };

        new Expectations() {
            {
                AgentTaskExecutor.submit((AgentBatchTask) any);
                minTimes = 0;
            }
        };

        try {
            com.starrocks.sql.analyzer.Analyzer.analyze(deleteStmt, connectContext);
        } catch (Exception e) {
            Assertions.fail();
        }
        try {
            deleteHandler.process(deleteStmt);
        } catch (QueryStateException e) {
        }

        Map<Long, DeleteJob> idToDeleteJob = Deencapsulation.getField(deleteHandler, "idToDeleteJob");
        Collection<DeleteJob> jobs = idToDeleteJob.values();
        Assertions.assertEquals(1, jobs.size());
        for (DeleteJob job : jobs) {
            Assertions.assertEquals(job.getState(), DeleteState.FINISHED);
        }
    }

    @Test
    public void testLockTimeout(@Mocked MarkedCountDownLatch countDownLatch) throws DdlException, QueryStateException {
        BinaryPredicate binaryPredicate = new BinaryPredicate(BinaryType.GT, new SlotRef(null, "k1"),
                new IntLiteral(3));

        DeleteStmt deleteStmt = new DeleteStmt(new TableName("test_db", "test_tbl"),
                new PartitionNames(false, Lists.newArrayList("test_tbl")), binaryPredicate);

        Set<Replica> finishedReplica = Sets.newHashSet();
        finishedReplica.add(new Replica(REPLICA_ID_1, BACKEND_ID_1, 0, Replica.ReplicaState.NORMAL));
        finishedReplica.add(new Replica(REPLICA_ID_2, BACKEND_ID_2, 0, Replica.ReplicaState.NORMAL));
        finishedReplica.add(new Replica(REPLICA_ID_3, BACKEND_ID_3, 0, Replica.ReplicaState.NORMAL));
        TabletDeleteInfo tabletDeleteInfo = new TabletDeleteInfo(PH_PARTITION_ID, TABLET_ID);
        tabletDeleteInfo.getFinishedReplicas().addAll(finishedReplica);

        new MockUp<OlapDeleteJob>() {
            @Mock
            public Collection<TabletDeleteInfo> getTabletDeleteInfo() {
                return Lists.newArrayList(tabletDeleteInfo);
            }
        };

        new MockUp<Locker>() {
            @Mock
            public void lock(long rid, LockType lockType, long timeout) throws LockException {
                throw new NotSupportLockException("");
            }
        };

        new Expectations() {
            {
                AgentTaskExecutor.submit((AgentBatchTask) any);
                minTimes = 0;
            }
        };

        try {
            com.starrocks.sql.analyzer.Analyzer.analyze(deleteStmt, connectContext);
        } catch (Exception e) {
            Assertions.fail();
        }
        try {
            deleteHandler.process(deleteStmt);
        } catch (ErrorReportException e) {
            Assertions.assertEquals(e.getErrorCode(), ErrorCode.ERR_LOCK_ERROR);
        }
    }

    @Test
    public void testRemoveOldOnReplay() throws Exception {
        new Expectations(globalStateMgr) {
            {
                globalStateMgr.getLocalMetastore().getDb(1L);
                minTimes = 0;
                result = db;

                globalStateMgr.getLocalMetastore().getTable(anyLong, anyLong);
                minTimes = 0;
                result = db.getTable("test_tbl");
            }
        };

        Config.label_keep_max_second = 1;
        Config.label_keep_max_num = 10;

        // 1. noraml replay
        DeleteInfo normalDelete = new DeleteInfo(1, 1, "test_tbl", -1, "test_partition", 1, new ArrayList<>());
        deleteHandler.replayDelete(normalDelete, globalStateMgr);
        Assertions.assertEquals(1, deleteHandler.getDeleteInfosByDb(1).size());
        MultiDeleteInfo multiDeleteInfo = new MultiDeleteInfo(1, 1, "test_tbl", new ArrayList<>());
        deleteHandler.replayMultiDelete(multiDeleteInfo, globalStateMgr);
        Assertions.assertEquals(2, deleteHandler.getDeleteInfosByDb(1).size());

        // 2. replay after expire
        DeleteInfo expireDelete = new DeleteInfo(1, 1, "test_tbl", -1, "test_partition", 1, new ArrayList<>());
        MultiDeleteInfo expireMultiDelete = new MultiDeleteInfo(1, 1, "test_tbl", new ArrayList<>());
        Thread.sleep(2000);
        deleteHandler.replayDelete(expireDelete, globalStateMgr);
        Assertions.assertEquals(2, deleteHandler.getDeleteInfosByDb(1).size());
        deleteHandler.replayMultiDelete(expireMultiDelete, globalStateMgr);
        Assertions.assertEquals(2, deleteHandler.getDeleteInfosByDb(1).size());

        // 3. run clean job clean expired job &
        Config.label_keep_max_second = 1;
        Config.label_keep_max_num = 1;
        normalDelete = new DeleteInfo(1, 1, "test_tbl", -1, "test_partition", 1, new ArrayList<>());
        deleteHandler.replayDelete(normalDelete, globalStateMgr);
        Assertions.assertEquals(3, deleteHandler.getDeleteInfosByDb(1).size());
        normalDelete = new DeleteInfo(1, 2, "test_tbl2", -1, "test_partition", 1, new ArrayList<>());
        deleteHandler.replayDelete(normalDelete, globalStateMgr);
        Assertions.assertEquals(4, deleteHandler.getDeleteInfosByDb(1).size());
        deleteHandler.removeOldDeleteInfo();
        List<List<Comparable>> deleteInfos = deleteHandler.getDeleteInfosByDb(1);
        Assertions.assertEquals(1, deleteInfos.size());
        Assertions.assertEquals("test_tbl2", (String) deleteInfos.get(0).get(0));

    }
}

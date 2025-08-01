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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/catalog/OlapTable.java

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

package com.starrocks.catalog;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Range;
import com.google.common.collect.Sets;
import com.google.gson.annotations.SerializedName;
import com.staros.proto.FileCacheInfo;
import com.staros.proto.FilePathInfo;
import com.starrocks.alter.AlterJobV2Builder;
import com.starrocks.alter.OlapTableAlterJobV2Builder;
import com.starrocks.alter.OlapTableRollupJobBuilder;
import com.starrocks.alter.OptimizeJobV2Builder;
import com.starrocks.analysis.DescriptorTable.ReferencedPartitionInfo;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.LiteralExpr;
import com.starrocks.analysis.SlotDescriptor;
import com.starrocks.analysis.SlotId;
import com.starrocks.analysis.SlotRef;
import com.starrocks.analysis.TableName;
import com.starrocks.backup.Status;
import com.starrocks.backup.Status.ErrCode;
import com.starrocks.backup.mv.MvBackupInfo;
import com.starrocks.backup.mv.MvRestoreContext;
import com.starrocks.binlog.BinlogConfig;
import com.starrocks.catalog.DistributionInfo.DistributionInfoType;
import com.starrocks.catalog.LocalTablet.TabletHealthStatus;
import com.starrocks.catalog.MaterializedIndex.IndexExtState;
import com.starrocks.catalog.MaterializedIndex.IndexState;
import com.starrocks.catalog.Partition.PartitionState;
import com.starrocks.catalog.Replica.ReplicaState;
import com.starrocks.catalog.constraint.ForeignKeyConstraint;
import com.starrocks.catalog.constraint.GlobalConstraintManager;
import com.starrocks.catalog.constraint.UniqueConstraint;
import com.starrocks.clone.TabletChecker;
import com.starrocks.clone.TabletSchedCtx;
import com.starrocks.clone.TabletScheduler;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.common.InvalidOlapTableStateException;
import com.starrocks.common.Pair;
import com.starrocks.common.io.DeepCopy;
import com.starrocks.common.util.DateUtils;
import com.starrocks.common.util.DynamicPartitionUtil;
import com.starrocks.common.util.PropertyAnalyzer;
import com.starrocks.common.util.RangeUtils;
import com.starrocks.common.util.TimeUtils;
import com.starrocks.common.util.Util;
import com.starrocks.common.util.WriteQuorum;
import com.starrocks.common.util.concurrent.MarkedCountDownLatch;
import com.starrocks.lake.DataCacheInfo;
import com.starrocks.lake.StarOSAgent;
import com.starrocks.lake.StorageInfo;
import com.starrocks.persist.ColocatePersistInfo;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.OriginStatement;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.server.TemporaryTableMgr;
import com.starrocks.sql.analyzer.AnalyzeState;
import com.starrocks.sql.analyzer.AnalyzerUtils;
import com.starrocks.sql.analyzer.ExpressionAnalyzer;
import com.starrocks.sql.analyzer.Field;
import com.starrocks.sql.analyzer.RelationFields;
import com.starrocks.sql.analyzer.RelationId;
import com.starrocks.sql.analyzer.Scope;
import com.starrocks.sql.analyzer.SelectAnalyzer;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.IndexDef.IndexType;
import com.starrocks.sql.ast.PartitionValue;
import com.starrocks.sql.common.MetaUtils;
import com.starrocks.sql.common.PCell;
import com.starrocks.sql.common.PListCell;
import com.starrocks.sql.common.PRangeCell;
import com.starrocks.sql.common.SyncPartitionUtils;
import com.starrocks.sql.optimizer.rule.mv.MVUtils;
import com.starrocks.sql.optimizer.statistics.IDictManager;
import com.starrocks.system.SystemInfoService;
import com.starrocks.task.AgentBatchTask;
import com.starrocks.task.AgentTask;
import com.starrocks.task.AgentTaskExecutor;
import com.starrocks.task.AgentTaskQueue;
import com.starrocks.task.DropAutoIncrementMapTask;
import com.starrocks.task.TabletTaskExecutor;
import com.starrocks.thrift.TCompactionStrategy;
import com.starrocks.thrift.TCompressionType;
import com.starrocks.thrift.TOlapTable;
import com.starrocks.thrift.TPersistentIndexType;
import com.starrocks.thrift.TStorageMedium;
import com.starrocks.thrift.TStorageType;
import com.starrocks.thrift.TTableDescriptor;
import com.starrocks.thrift.TTableType;
import com.starrocks.thrift.TWriteQuorumType;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections4.ListUtils;
import org.apache.commons.collections4.map.CaseInsensitiveMap;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.threeten.extra.PeriodDuration;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;
import java.util.zip.Adler32;
import javax.annotation.Nullable;

import static com.starrocks.common.util.PropertyAnalyzer.PROPERTIES_STORAGE_TYPE_COLUMN;
import static com.starrocks.common.util.PropertyAnalyzer.PROPERTIES_STORAGE_TYPE_COLUMN_WITH_ROW;

/**
 * Internal representation of tableFamilyGroup-related metadata. A
 * OlaptableFamilyGroup contains several tableFamily.
 * Note: when you add a new olap table property, you should modify TableProperty
 * class
 * ATTN: serialize by gson is used by MaterializedView
 */
public class OlapTable extends Table {
    private static final Logger LOG = LogManager.getLogger(OlapTable.class);

    public enum OlapTableState {
        NORMAL,
        ROLLUP,
        SCHEMA_CHANGE,
        @Deprecated
        BACKUP,
        RESTORE,
        RESTORE_WITH_LOAD,
        /*
         * this state means table is under PENDING alter operation(SCHEMA_CHANGE or
         * ROLLUP), and is not
         * stable. The tablet scheduler will continue fixing the tablets of this table.
         * And the state will
         * change back to SCHEMA_CHANGE or ROLLUP after table is stable, and continue
         * doing alter operation.
         * This state is an in-memory state and no need to persist.
         */
        WAITING_STABLE,
        /* This state means table is updating table meta during alter operation(SCHEMA_CHANGE
         * or ROLLUP).
         * The query plan which is generate during this state is invalid because the meta
         * during the creation of the logical plan and the physical plan might be inconsistent.
        */
        UPDATING_META,
        OPTIMIZE
    }

    @SerializedName(value = "state")
    protected OlapTableState state;

    // index id -> index meta
    @SerializedName(value = "indexIdToMeta")
    protected Map<Long, MaterializedIndexMeta> indexIdToMeta = Maps.newHashMap();
    // index name -> index id
    @SerializedName(value = "indexNameToId")
    protected Map<String, Long> indexNameToId = Maps.newHashMap();

    @SerializedName(value = "keysType")
    protected KeysType keysType;

    @SerializedName(value = "partitionInfo")
    protected PartitionInfo partitionInfo;

    @SerializedName(value = "idToPartition")
    protected Map<Long, Partition> idToPartition = new HashMap<>();
    protected Map<String, Partition> nameToPartition = Maps.newTreeMap(String.CASE_INSENSITIVE_ORDER);

    protected Map<Long, Long> physicalPartitionIdToPartitionId = new HashMap<>();
    protected Map<String, Long> physicalPartitionNameToPartitionId = new HashMap<>();

    @SerializedName(value = "defaultDistributionInfo")
    protected DistributionInfo defaultDistributionInfo;

    // all info about temporary partitions are save in "tempPartitions"
    @SerializedName(value = "tempPartitions")
    protected TempPartitions tempPartitions = new TempPartitions();

    // bloom filter columns
    @SerializedName(value = "bfColumns")
    protected Set<ColumnId> bfColumns;

    @SerializedName(value = "bfFpp")
    protected double bfFpp;

    @SerializedName(value = "colocateGroup")
    protected String colocateGroup;

    @SerializedName(value = "indexes")
    protected TableIndexes indexes;

    // In former implementation, base index id is same as table id.
    // But when refactoring the process of alter table job, we find that
    // using same id is not suitable for our new framework.
    // So we add this 'baseIndexId' to explicitly specify the base index id,
    // which should be different with table id.
    // The init value is -1, which means there is not partition and index at all.
    @SerializedName(value = "baseIndexId")
    protected long baseIndexId = -1;

    @SerializedName(value = "tableProperty")
    protected TableProperty tableProperty;

    @SerializedName(value = "maxColUniqueId")
    protected AtomicInteger maxColUniqueId = new AtomicInteger(-1);

    // We can utilize a monotonically increasing IndexId,
    // which is based on the OlapTable, to uniquely identify an index. When adding a multi-column index,
    // we can assign the 'indexId' as the index's name. Furthermore, if we need to replace an old index
    // with a new one that has the same 'indexName', the unique 'indexId' allows us to distinguish between them.
    @SerializedName(value = "maxIndexId")
    protected long maxIndexId = -1;

    // the id of the session that created this table, only used in temporary table
    @SerializedName(value = "sessionId")
    protected UUID sessionId = null;

    protected BinlogConfig curBinlogConfig;

    // After ensuring that all binlog config of tablets in BE have taken effect,
    // apply for a transaction id as binlogtxnId.
    // The purpose is to ensure that in the case of concurrent imports,
    // need to wait for the completion of concurrent imports,
    // that is, all transactions which id is smaller than binlogTxnId have been
    // finished/aborted,
    // then binlog is available
    protected long binlogTxnId = -1;

    // Record the alter, schema change, MV update time
    public AtomicLong lastSchemaUpdateTime = new AtomicLong(-1);

    private Map<String, Lock> createPartitionLocks = Maps.newHashMap();

    protected Map<Long, Long> doubleWritePartitions = new HashMap<>();

    // Both the following two flags are used by StarMgrMetaSyncer
    private long lastCollectProfileTime = 0;

    // The flag is used to indicate whether the table shard group has changed.
    public AtomicBoolean isShardGroupChanged = new AtomicBoolean(false);
    // The flag is used to indicate whether the table is doing automatic bucketing.
    public AtomicBoolean isAutomaticBucketing = new AtomicBoolean(false);

    // It's not persisted but resolved in QueryAnalyzer, so only exists if it's in the context of query
    public volatile String dbName;

    public OlapTable() {
        this(TableType.OLAP);
    }

    public OlapTable(TableType type) {
        // for persist
        super(type);

        this.bfColumns = null;
        this.bfFpp = 0;

        this.colocateGroup = null;

        this.indexes = null;

        this.tableProperty = null;
    }

    public OlapTable(long id, String tableName, List<Column> baseSchema, KeysType keysType,
                     PartitionInfo partitionInfo, DistributionInfo defaultDistributionInfo) {
        this(id, tableName, baseSchema, keysType, partitionInfo, defaultDistributionInfo, null);
    }

    public OlapTable(long id, String tableName, List<Column> baseSchema, KeysType keysType,
                     PartitionInfo partitionInfo, DistributionInfo defaultDistributionInfo,
                     TableIndexes indexes) {
        this(id, tableName, baseSchema, keysType, partitionInfo, defaultDistributionInfo, indexes, TableType.OLAP);
    }

    public OlapTable(long id, String tableName, List<Column> baseSchema, KeysType keysType,
                     PartitionInfo partitionInfo, DistributionInfo defaultDistributionInfo,
                     TableIndexes indexes, TableType tableType) {
        super(id, tableName, tableType, baseSchema);

        this.state = OlapTableState.NORMAL;

        this.keysType = keysType;
        this.partitionInfo = partitionInfo;

        this.defaultDistributionInfo = defaultDistributionInfo;

        this.bfColumns = null;
        this.bfFpp = 0;

        this.colocateGroup = null;

        this.indexes = indexes;
        tryToAssignIndexId();

        this.tableProperty = null;
    }

    public static List<Index> getIndexesBySchema(List<Index> indexes, List<Column> schema) {
        List<Index> hitIndexes = Lists.newArrayList();
        Set<ColumnId> columnIdsSetForSchema =
                            schema.stream().map(col -> col.getColumnId()).collect(Collectors.toSet());

        for (Index index : indexes) {
            Set<ColumnId> columnIdsSetForIndex = index.getColumns().stream().collect(Collectors.toSet());
            if (columnIdsSetForSchema.containsAll(columnIdsSetForIndex)) {
                hitIndexes.add(index);
            }
        }
        return hitIndexes;
    }

    @Override
    public synchronized Optional<String> mayGetDatabaseName() {
        return Optional.ofNullable(dbName);
    }

    public synchronized void maySetDatabaseName(String dbName) {
        if (this.dbName == null) {
            this.dbName = dbName;
        }
    }

    // Only Copy necessary metadata for query.
    // We don't do deep copy, because which is very expensive;
    public void copyOnlyForQuery(OlapTable olapTable) {
        olapTable.id = this.id;
        olapTable.name = this.name;
        olapTable.type = this.type;
        olapTable.fullSchema = Lists.newArrayList(this.fullSchema);
        olapTable.nameToColumn = new CaseInsensitiveMap(this.nameToColumn);
        olapTable.idToColumn = new CaseInsensitiveMap(this.idToColumn);
        olapTable.state = this.state;
        olapTable.indexNameToId = Maps.newHashMap(this.indexNameToId);
        olapTable.indexIdToMeta = Maps.newHashMap(this.indexIdToMeta);
        olapTable.indexes = indexes == null ? null : indexes.shallowCopy();
        if (bfColumns != null) {
            olapTable.bfColumns = Sets.newTreeSet(ColumnId.CASE_INSENSITIVE_ORDER);
            olapTable.bfColumns.addAll(bfColumns);
        } else {
            olapTable.bfColumns = null;
        }

        olapTable.keysType = this.keysType;
        if (this.relatedMaterializedViews != null) {
            olapTable.relatedMaterializedViews = Sets.newHashSet(this.relatedMaterializedViews);
        }
        if (this.uniqueConstraints != null) {
            olapTable.uniqueConstraints = Lists.newArrayList(this.uniqueConstraints);
        }
        if (this.foreignKeyConstraints != null) {
            olapTable.foreignKeyConstraints = Lists.newArrayList(this.foreignKeyConstraints);
        }
        if (this.partitionInfo != null) {
            olapTable.partitionInfo = (PartitionInfo) this.partitionInfo.clone();
        }
        if (this.defaultDistributionInfo != null) {
            olapTable.defaultDistributionInfo = this.defaultDistributionInfo.copy();
        }
        Map<Long, Partition> idToPartitions = new HashMap<>(this.idToPartition.size());
        Map<String, Partition> nameToPartitions = Maps.newLinkedHashMap();
        for (Map.Entry<Long, Partition> kv : this.idToPartition.entrySet()) {
            Partition copiedPartition = kv.getValue().shallowCopy();
            idToPartitions.put(kv.getKey(), copiedPartition);
            nameToPartitions.put(kv.getValue().getName(), copiedPartition);
        }
        olapTable.idToPartition = idToPartitions;
        olapTable.nameToPartition = nameToPartitions;
        olapTable.physicalPartitionIdToPartitionId = this.physicalPartitionIdToPartitionId;
        olapTable.physicalPartitionNameToPartitionId = this.physicalPartitionNameToPartitionId;
        olapTable.tempPartitions = new TempPartitions();
        for (Partition tempPartition : this.getTempPartitions()) {
            olapTable.tempPartitions.addPartition(tempPartition.shallowCopy());
        }
        olapTable.baseIndexId = this.baseIndexId;
        if (this.tableProperty != null) {
            olapTable.tableProperty = this.tableProperty.copy();
        }

        // Shallow copy shared data to check whether the copied table has changed or not.
        olapTable.lastSchemaUpdateTime = this.lastSchemaUpdateTime;
        olapTable.sessionId = this.sessionId;

        if (this.bfColumns != null) {
            olapTable.bfColumns = Sets.newHashSet(this.bfColumns);
        }
        olapTable.bfFpp = this.bfFpp;
        if (this.curBinlogConfig != null) {
            olapTable.curBinlogConfig = new BinlogConfig(this.curBinlogConfig);
        }
        olapTable.dbName = this.dbName;
    }

    public void addDoubleWritePartition(long sourcePartitionId, long tempPartitionId) {
        doubleWritePartitions.put(sourcePartitionId, tempPartitionId);
    }

    public void clearDoubleWritePartition() {
        doubleWritePartitions.clear();
    }

    public Map<Long, Long> getDoubleWritePartitions() {
        return doubleWritePartitions;
    }

    public boolean hasShardGroupChanged() {
        return isShardGroupChanged.get();
    }

    public void setShardGroupChanged(boolean isShardGroupChanged) {
        this.isShardGroupChanged.set(isShardGroupChanged);
    }

    public boolean isAutomaticBucketing() {
        return isAutomaticBucketing.get();
    }

    public void setAutomaticBucketing(boolean isAutomaticBucketing) {
        this.isAutomaticBucketing.set(isAutomaticBucketing);
    }

    public BinlogConfig getCurBinlogConfig() {
        if (tableProperty != null) {
            return tableProperty.getBinlogConfig();
        }
        return null;
    }

    public void setCurBinlogConfig(BinlogConfig curBinlogConfig) {
        if (tableProperty == null) {
            tableProperty = new TableProperty(Maps.newHashMap());
        }
        tableProperty.modifyTableProperties(curBinlogConfig.toProperties());
        tableProperty.setBinlogConfig(curBinlogConfig);
    }

    public void setFlatJsonConfig(FlatJsonConfig flatJsonConfig) {
        if (tableProperty == null) {
            tableProperty = new TableProperty(Maps.newHashMap());
        }
        tableProperty.modifyTableProperties(flatJsonConfig.toProperties());
        tableProperty.setFlatJsonConfig(flatJsonConfig);
    }

    public FlatJsonConfig getFlatJsonConfig() {
        if (tableProperty != null) {
            return tableProperty.getFlatJsonConfig();
        }
        return null;
    }

    public boolean containsBinlogConfig() {
        if (tableProperty == null ||
                tableProperty.getBinlogConfig() == null ||
                tableProperty.getBinlogConfig().getVersion() == BinlogConfig.INVALID) {
            return false;
        }
        return true;
    }

    public boolean containsFlatJsonConfig() {
        if (tableProperty == null ||
                tableProperty.getFlatJsonConfig() == null) {
            return false;
        }
        return true;
    }

    public long getBinlogTxnId() {
        return binlogTxnId;
    }

    public void setBinlogTxnId(long binlogTxnId) {
        this.binlogTxnId = binlogTxnId;
    }

    public void setTableProperty(TableProperty tableProperty) {
        this.tableProperty = tableProperty;
    }

    public TableProperty getTableProperty() {
        return this.tableProperty;
    }

    public int incAndGetMaxColUniqueId() {
        return this.maxColUniqueId.incrementAndGet();
    }

    public int getMaxColUniqueId() {
        return this.maxColUniqueId.get();
    }

    public void setMaxColUniqueId(int maxColUniqueId) {
        this.maxColUniqueId.set(maxColUniqueId);
    }

    public synchronized long incAndGetMaxIndexId() {
        this.maxIndexId++;
        return this.maxIndexId;
    }

    public long getMaxIndexId() {
        return this.maxIndexId;
    }

    public void setMaxIndexId(long maxIndexId) {
        this.maxIndexId = maxIndexId;
    }

    public boolean dynamicPartitionExists() {
        return tableProperty != null
                && tableProperty.getDynamicPartitionProperty() != null
                && tableProperty.getDynamicPartitionProperty().isExists();
    }

    public void setBaseIndexId(long baseIndexId) {
        this.baseIndexId = baseIndexId;
    }

    public long getBaseIndexId() {
        return baseIndexId;
    }

    public void setState(OlapTableState state) {
        this.state = state;
    }

    public OlapTableState getState() {
        return state;
    }

    public List<Index> getIndexes() {
        if (indexes == null) {
            return Lists.newArrayList();
        }
        return indexes.getIndexes();
    }

    @Override
    public boolean isTemporaryTable() {
        return this.sessionId != null;
    }

    public void checkAndSetName(String newName, boolean onlyCheck) throws DdlException {
        // check if rollup has same name
        for (String idxName : getIndexNameToId().keySet()) {
            if (idxName.equals(newName)) {
                throw new DdlException("New name conflicts with rollup index name: " + idxName);
            }
        }
        if (!onlyCheck) {
            setName(newName);
        }
    }

    public void setName(String newName) {
        // change name in indexNameToId
        if (this.name != null) {
            long baseIndexId = indexNameToId.remove(this.name);
            indexNameToId.put(newName, baseIndexId);
        }

        // change name
        String oldTableName = this.name;
        this.name = newName;

        // change single partition name
        if (this.partitionInfo != null && this.partitionInfo.getType() == PartitionType.UNPARTITIONED) {
            if (getPartitions().stream().findFirst().isPresent()) {
                Optional<Partition> optPartition = getPartitions().stream().findFirst();
                Preconditions.checkState(optPartition.isPresent());
                Partition partition = optPartition.get();
                partition.setName(newName);
                nameToPartition.clear();
                nameToPartition.put(newName, partition);
            }
        }

        // change ExpressionRangePartitionInfo
        if (partitionInfo instanceof ExpressionRangePartitionInfo) {
            ExpressionRangePartitionInfo expressionRangePartitionInfo = (ExpressionRangePartitionInfo) partitionInfo;
            Preconditions.checkState(expressionRangePartitionInfo.getPartitionExprsSize() == 1);
            expressionRangePartitionInfo.renameTableName("", newName);
        }

        if (tableProperty != null) {
            // renew unique constraints
            List<UniqueConstraint> tpUniqueConstraints = tableProperty.getUniqueConstraints();
            if (!CollectionUtils.isEmpty(tpUniqueConstraints)) {
                for (UniqueConstraint constraint : tpUniqueConstraints) {
                    constraint.onTableRename(this, oldTableName);
                }
                setUniqueConstraints(tpUniqueConstraints);
            }
            // renew foreign constraints
            List<ForeignKeyConstraint> tpForeignConstraints = tableProperty.getForeignKeyConstraints();
            if (!CollectionUtils.isEmpty(tpForeignConstraints)) {
                for (ForeignKeyConstraint constraint : tpForeignConstraints) {
                    constraint.onTableRename(this, oldTableName);
                }
                setForeignKeyConstraints(tpForeignConstraints);
            }
        }
    }

    public boolean hasMaterializedIndex(String indexName) {
        return indexNameToId.containsKey(indexName);
    }

    public boolean hasGeneratedColumn() {
        for (Column column : getFullSchema()) {
            if (column.isGeneratedColumn()) {
                return true;
            }
        }
        return false;
    }

    public void setIndexMeta(long indexId, String indexName, List<Column> schema, int schemaVersion,
                             int schemaHash, short shortKeyColumnCount, TStorageType storageType, KeysType keysType) {
        setIndexMeta(indexId, indexName, schema, schemaVersion, schemaHash, shortKeyColumnCount, storageType, keysType,
                null, null);
    }

    public void setIndexMeta(long indexId, String indexName, List<Column> schema, int schemaVersion,
                             int schemaHash, short shortKeyColumnCount, TStorageType storageType, KeysType keysType,
                             OriginStatement origStmt) {
        setIndexMeta(indexId, indexName, schema, schemaVersion, schemaHash, shortKeyColumnCount, storageType, keysType,
                origStmt, null);
    }

    public void setIndexMeta(long indexId, String indexName, List<Column> schema, int schemaVersion,
                             int schemaHash, short shortKeyColumnCount, TStorageType storageType, KeysType keysType,
                             OriginStatement origStmt, List<Integer> sortColumns) {
        setIndexMeta(indexId, indexName, schema, schemaVersion, schemaHash, shortKeyColumnCount, storageType, keysType,
                origStmt, sortColumns, null);
    }

    public void setIndexMeta(long indexId, String indexName, List<Column> schema, int schemaVersion,
                             int schemaHash, short shortKeyColumnCount, TStorageType storageType, KeysType keysType,
                             OriginStatement origStmt, List<Integer> sortColumns, List<Integer> sortColumnUniqueIds) {
        // Nullable when meta comes from schema change log replay.
        // The replay log only save the index id, so we need to get name by id.
        if (indexName == null) {
            indexName = getIndexNameById(indexId);
            Preconditions.checkState(indexName != null);
        }
        // Nullable when meta is less then VERSION_74
        if (keysType == null) {
            keysType = this.keysType;
        }
        // Nullable when meta comes from schema change
        if (storageType == null) {
            MaterializedIndexMeta oldIndexMeta = indexIdToMeta.get(indexId);
            Preconditions.checkState(oldIndexMeta != null);
            storageType = oldIndexMeta.getStorageType();
            Preconditions.checkState(storageType != null);
        } else {
            // The new storage type must be TStorageType.COLUMN
            Preconditions.checkState(storageType == TStorageType.COLUMN || storageType == TStorageType.COLUMN_WITH_ROW);
        }

        MaterializedIndexMeta indexMeta = new MaterializedIndexMeta(indexId, schema, schemaVersion,
                schemaHash, shortKeyColumnCount, storageType, keysType, origStmt, sortColumns,
                sortColumnUniqueIds);
        indexIdToMeta.put(indexId, indexMeta);
        indexNameToId.put(indexName, indexId);
    }

    public boolean hasMaterializedView() {
        Optional<Partition> partition = idToPartition.values().stream().findFirst();
        if (partition.isEmpty()) {
            return false;
        } else {
            PhysicalPartition physicalPartition = partition.get().getDefaultPhysicalPartition();
            return physicalPartition.hasMaterializedView();
        }
    }

    // rebuild the full schema of table
    // the order of columns in fullSchema is meaningless
    public void rebuildFullSchema() {
        List<Column> newFullSchema = new CopyOnWriteArrayList<>();
        Set<String> addedColumnNames = Sets.newTreeSet(String.CASE_INSENSITIVE_ORDER);
        for (Column baseColumn : indexIdToMeta.get(baseIndexId).getSchema()) {
            newFullSchema.add(baseColumn);
            addedColumnNames.add(baseColumn.getName());
        }
        for (MaterializedIndexMeta indexMeta : indexIdToMeta.values()) {
            for (Column column : indexMeta.getSchema()) {
                if (!addedColumnNames.contains(column.getName())) {
                    newFullSchema.add(column);
                    addedColumnNames.add(column.getName());
                }
            }
        }
        fullSchema = newFullSchema;
        updateSchemaIndex();
        // update max column unique id
        int maxColUniqueId = getMaxColUniqueId();
        for (Column column : fullSchema) {
            maxColUniqueId = Math.max(maxColUniqueId, column.getMaxUniqueId());
        }
        setMaxColUniqueId(maxColUniqueId);
        LOG.debug("after rebuild full schema. table {}, schema: {}, max column unique id: {}",
                 name, fullSchema, maxColUniqueId);
    }

    public boolean deleteIndexInfo(String indexName) {
        if (!indexNameToId.containsKey(indexName)) {
            return false;
        }

        long indexId = this.indexNameToId.remove(indexName);
        this.indexIdToMeta.remove(indexId);
        // Some column of deleted index should be removed during `deleteIndexInfo` such
        // as `mv_bitmap_union_c1`
        // If deleted index id == base index id, the schema will not be rebuilt.
        // The reason is that the base index has been removed from indexIdToMeta while
        // the new base index hasn't changed.
        // The schema could not be rebuild in here with error base index id.
        if (indexId != baseIndexId) {
            rebuildFullSchema();
        }
        return true;
    }

    public Map<String, Long> getIndexNameToId() {
        return indexNameToId;
    }

    public Long getIndexIdByName(String indexName) {
        return indexNameToId.get(indexName);
    }

    public String getIndexNameById(long indexId) {
        for (Map.Entry<String, Long> entry : indexNameToId.entrySet()) {
            if (entry.getValue() == indexId) {
                return entry.getKey();
            }
        }
        return null;
    }

    public List<MaterializedIndexMeta> getVisibleIndexMetas() {
        List<MaterializedIndexMeta> visibleMVs = Lists.newArrayList();
        List<MaterializedIndex> mvs = getVisibleIndex();
        for (MaterializedIndex mv : mvs) {
            if (!indexIdToMeta.containsKey(mv.getId())) {
                continue;
            }
            visibleMVs.add(indexIdToMeta.get(mv.getId()));
        }
        return visibleMVs;
    }

    // Fetch the 1th partition's MaterializedViewIndex which should be not used
    // directly.
    private List<MaterializedIndex> getVisibleIndex() {
        Optional<Partition> firstPartition = idToPartition.values().stream().findFirst();
        if (firstPartition.isPresent()) {
            Partition partition = firstPartition.get();
            Optional<PhysicalPartition> firstPhysicalPartition = partition.getSubPartitions().stream().findFirst();
            if (firstPhysicalPartition.isPresent()) {
                PhysicalPartition physicalPartition = firstPhysicalPartition.get();
                return physicalPartition.getMaterializedIndices(IndexExtState.VISIBLE);
            }
        }
        return Lists.newArrayList();
    }

    @Override
    public Column getColumn(ColumnId id) {
        return idToColumn.get(id);
    }

    public Map<ColumnId, Column> getIdToColumn() {
        return idToColumn;
    }

    // this is only for schema change.
    public void renameIndexForSchemaChange(String name, String newName) {
        long idxId = indexNameToId.remove(name);
        indexNameToId.put(newName, idxId);
    }

    public void renameColumnNamePrefix(long idxId) {
        List<Column> columns = indexIdToMeta.get(idxId).getSchema();
        for (Column column : columns) {
            column.setName(Column.removeNamePrefix(column.getName()));
        }
    }

    public void renameColumn(String oldName, String newName) {
        Column column = this.nameToColumn.remove(oldName);
        Preconditions.checkState(column != null, "column of name: " + oldName + " does not exist");
        column.setName(newName);
        this.nameToColumn.put(newName, column);
    }

    public Status resetIdsForRestore(GlobalStateMgr globalStateMgr, Database db, int restoreReplicationNum,
                                     MvRestoreContext mvRestoreContext) {
        // copy an origin index id to name map
        Map<Long, String> origIdxIdToName = Maps.newHashMap();
        for (Map.Entry<String, Long> entry : indexNameToId.entrySet()) {
            origIdxIdToName.put(entry.getValue(), entry.getKey());
        }

        // reset table id
        setId(globalStateMgr.getNextId());

        // reset all 'indexIdToXXX' map
        Map<Long, MaterializedIndexMeta> origIndexIdToMeta = Maps.newHashMap(indexIdToMeta);
        indexIdToMeta.clear();
        for (Map.Entry<Long, String> entry : origIdxIdToName.entrySet()) {
            long newIdxId = globalStateMgr.getNextId();
            if (entry.getValue().equals(name)) {
                // base index
                baseIndexId = newIdxId;
            }
            MaterializedIndexMeta indexMeta = origIndexIdToMeta.get(entry.getKey());
            indexMeta.setIndexIdForRestore(newIdxId);
            indexMeta.setSchemaId(newIdxId);
            indexIdToMeta.put(newIdxId, indexMeta);
            indexNameToId.put(entry.getValue(), newIdxId);
        }

        // generate a partition old id to new id map
        Map<Long, Long> partitionOldIdToNewId = Maps.newHashMap();
        for (Long id : idToPartition.keySet()) {
            partitionOldIdToNewId.put(id, globalStateMgr.getNextId());
            // reset replication number for partition info
            partitionInfo.setReplicationNum(id, (short) restoreReplicationNum);
        }

        // reset partition info
        partitionInfo.setPartitionIdsForRestore(partitionOldIdToNewId);

        // reset partitions
        List<Partition> partitions = Lists.newArrayList(idToPartition.values());
        idToPartition.clear();
        physicalPartitionIdToPartitionId.clear();
        physicalPartitionNameToPartitionId.clear();
        for (Partition partition : partitions) {
            long newPartitionId = partitionOldIdToNewId.get(partition.getId());
            partition.setIdForRestore(newPartitionId);
            idToPartition.put(newPartitionId, partition);
            List<PhysicalPartition> origPhysicalPartitions = Lists.newArrayList(partition.getSubPartitions());
            origPhysicalPartitions.forEach(physicalPartition -> {
                // after refactor, the first physicalPartition id is different from logical partition id
                if (physicalPartition.getParentId() != newPartitionId) {
                    partition.removeSubPartition(physicalPartition.getId());
                }
            });
            origPhysicalPartitions.forEach(physicalPartition -> {
                // after refactor, the first physicalPartition id is different from logical partition id
                if (physicalPartition.getParentId() != newPartitionId) {
                    physicalPartition.setIdForRestore(GlobalStateMgr.getCurrentState().getNextId());
                    physicalPartition.setParentId(newPartitionId);
                    partition.addSubPartition(physicalPartition);
                }
                physicalPartitionIdToPartitionId.put(physicalPartition.getId(), newPartitionId);
                physicalPartitionNameToPartitionId.put(physicalPartition.getName(), newPartitionId);
            });
        }

        // reset replication number for olaptable
        setReplicationNum((short) restoreReplicationNum);

        // for each partition, reset rollup index map
        for (Map.Entry<Long, Partition> entry : idToPartition.entrySet()) {
            Partition partition = entry.getValue();
            for (PhysicalPartition physicalPartition : partition.getSubPartitions()) {
                Map<Long, MaterializedIndex> origIdToIndex = Maps.newHashMapWithExpectedSize(origIdxIdToName.size());
                for (Map.Entry<Long, String> entry2 : origIdxIdToName.entrySet()) {
                    MaterializedIndex idx = physicalPartition.getIndex(entry2.getKey());
                    origIdToIndex.put(entry2.getKey(), idx);
                    long newIdxId = indexNameToId.get(entry2.getValue());
                    if (newIdxId != baseIndexId) {
                        // not base table, delete old index
                        physicalPartition.deleteRollupIndex(entry2.getKey());
                    }
                }
                for (Map.Entry<Long, String> entry2 : origIdxIdToName.entrySet()) {
                    MaterializedIndex idx = origIdToIndex.get(entry2.getKey());
                    long newIdxId = indexNameToId.get(entry2.getValue());
                    int schemaHash = indexIdToMeta.get(newIdxId).getSchemaHash();
                    idx.setIdForRestore(newIdxId);
                    if (newIdxId != baseIndexId) {
                        // not base table, reset
                        physicalPartition.createRollupIndex(idx);
                    }

                    // generate new tablets in origin tablet order
                    int tabletNum = idx.getTablets().size();
                    idx.clearTabletsForRestore();
                    Status status = createTabletsForRestore(tabletNum, idx, globalStateMgr,
                            partitionInfo.getReplicationNum(entry.getKey()), physicalPartition.getVisibleVersion(),
                            schemaHash, physicalPartition.getId(), db);
                    if (!status.ok()) {
                        return status;
                    }
                }
            }
        }

        return Status.OK;
    }

    public Status createTabletsForRestore(int tabletNum, MaterializedIndex index, GlobalStateMgr globalStateMgr,
                                          int replicationNum, long version, int schemaHash,
                                          long partitionId, Database db) {
        Preconditions.checkArgument(replicationNum > 0);
        boolean isColocate = (this.colocateGroup != null && !this.colocateGroup.isEmpty() && db != null);

        if (isColocate) {
            try {
                isColocate = GlobalStateMgr.getCurrentState().getColocateTableIndex()
                                            .addTableToGroup(db, this, this.colocateGroup, false);
            } catch (Exception e) {
                return new Status(ErrCode.COMMON_ERROR,
                    "check colocate restore failed, errmsg: " + e.getMessage() +
                    ", you can disable colocate restore by turn off Config.enable_colocate_restore");
            }
        }

        if (isColocate) {
            ColocateTableIndex colocateTableIndex = GlobalStateMgr.getCurrentState().getColocateTableIndex();
            ColocateTableIndex.GroupId groupId = colocateTableIndex.getGroup(this.id);
            List<List<Long>> backendsPerBucketSeq = colocateTableIndex.getBackendsPerBucketSeq(groupId);
            boolean chooseBackendsArbitrary = backendsPerBucketSeq == null || backendsPerBucketSeq.isEmpty();

            if (chooseBackendsArbitrary) {
                backendsPerBucketSeq = Lists.newArrayList();
            }

            for (int i = 0; i < tabletNum; ++i) {
                long newTabletId = globalStateMgr.getNextId();
                LocalTablet newTablet = new LocalTablet(newTabletId);
                index.addTablet(newTablet, null /* tablet meta */, false/* update inverted index */);

                // replicas
                List<Long> beIds;
                if (chooseBackendsArbitrary) {
                    // This is the first colocate table in the group, or just a normal table,
                    // randomly choose backends
                    beIds = GlobalStateMgr.getCurrentState().getNodeMgr()
                        .getClusterInfo().getNodeSelector().seqChooseBackendIds(replicationNum, true, true, getLocation());
                    backendsPerBucketSeq.add(beIds);
                } else {
                    // get backends from existing backend sequence
                    beIds = backendsPerBucketSeq.get(i);
                }

                if (CollectionUtils.isEmpty(beIds)) {
                    return new Status(ErrCode.COMMON_ERROR, "failed to find "
                            + replicationNum
                            + " different hosts to create table: " + name);
                }
                for (Long beId : beIds) {
                    long newReplicaId = globalStateMgr.getNextId();
                    Replica replica = new Replica(newReplicaId, beId, ReplicaState.NORMAL,
                            version, schemaHash);
                    newTablet.addReplica(replica, false/* update inverted index */);
                }
                Preconditions.checkState(beIds.size() == replicationNum,
                                         beIds.size() + " vs. " + replicationNum);
            }

            // first colocate table in CG
            if (groupId != null && chooseBackendsArbitrary) {
                colocateTableIndex.addBackendsPerBucketSeq(groupId, backendsPerBucketSeq);
            }
        } else {
            for (int i = 0; i < tabletNum; i++) {
                long newTabletId = globalStateMgr.getNextId();
                LocalTablet newTablet = new LocalTablet(newTabletId);
                index.addTablet(newTablet, null /* tablet meta */, false/* update inverted index */);

                // replicas
                List<Long> beIds = GlobalStateMgr.getCurrentState().getNodeMgr()
                        .getClusterInfo().getNodeSelector().seqChooseBackendIds(replicationNum, true, true, getLocation());
                if (CollectionUtils.isEmpty(beIds)) {
                    return new Status(ErrCode.COMMON_ERROR, "failed to find "
                            + replicationNum
                            + " different hosts to create table: " + name);
                }
                for (Long beId : beIds) {
                    long newReplicaId = globalStateMgr.getNextId();
                    Replica replica = new Replica(newReplicaId, beId, ReplicaState.NORMAL,
                            version, schemaHash);
                    newTablet.addReplica(replica, false/* update inverted index */);
                }
            }
        }
        return Status.OK;
    }

    public Status doAfterRestore(MvRestoreContext mvRestoreContext) throws DdlException {
        if (relatedMaterializedViews == null || relatedMaterializedViews.isEmpty()) {
            return Status.OK;
        }

        Map<MvId, MvBackupInfo> mvIdTableNameMap = mvRestoreContext.getMvIdToTableNameMap();
        for (MvId mvId : relatedMaterializedViews) {
            // Find the associated mv if possible
            MvBackupInfo mvBackupInfo = mvIdTableNameMap.get(mvId);
            if (mvBackupInfo == null) {
                continue;
            }
            MvId localMvId = mvBackupInfo.getLocalMvId();
            if (localMvId == null) {
                continue;
            }
            Database mvDb = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(localMvId.getDbId());
            if (mvDb == null) {
                continue;
            }
            Table mvTable = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(mvDb.getId(), localMvId.getId());
            if (mvTable == null) {
                continue;
            }
            if (!mvTable.isMaterializedView()) {
                LOG.warn("Base table {} related materialized view {} is not a mv, local mvId:{}, remote mvId:{}",
                        this.name, mvTable.getName(), localMvId, mvId);
                continue;
            }
            MaterializedView mv = (MaterializedView) mvTable;
            // Do this no matter whether mv is active or not to restore version map for mv rewrite.
            mv.doAfterRestore(mvRestoreContext);
        }
        return Status.OK;
    }

    public Map<Long, MaterializedIndexMeta> getIndexIdToMeta() {
        return indexIdToMeta;
    }

    public Map<Long, MaterializedIndexMeta> getCopiedIndexIdToMeta() {
        return new HashMap<>(indexIdToMeta);
    }

    public MaterializedIndexMeta getIndexMetaByIndexId(long indexId) {
        return indexIdToMeta.get(indexId);
    }

    public List<Long> getIndexIdListExceptBaseIndex() {
        List<Long> result = Lists.newArrayList();
        for (Long indexId : indexIdToMeta.keySet()) {
            if (indexId != baseIndexId) {
                result.add(indexId);
            }
        }
        return result;
    }

    // schema
    public Map<Long, List<Column>> getIndexIdToSchema() {
        Map<Long, List<Column>> result = Maps.newHashMap();
        for (Map.Entry<Long, MaterializedIndexMeta> entry : indexIdToMeta.entrySet()) {
            result.put(entry.getKey(), entry.getValue().getSchema());
        }
        return result;
    }

    public List<Column> getSchemaByIndexId(Long indexId) {
        MaterializedIndexMeta meta = indexIdToMeta.get(indexId);
        if (meta != null) {
            return meta.getSchema();
        }
        return new ArrayList<Column>();
    }

    /**
     * NOTE: The result key columns are not in the creating order because `nameToColumn`
     * uses unordered hashmap to keeps name to column's mapping.
     */
    public List<Column> getKeyColumns() {
        return getColumns().stream().filter(Column::isKey).collect(Collectors.toList());
    }

    public List<Column> getKeyColumnsInOrder() {
        return getFullSchema().stream().filter(Column::isKey).collect(Collectors.toList());
    }

    public List<Column> getKeyColumnsByIndexId(Long indexId) {
        ArrayList<Column> keyColumns = Lists.newArrayList();
        List<Column> allColumns = this.getSchemaByIndexId(indexId);
        for (Column column : allColumns) {
            if (column.isKey()) {
                keyColumns.add(column);
            }
        }

        return keyColumns;
    }

    // schemaHash
    public Map<Long, Integer> getIndexIdToSchemaHash() {
        Map<Long, Integer> result = Maps.newHashMap();
        for (Map.Entry<Long, MaterializedIndexMeta> entry : indexIdToMeta.entrySet()) {
            result.put(entry.getKey(), entry.getValue().getSchemaHash());
        }
        return result;
    }

    public int getSchemaHashByIndexId(Long indexId) {
        MaterializedIndexMeta indexMeta = indexIdToMeta.get(indexId);
        if (indexMeta == null) {
            return -1;
        }
        return indexMeta.getSchemaHash();
    }

    public TStorageType getStorageTypeByIndexId(Long indexId) {
        MaterializedIndexMeta indexMeta = indexIdToMeta.get(indexId);
        if (indexMeta == null) {
            return TStorageType.COLUMN;
        }
        return indexMeta.getStorageType();
    }

    public KeysType getKeysType() {
        return keysType;
    }

    public KeysType getKeysTypeByIndexId(long indexId) {
        MaterializedIndexMeta indexMeta = indexIdToMeta.get(indexId);
        Preconditions.checkNotNull(indexMeta, "index id:" + indexId + " meta is null");
        return indexMeta.getKeysType();
    }

    public PartitionInfo getPartitionInfo() {
        return partitionInfo;
    }

    public void sendDropAutoIncrementMapTask() {
        Set<Long> fullBackendId = Sets.newHashSet();
        for (Partition partition : this.getAllPartitions()) {
            for (PhysicalPartition physicalPartition : partition.getSubPartitions()) {
                List<MaterializedIndex> allIndices = physicalPartition
                        .getMaterializedIndices(MaterializedIndex.IndexExtState.ALL);
                for (MaterializedIndex materializedIndex : allIndices) {
                    for (Tablet tablet : materializedIndex.getTablets()) {
                        Set<Long> backendIds = tablet.getBackendIds();
                        for (long backendId : backendIds) {
                            fullBackendId.add(backendId);
                        }
                    }
                }
            }
        }

        AgentBatchTask batchTask = new AgentBatchTask();

        for (long backendId : fullBackendId) {
            DropAutoIncrementMapTask dropAutoIncrementMapTask = new DropAutoIncrementMapTask(backendId, this.id,
                    GlobalStateMgr.getCurrentState().getNextId());
            batchTask.addTask(dropAutoIncrementMapTask);
        }

        if (batchTask.getTaskNum() > 0) {
            MarkedCountDownLatch<Long, Long> latch = new MarkedCountDownLatch<>(batchTask.getTaskNum());
            for (AgentTask task : batchTask.getAllTasks()) {
                latch.addMark(task.getBackendId(), -1L);
                ((DropAutoIncrementMapTask) task).setLatch(latch);
                AgentTaskQueue.addTask(task);
            }
            AgentTaskExecutor.submit(batchTask);

            // estimate timeout, at most 10 min
            long timeout = 60L * 1000L;
            boolean ok = false;
            try {
                LOG.info("begin to send drop auto increment map tasks to BE, total {} tasks. timeout: {}",
                        batchTask.getTaskNum(), timeout);
                ok = latch.await(timeout, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                LOG.warn("InterruptedException: ", e);
            }

            if (!ok) {
                LOG.warn("drop auto increment map tasks failed");
            }

        }
    }

    /**
     * @return : table's partition name to range partition key mapping.
     */
    public Map<String, Range<PartitionKey>> getRangePartitionMap() {
        Preconditions.checkState(partitionInfo instanceof RangePartitionInfo);
        RangePartitionInfo rangePartitionInfo = (RangePartitionInfo) partitionInfo;
        Map<String, Range<PartitionKey>> rangePartitionMap = Maps.newHashMap();
        for (Map.Entry<Long, Partition> partitionEntry : idToPartition.entrySet()) {
            Long partitionId = partitionEntry.getKey();
            String partitionName = partitionEntry.getValue().getName();
            // FE and BE at the same time ignore the hidden partition at the same time
            if (partitionName.startsWith(ExpressionRangePartitionInfo.SHADOW_PARTITION_PREFIX)) {
                continue;
            }
            rangePartitionMap.put(partitionName, rangePartitionInfo.getRange(partitionId));
        }
        return rangePartitionMap;
    }

    /**
     * NOTE: Only list partitioned olap table can call this method.
     * @return : table's partition name to all list partition cells.
     * eg:
     *  partition columns   : (a, b, c)
     *  values              : [[1, 2, 3], [4, 5, 6]]
     */
    public Map<String, PListCell> getListPartitionItems() {
        return getListPartitionItems(Optional.empty());
    }

    /**
     * Return the partition items of the selected partition columns if selectedPartitionCols is not null, otherwise return
     * all partition items.
     */
    public Map<String, PListCell> getListPartitionItems(Optional<List<Column>> selectedPartitionCols) {
        Preconditions.checkState(partitionInfo instanceof ListPartitionInfo, "partition info is not list partition");
        ListPartitionInfo listPartitionInfo = (ListPartitionInfo) partitionInfo;
        List<Column> partitionCols = listPartitionInfo.getPartitionColumns(this.idToColumn);
        List<Integer> colIdxes = Lists.newArrayList();
        if (selectedPartitionCols.isPresent()) {
            Preconditions.checkState(!selectedPartitionCols.isEmpty(), "selected partition columns is empty");
            Preconditions.checkState(partitionCols.size() >= selectedPartitionCols.get().size(),
                    "selected partition columns size is larger than partition columns size");
            for (Column select : selectedPartitionCols.get()) {
                int idx = partitionCols.indexOf(select);
                if (idx == -1) {
                    throw new SemanticException("column " + select.getName() + " is not partition column");
                }
                colIdxes.add(idx);
            }
        }
        Map<String, PListCell> partitionItems = Maps.newHashMap();
        for (Map.Entry<Long, Partition> partitionEntry : idToPartition.entrySet()) {
            Long partitionId = partitionEntry.getKey();
            String partitionName = partitionEntry.getValue().getName();
            // FE and BE at the same time ignore the hidden partition at the same time
            if (partitionName.startsWith(ExpressionRangePartitionInfo.SHADOW_PARTITION_PREFIX)) {
                continue;
            }
            // one item
            List<LiteralExpr> literalValues = listPartitionInfo.getLiteralExprValues().get(partitionId);
            if (CollectionUtils.isNotEmpty(literalValues)) {
                List<List<String>> cellValue = Lists.newArrayList();
                // for one item(single value), treat it as multi values.
                for (LiteralExpr val : literalValues) {
                    cellValue.add(Lists.newArrayList(val.getStringValue()));
                }
                partitionItems.put(partitionName, new PListCell(cellValue));
            }

            // multi items
            List<List<LiteralExpr>> multiExprValues = listPartitionInfo.getMultiLiteralExprValues().get(partitionId);
            if (CollectionUtils.isNotEmpty(multiExprValues)) {
                List<List<String>> multiValues = Lists.newArrayList();
                for (List<LiteralExpr> exprValues : multiExprValues) {
                    List<String> values = Lists.newArrayList();
                    if (CollectionUtils.isEmpty(colIdxes)) {
                        for (LiteralExpr literalExpr : exprValues) {
                            values.add(literalExpr.getStringValue());
                        }
                    } else {
                        for (int idx : colIdxes) {
                            if (idx >= 0 && idx < exprValues.size()) {
                                values.add(exprValues.get(idx).getStringValue());
                            } else {
                                // print index and exprValues
                                throw new SemanticException("Invalid column index during partition processing. " +
                                        "Index: " + idx + ", ExprValues: " + exprValues);
                            }
                        }
                    }
                    multiValues.add(values);
                }
                partitionItems.put(partitionName, new PListCell(multiValues));
            }
        }
        return partitionItems;
    }

    @Override
    public List<Column> getPartitionColumns() {
        return partitionInfo.getPartitionColumns(this.idToColumn);
    }

    @Override
    public List<String> getPartitionColumnNames() {
        List<String> partitionColumnNames = Lists.newArrayList();
        if (partitionInfo.isUnPartitioned()) {
            return partitionColumnNames;
        } else if (partitionInfo.isRangePartition()) {
            List<Column> partitionColumns = partitionInfo.getPartitionColumns(this.idToColumn);
            for (Column column : partitionColumns) {
                partitionColumnNames.add(column.getName());
            }
            return partitionColumnNames;
        } else if (partitionInfo.isListPartition()) {
            List<Column> partitionColumns = partitionInfo.getPartitionColumns(this.idToColumn);
            for (Column column : partitionColumns) {
                partitionColumnNames.add(column.getName());
            }
            return partitionColumnNames;
        }
        throw new SemanticException("unknown partition info:" + partitionInfo.getClass().getName());
    }

    public void setDefaultDistributionInfo(DistributionInfo distributionInfo) {
        defaultDistributionInfo = distributionInfo;
    }

    public DistributionInfo getDefaultDistributionInfo() {
        return defaultDistributionInfo;
    }

    /*
     * Infer the distribution info based on partitions and cluster status
     */
    public void inferDistribution(DistributionInfo info) throws DdlException {
        if (info.getType() == DistributionInfo.DistributionInfoType.HASH) {
            // infer bucket num
            if (info.getBucketNum() == 0) {
                int numBucket = CatalogUtils.calAvgBucketNumOfRecentPartitions(this,
                        5, Config.enable_auto_tablet_distribution);
                info.setBucketNum(numBucket);
            }
        } else if (info.getType() == DistributionInfo.DistributionInfoType.RANDOM) {
            // prior to use user set mutable bucket num
            long numBucket = getMutableBucketNum();
            if (numBucket > 0) {
                info.setBucketNum((int) numBucket);
            } else if (info.getBucketNum() == 0) {
                numBucket = CatalogUtils.calPhysicalPartitionBucketNum();
                info.setBucketNum((int) numBucket);
            }
        } else {
            throw new DdlException("Unknown distribution info type: " + info.getType());
        }
    }

    public void optimizeDistribution(DistributionInfo info, Partition partition) throws DdlException {
        long bucketNum = (partition.getDataSize() / (1024 * 1024 * 1024)) + 1;
        info.setBucketNum((int) bucketNum);
    }

    @Override
    public Set<String> getDistributionColumnNames() {
        Set<String> distributionColumnNames = Sets.newHashSet();
        if (defaultDistributionInfo instanceof RandomDistributionInfo) {
            return distributionColumnNames;
        }
        HashDistributionInfo hashDistributionInfo = (HashDistributionInfo) defaultDistributionInfo;
        List<Column> partitionColumns = MetaUtils.getColumnsByColumnIds(
                this, hashDistributionInfo.getDistributionColumns());
        for (Column column : partitionColumns) {
            distributionColumnNames.add(column.getName().toLowerCase());
        }
        return distributionColumnNames;
    }

    public void renamePartition(String partitionName, String newPartitionName) {
        if (partitionInfo.getType() == PartitionType.UNPARTITIONED) {
            Optional<Partition> optionalPartition = idToPartition.values().stream().findFirst();
            if (optionalPartition.isPresent()) {
                Partition partition = optionalPartition.get();
                partition.setName(newPartitionName);
                nameToPartition.clear();
                nameToPartition.put(newPartitionName, partition);
                LOG.info("rename partition {} in table {}", newPartitionName, name);
            }
        } else {
            Partition partition = nameToPartition.remove(partitionName);
            partition.setName(newPartitionName);
            nameToPartition.put(newPartitionName, partition);
        }
    }

    public void addPartition(Partition partition) {
        idToPartition.put(partition.getId(), partition);
        nameToPartition.put(partition.getName(), partition);
        for (PhysicalPartition physicalPartition : partition.getSubPartitions()) {
            physicalPartitionIdToPartitionId.put(physicalPartition.getId(), partition.getId());
            physicalPartitionNameToPartitionId.put(physicalPartition.getName(), partition.getId());
        }
    }

    public void removePhysicalPartition(PhysicalPartition physicalPartition) {
        physicalPartitionIdToPartitionId.remove(physicalPartition.getId());
        physicalPartitionNameToPartitionId.remove(physicalPartition.getName());
    }

    public void addPhysicalPartition(PhysicalPartition physicalPartition) {
        physicalPartitionIdToPartitionId.put(physicalPartition.getId(), physicalPartition.getParentId());
        physicalPartitionNameToPartitionId.put(physicalPartition.getName(), physicalPartition.getParentId());
    }

    // This is a private method.
    // Call public "dropPartitionAndReserveTablet" and "dropPartition"
    private void dropPartition(long dbId, String partitionName, boolean isForceDrop, boolean reserveTablets) {
        Partition partition = nameToPartition.get(partitionName);
        if (partition == null) {
            return;
        }

        if (!reserveTablets) {
            RecyclePartitionInfo recyclePartitionInfo = buildRecyclePartitionInfo(dbId, partition);
            recyclePartitionInfo.setRecoverable(!isForceDrop);
            GlobalStateMgr.getCurrentState().getRecycleBin().recyclePartition(recyclePartitionInfo);
        }

        partitionInfo.dropPartition(partition.getId());
        idToPartition.remove(partition.getId());
        nameToPartition.remove(partitionName);
        physicalPartitionIdToPartitionId.keySet().removeAll(partition.getSubPartitions()
                .stream().map(PhysicalPartition::getId)
                .collect(Collectors.toList()));
        physicalPartitionNameToPartitionId.keySet().removeAll(partition.getSubPartitions()
                .stream().map(PhysicalPartition::getName)
                .collect(Collectors.toList()));
    }

    protected RecyclePartitionInfo buildRecyclePartitionInfo(long dbId, Partition partition) {
        if (partitionInfo.isRangePartition()) {
            RangePartitionInfo rangePartitionInfo = (RangePartitionInfo) partitionInfo;
            return new RecycleRangePartitionInfo(dbId, id, partition,
                    rangePartitionInfo.getRange(partition.getId()),
                    rangePartitionInfo.getDataProperty(partition.getId()),
                    rangePartitionInfo.getReplicationNum(partition.getId()),
                    rangePartitionInfo.getIsInMemory(partition.getId()),
                    rangePartitionInfo.getDataCacheInfo(partition.getId()));
        } else if (partitionInfo.isListPartition()) {
            return new RecycleListPartitionInfo(dbId, id, partition,
                    partitionInfo.getDataProperty(partition.getId()),
                    partitionInfo.getReplicationNum(partition.getId()),
                    partitionInfo.getIsInMemory(partition.getId()),
                    partitionInfo.getDataCacheInfo(partition.getId()));
        } else if (partitionInfo.isUnPartitioned()) {
            return new RecycleUnPartitionInfo(dbId, id, partition,
                    partitionInfo.getDataProperty(partition.getId()),
                    partitionInfo.getReplicationNum(partition.getId()),
                    partitionInfo.getIsInMemory(partition.getId()),
                    partitionInfo.getDataCacheInfo(partition.getId()));
        } else {
            throw new RuntimeException("Unknown partition type: " + partitionInfo.getType());
        }
    }

    public void dropPartitionAndReserveTablet(String partitionName) {
        // reserveTablets is true and partition is not recycled, so dbId -1 is ok.
        dropPartition(-1, partitionName, true, true);
    }

    public void dropPartition(long dbId, String partitionName, boolean isForceDrop) {
        dropPartition(dbId, partitionName, isForceDrop, false);
    }

    // check input partition has temporary partition
    public boolean inputHasTempPartition(List<Long> partitionIds) {
        for (Long pid : partitionIds) {
            if (tempPartitions.getPartition(pid) != null) {
                return true;
            }
        }
        return false;
    }

    /*
     * A table may contain both formal and temporary partitions.
     * There are several methods to get the partition of a table.
     * Typically divided into two categories:
     *
     * 1. Get partition by id
     * 2. Get partition by name
     *
     * According to different requirements, the caller may want to obtain
     * a formal partition or a temporary partition. These methods are
     * described below in order to obtain the partition by using the correct method.
     *
     * 1. Get by name
     *
     * This type of request usually comes from a user with partition names. Such as
     * `select * from tbl partition(p1);`.
     * This type of request has clear information to indicate whether to obtain a
     * formal or temporary partition.
     * Therefore, we need to get the partition through this method:
     *
     * `getPartition(String partitionName, boolean isTemp)`
     *
     * To avoid modifying too much code, we leave the `getPartition(String
     * partitionName)`, which is same as:
     *
     * `getPartition(partitionName, false)`
     *
     * 2. Get by id
     *
     * This type of request usually means that the previous step has obtained
     * certain partition ids in some way,
     * so we only need to get the corresponding partition through this method:
     *
     * `getPartition(long partitionId)`.
     *
     * This method will try to get both formal partitions and temporary partitions.
     *
     * 3. Get all partition instances
     *
     * Depending on the requirements, the caller may want to obtain all formal
     * partitions,
     * all temporary partitions, or all partitions. Therefore we provide 3 methods,
     * the caller chooses according to needs.
     *
     * `getPartitions()`
     * `getTempPartitions()`
     * `getAllPartitions()`
     *
     */

    // get partition by name, not including temp partitions
    @Override
    public Partition getPartition(String partitionName) {
        return getPartition(partitionName, false);
    }

    // get partition by name
    public Partition getPartition(String partitionName, boolean isTempPartition) {
        if (isTempPartition) {
            return tempPartitions.getPartition(partitionName);
        } else {
            return nameToPartition.get(partitionName);
        }
    }

    // get partition by id, including temp partitions
    public Partition getPartition(long partitionId) {
        Partition partition = idToPartition.get(partitionId);
        if (partition == null) {
            partition = tempPartitions.getPartition(partitionId);
        }
        return partition;
    }

    public Optional<Partition> mayGetPartition(long partitionId) {
        return Optional.ofNullable(getPartition(partitionId));
    }

    public PhysicalPartition getPhysicalPartition(long physicalPartitionId) {
        Long partitionId = physicalPartitionIdToPartitionId.get(physicalPartitionId);
        if (partitionId == null) {
            for (Partition partition : tempPartitions.getAllPartitions()) {
                for (PhysicalPartition subPartition : partition.getSubPartitions()) {
                    if (subPartition.getId() == physicalPartitionId) {
                        return subPartition;
                    }
                }
            }
            for (Partition partition : idToPartition.values()) {
                for (PhysicalPartition subPartition : partition.getSubPartitions()) {
                    if (subPartition.getId() == physicalPartitionId) {
                        return subPartition;
                    }
                }
            }
        } else {
            Partition partition = getPartition(partitionId);
            if (partition != null) {
                return partition.getSubPartition(physicalPartitionId);
            }
        }

        return null;
    }

    public PhysicalPartition getPhysicalPartition(String physicalPartitionName) {
        Long partitionId = physicalPartitionNameToPartitionId.get(physicalPartitionName);
        if (partitionId == null) {
            for (Partition partition : idToPartition.values()) {
                for (PhysicalPartition subPartition : partition.getSubPartitions()) {
                    if (subPartition.getName().equals(physicalPartitionName)) {
                        return subPartition;
                    }
                }
            }
            for (Partition partition : tempPartitions.getAllPartitions()) {
                for (PhysicalPartition subPartition : partition.getSubPartitions()) {
                    if (subPartition.getName().equals(physicalPartitionName)) {
                        return subPartition;
                    }
                }
            }
        } else {
            Partition partition = getPartition(partitionId);
            if (partition != null) {
                return partition.getSubPartition(physicalPartitionName);
            }
        }

        return null;
    }

    public Collection<PhysicalPartition> getPhysicalPartitions() {
        return idToPartition.values().stream()
                .flatMap(partition -> partition.getSubPartitions().stream())
                .collect(Collectors.toList());
    }

    public Collection<PhysicalPartition> getAllPhysicalPartitions() {
        List<PhysicalPartition> physicalPartitions = idToPartition.values().stream()
                .flatMap(partition -> partition.getSubPartitions().stream()).collect(Collectors.toList());
        physicalPartitions.addAll(tempPartitions.getAllPartitions().stream()
                .flatMap(partition -> partition.getSubPartitions().stream()).collect(Collectors.toList()));
        return physicalPartitions;
    }

    // get all partitions except temp partitions
    @Override
    public Collection<Partition> getPartitions() {
        return idToPartition.values();
    }

    /**
     * Return all visible partitions except shadow partitions.
     */
    public List<Partition> getVisiblePartitions() {
        return nameToPartition.entrySet().stream()
                .filter(e -> !e.getKey().startsWith(ExpressionRangePartitionInfo.SHADOW_PARTITION_PREFIX))
                .map(e -> e.getValue())
                .collect(Collectors.toList());
    }

    public List<Partition> getNonEmptyPartitions() {
        return idToPartition.values().stream().filter(Partition::hasData).collect(Collectors.toList());
    }

    public int getNumberOfPartitions() {
        return idToPartition.size();
    }

    // get only temp partitions
    public Collection<Partition> getTempPartitions() {
        return tempPartitions.getAllPartitions();
    }

    // get all partitions including temp partitions
    public Collection<Partition> getAllPartitions() {
        List<Partition> partitions = Lists.newArrayList(idToPartition.values());
        partitions.addAll(tempPartitions.getAllPartitions());
        return partitions;
    }

    public List<Long> getAllPartitionIds() {
        return new ArrayList<>(idToPartition.keySet());
    }

    public Collection<Partition> getRecentPartitions(int recentPartitionNum) {
        List<Partition> partitions = Lists.newArrayList(idToPartition.values());
        Collections.sort(partitions, new Comparator<Partition>() {
            @Override
            public int compare(Partition h1, Partition h2) {
                return (int) (h2.getDefaultPhysicalPartition().getVisibleVersion()
                        - h1.getDefaultPhysicalPartition().getVisibleVersion());
            }
        });
        return partitions.subList(0, recentPartitionNum);
    }

    // get all partitions' name except the temp partitions
    public Set<String> getPartitionNames() {
        return Sets.newHashSet(nameToPartition.keySet());
    }

    /**
     * Return all visible partition names which exclude all shadow partitions.
     */
    public Set<String> getVisiblePartitionNames() {
        return nameToPartition.keySet().stream()
                .filter(n -> !n.startsWith(ExpressionRangePartitionInfo.SHADOW_PARTITION_PREFIX))
                .collect(Collectors.toSet());
    }

    public Map<String, Range<PartitionKey>> getValidRangePartitionMap(int lastPartitionNum) throws AnalysisException {
        Map<String, Range<PartitionKey>> rangePartitionMap = getRangePartitionMap();
        // less than 0 means not set
        if (lastPartitionNum < 0) {
            return rangePartitionMap;
        }

        int partitionNum = rangePartitionMap.size();
        if (lastPartitionNum > partitionNum) {
            return rangePartitionMap;
        }

        List<Column> partitionColumns = partitionInfo.getPartitionColumns(this.idToColumn);
        Column partitionColumn = partitionColumns.get(0);
        Type partitionType = partitionColumn.getType();

        List<Range<PartitionKey>> sortedRange = rangePartitionMap.values().stream()
                .sorted(RangeUtils.RANGE_COMPARATOR).collect(Collectors.toList());
        int startIndex;
        if (partitionType.isNumericType()) {
            startIndex = partitionNum - lastPartitionNum;
        } else if (partitionType.isDateType()) {
            LocalDateTime currentDateTime = LocalDateTime.now();
            PartitionValue currentPartitionValue = new PartitionValue(
                    currentDateTime.format(DateUtils.DATE_FORMATTER_UNIX));
            PartitionKey currentPartitionKey = PartitionKey.createPartitionKey(
                    ImmutableList.of(currentPartitionValue), partitionColumns);
            // For date types, ttl number should not consider future time
            int futurePartitionNum = 0;
            for (int i = sortedRange.size(); i > 0; i--) {
                PartitionKey lowerEndpoint = sortedRange.get(i - 1).lowerEndpoint();
                if (lowerEndpoint.compareTo(currentPartitionKey) > 0) {
                    futurePartitionNum++;
                } else {
                    break;
                }
            }

            if (partitionNum - lastPartitionNum - futurePartitionNum <= 0) {
                return rangePartitionMap;
            } else {
                startIndex = partitionNum - lastPartitionNum - futurePartitionNum;
            }
        } else {
            throw new AnalysisException("Unsupported partition type: " + partitionType);
        }

        PartitionKey lowerEndpoint = sortedRange.get(startIndex).lowerEndpoint();
        PartitionKey upperEndpoint = sortedRange.get(partitionNum - 1).upperEndpoint();
        String start = AnalyzerUtils.parseLiteralExprToDateString(lowerEndpoint, 0);
        String end = AnalyzerUtils.parseLiteralExprToDateString(upperEndpoint, 0);

        Map<String, Range<PartitionKey>> result = Maps.newHashMap();
        Range<PartitionKey> rangeToInclude = SyncPartitionUtils.createRange(start, end, partitionColumn);
        for (Map.Entry<String, Range<PartitionKey>> entry : rangePartitionMap.entrySet()) {
            Range<PartitionKey> rangeToCheck = entry.getValue();
            int lowerCmp = rangeToInclude.lowerEndpoint().compareTo(rangeToCheck.upperEndpoint());
            int upperCmp = rangeToInclude.upperEndpoint().compareTo(rangeToCheck.lowerEndpoint());
            if (!(lowerCmp >= 0 || upperCmp <= 0)) {
                result.put(entry.getKey(), entry.getValue());
            }
        }
        return result;
    }

    // partitionName -> formatValue 1:1/1:N
    public Map<String, PListCell> getValidListPartitionMap(int lastPartitionNum) {
        Map<String, PListCell> listPartitionMap = getListPartitionItems();
        // less than 0 means not set
        if (lastPartitionNum < 0) {
            return listPartitionMap;
        }

        int partitionNum = listPartitionMap.size();
        if (lastPartitionNum > partitionNum) {
            return listPartitionMap;
        }

        return listPartitionMap.entrySet().stream()
                .sorted(Map.Entry.comparingByValue(PListCell::compareTo))
                .skip(Math.max(0, partitionNum - lastPartitionNum))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    public Set<ColumnId> getBfColumnIds() {
        return bfColumns;
    }

    public Set<String> getBfColumnNames() {
        if (bfColumns == null) {
            return null;
        }

        Set<String> columnNames = Sets.newTreeSet(String.CASE_INSENSITIVE_ORDER);
        for (ColumnId columnId : bfColumns) {
            Column column = idToColumn.get(columnId);
            if (column == null) {
                LOG.warn("can not find column by column id: {}, maybe the column has been dropped.", columnId);
                continue;
            }
            columnNames.add(column.getName());
        }
        if (columnNames.isEmpty()) {
            return null;
        } else {
            return columnNames;
        }
    }

    public List<Index> getCopiedIndexes() {
        if (indexes == null) {
            return Lists.newArrayList();
        }
        return indexes.getCopiedIndexes();
    }

    public double getBfFpp() {
        return bfFpp;
    }

    public void setBloomFilterInfo(Set<ColumnId> bfColumns, double bfFpp) {
        this.bfColumns = bfColumns;
        this.bfFpp = bfFpp;
    }

    public void setIndexes(List<Index> indexes) {
        if (this.indexes == null) {
            this.indexes = new TableIndexes(null);
        }
        this.indexes.setIndexes(indexes);
    }

    public String getColocateGroup() {
        return colocateGroup;
    }

    public void setColocateGroup(String colocateGroup) {
        this.colocateGroup = colocateGroup;
    }

    public boolean isEnableColocateMVIndex() {
        if (!isOlapTableOrMaterializedView()) {
            return false;
        }

        // If the table's colocate group is empty, return false
        if (Strings.isNullOrEmpty(colocateGroup)) {
            return false;
        }

        // If there is only one meta, return false
        if (indexIdToMeta.size() == 1) {
            return false;
        }

        // If the colocate group is not stable, return false
        ColocateTableIndex colocateIndex = GlobalStateMgr.getCurrentState().getColocateTableIndex();
        if (colocateIndex.isGroupUnstable(colocateIndex.getGroup(getId()))) {
            return false;
        }

        // If all indexes except the basic index are all colocate, we can use colocate
        // mv index optimization.
        return indexIdToMeta.values().stream()
                .filter(x -> x.getIndexId() != baseIndexId)
                .allMatch(MaterializedIndexMeta::isColocateMVIndex);
    }

    // when the table is creating new rollup and enter finishing state, should tell
    // be not auto load to new rollup
    // it is used for stream load
    // the caller should get db lock when call this method
    public boolean shouldLoadToNewRollup() {
        return false;
    }

    public boolean isTempPartition(long partitionId) {
        return tempPartitions.getPartition(partitionId) != null;
    }

    @Override
    public TTableDescriptor toThrift(List<ReferencedPartitionInfo> partitions) {
        TOlapTable tOlapTable = new TOlapTable(getName());
        TTableDescriptor tTableDescriptor = new TTableDescriptor(id, TTableType.OLAP_TABLE,
                fullSchema.size(), 0, getName(), "");
        tTableDescriptor.setOlapTable(tOlapTable);
        return tTableDescriptor;
    }

    public long getRowCount() {
        long rowCount = 0;
        for (Map.Entry<Long, Partition> entry : idToPartition.entrySet()) {
            for (PhysicalPartition partition : entry.getValue().getSubPartitions()) {
                rowCount += partition.getBaseIndex().getRowCount();
            }
        }
        return rowCount;
    }

    public int getSignature(int signatureVersion, List<String> partNames, boolean isRestore) {
        Adler32 adler32 = new Adler32();
        adler32.update(signatureVersion);

        // table name
        adler32.update(name.getBytes(StandardCharsets.UTF_8));
        LOG.debug("signature. table name: {}", name);
        // type
        adler32.update(type.name().getBytes(StandardCharsets.UTF_8));
        LOG.debug("signature. table type: {}", type.name());

        // all indices(should be in order)
        Set<String> indexNames = Sets.newTreeSet();
        indexNames.addAll(indexNameToId.keySet());
        for (String indexName : indexNames) {
            long indexId = indexNameToId.get(indexName);
            adler32.update(indexName.getBytes(StandardCharsets.UTF_8));
            LOG.debug("signature. index name: {}", indexName);
            MaterializedIndexMeta indexMeta = indexIdToMeta.get(indexId);
            // schema hash
            // schema hash will change after finish schema change. It is make no sense
            // that check the schema hash here when doing restore
            if (!isRestore) {
                adler32.update(indexMeta.getSchemaHash());
                LOG.debug("signature. index schema hash: {}", indexMeta.getSchemaHash());
            }
            // short key column count
            adler32.update(indexMeta.getShortKeyColumnCount());
            LOG.debug("signature. index short key: {}", indexMeta.getShortKeyColumnCount());
            // storage type
            adler32.update(indexMeta.getStorageType().name().getBytes(StandardCharsets.UTF_8));
            LOG.debug("signature. index storage type: {}", indexMeta.getStorageType());
        }

        // bloom filter
        if (bfColumns != null && !bfColumns.isEmpty()) {
            for (ColumnId bfCol : bfColumns) {
                adler32.update(bfCol.getId().getBytes());
                LOG.debug("signature. bf col: {}", bfCol);
            }
            adler32.update(String.valueOf(bfFpp).getBytes());
            LOG.debug("signature. bf fpp: {}", bfFpp);
        }

        // partition type
        adler32.update(partitionInfo.getType().name().getBytes(StandardCharsets.UTF_8));
        LOG.debug("signature. partition type: {}", partitionInfo.getType().name());
        // partition columns
        if (partitionInfo.isRangePartition()) {
            List<Column> partitionColumns = partitionInfo.getPartitionColumns(this.idToColumn);
            adler32.update(Util.schemaHash(0, partitionColumns, null, 0));
            LOG.debug("signature. partition col hash: {}", Util.schemaHash(0, partitionColumns, null, 0));
        }

        // partition and distribution
        Collections.sort(partNames, String.CASE_INSENSITIVE_ORDER);
        for (String partName : partNames) {
            Partition partition = getPartition(partName);
            Preconditions.checkNotNull(partition, partName);
            adler32.update(partName.getBytes(StandardCharsets.UTF_8));
            LOG.debug("signature. partition name: {}", partName);
            DistributionInfo distributionInfo = partition.getDistributionInfo();
            adler32.update(distributionInfo.getType().name().getBytes(StandardCharsets.UTF_8));
            if (distributionInfo.getType() == DistributionInfoType.HASH) {
                HashDistributionInfo hashDistributionInfo = (HashDistributionInfo) distributionInfo;
                List<Column> distributionColumns = MetaUtils.getColumnsByColumnIds(
                        this, hashDistributionInfo.getDistributionColumns());
                adler32.update(Util.schemaHash(0, distributionColumns, null, 0));
                LOG.debug("signature. distribution col hash: {}",
                        Util.schemaHash(0, distributionColumns, null, 0));
                adler32.update(hashDistributionInfo.getBucketNum());
                LOG.debug("signature. bucket num: {}", hashDistributionInfo.getBucketNum());
            }
        }

        LOG.debug("signature: {}", Math.abs((int) adler32.getValue()));
        return Math.abs((int) adler32.getValue());
    }

    // This function is only used for getting the err msg for restore job
    public List<Pair<Integer, String>> getSignatureSequence(int signatureVersion, List<String> partNames) {
        List<Pair<Integer, String>> checkSumList = Lists.newArrayList();
        Adler32 adler32 = new Adler32();
        adler32.update(signatureVersion);

        // table name
        adler32.update(name.getBytes(StandardCharsets.UTF_8));
        checkSumList.add(new Pair(Math.abs((int) adler32.getValue()), "Table name is inconsistent"));
        // type
        adler32.update(type.name().getBytes(StandardCharsets.UTF_8));
        LOG.info("test getBytes", type.name().getBytes(StandardCharsets.UTF_8));
        checkSumList.add(new Pair(Math.abs((int) adler32.getValue()), "Table type is inconsistent"));

        // all indices(should be in order)
        Set<String> indexNames = Sets.newTreeSet();
        indexNames.addAll(indexNameToId.keySet());
        for (String indexName : indexNames) {
            long indexId = indexNameToId.get(indexName);
            adler32.update(indexName.getBytes(StandardCharsets.UTF_8));
            checkSumList.add(new Pair(Math.abs((int) adler32.getValue()), "indexName is inconsistent"));
            MaterializedIndexMeta indexMeta = indexIdToMeta.get(indexId);
            // short key column count
            adler32.update(indexMeta.getShortKeyColumnCount());
            checkSumList.add(new Pair(Math.abs((int) adler32.getValue()), "short key column count is inconsistent"));
            // storage type
            adler32.update(indexMeta.getStorageType().name().getBytes(StandardCharsets.UTF_8));
            checkSumList.add(new Pair(Math.abs((int) adler32.getValue()), "storage type is inconsistent"));
        }

        // bloom filter
        if (bfColumns != null && !bfColumns.isEmpty()) {
            for (ColumnId bfCol : bfColumns) {
                adler32.update(bfCol.getId().getBytes());
                checkSumList.add(new Pair(Math.abs((int) adler32.getValue()), "bloom filter is inconsistent"));
            }
            adler32.update(String.valueOf(bfFpp).getBytes());
            checkSumList.add(new Pair(Math.abs((int) adler32.getValue()), "bloom filter is inconsistent"));
        }

        // partition type
        adler32.update(partitionInfo.getType().name().getBytes(StandardCharsets.UTF_8));
        checkSumList.add(new Pair(Math.abs((int) adler32.getValue()), "partition type is inconsistent"));
        // partition columns
        if (partitionInfo.isRangePartition()) {
            List<Column> partitionColumns = partitionInfo.getPartitionColumns(this.idToColumn);
            adler32.update(Util.schemaHash(0, partitionColumns, null, 0));
            checkSumList.add(new Pair(Math.abs((int) adler32.getValue()), "partition columns is inconsistent"));
        }

        // partition and distribution
        Collections.sort(partNames, String.CASE_INSENSITIVE_ORDER);
        for (String partName : partNames) {
            Partition partition = getPartition(partName);
            Preconditions.checkNotNull(partition, partName);
            adler32.update(partName.getBytes(StandardCharsets.UTF_8));
            checkSumList.add(new Pair(Math.abs((int) adler32.getValue()), "partition name is inconsistent"));
            DistributionInfo distributionInfo = partition.getDistributionInfo();
            adler32.update(distributionInfo.getType().name().getBytes(StandardCharsets.UTF_8));
            if (distributionInfo.getType() == DistributionInfoType.HASH) {
                HashDistributionInfo hashDistributionInfo = (HashDistributionInfo) distributionInfo;
                List<Column> distributionColumns = MetaUtils.getColumnsByColumnIds(
                        this, hashDistributionInfo.getDistributionColumns());
                adler32.update(Util.schemaHash(0, distributionColumns, null, 0));
                checkSumList.add(new Pair(Math.abs((int) adler32.getValue()), "partition distribution col hash is inconsistent"));
                adler32.update(hashDistributionInfo.getBucketNum());
                checkSumList.add(new Pair(Math.abs((int) adler32.getValue()), "bucket num is inconsistent"));
            }
        }

        return checkSumList;
    }

    // get intersect partition names with the given table "anotherTbl". not
    // including temp partitions
    public Status getIntersectPartNamesWith(OlapTable anotherTbl, List<String> intersectPartNames) {
        if (this.getPartitionInfo().getType() != anotherTbl.getPartitionInfo().getType()) {
            return new Status(ErrCode.COMMON_ERROR, "Table's partition type is different");
        }

        Set<String> intersect = this.getPartitionNames();
        intersect.retainAll(anotherTbl.getPartitionNames());
        intersectPartNames.addAll(intersect);
        return Status.OK;
    }

    // Whether it's a partitioned table partition by columns, range or list.
    public boolean isPartitionedTable() {
        return partitionInfo != null && partitionInfo.isPartitioned();
    }

    @Override
    public boolean isUnPartitioned() {
        return !isPartitionedTable();
    }

    // NOTE: It's different from `isPartitionedTable` which `isPartitioned` means table has many buckets rather than
    // partitions.
    @Override
    public boolean isPartitioned() {
        int numSegs = 0;
        for (Partition part : getPartitions()) {
            numSegs += part.getDistributionInfo().getBucketNum();
            if (numSegs > 1) {
                return true;
            }
        }
        return false;
    }

    @Override
    public void gsonPostProcess() throws IOException {
        // In the present, the fullSchema could be rebuilt by schema change while the
        // properties is changed by MV.
        // After that, some properties of fullSchema and nameToColumn may be not same as
        // properties of base columns.
        // So, here we need to rebuild the fullSchema to ensure the correctness of the
        // properties.
        rebuildFullSchema();

        // Recover nameToPartition from idToPartition
        nameToPartition = Maps.newTreeMap(String.CASE_INSENSITIVE_ORDER);
        physicalPartitionIdToPartitionId = Maps.newHashMap();
        physicalPartitionNameToPartitionId = Maps.newHashMap();
        for (Partition partition : idToPartition.values()) {
            nameToPartition.put(partition.getName(), partition);
            for (PhysicalPartition physicalPartition : partition.getSubPartitions()) {
                physicalPartitionIdToPartitionId.put(physicalPartition.getId(), partition.getId());
                physicalPartitionNameToPartitionId.put(physicalPartition.getName(), partition.getId());

                // Every partition has a ShardGroup previously,
                // and now every Materialized index has a shardGroup.
                // So the original partition's shardGroup is moved to the base materialized index for compatibility
                if (physicalPartition.getShardGroupId() != PhysicalPartition.INVALID_SHARD_GROUP_ID) {
                    physicalPartition.getBaseIndex().setShardGroupId(physicalPartition.getShardGroupId());
                }
            }
        }

        if (partitionInfo instanceof ExpressionRangePartitionInfo) {
            ((ExpressionRangePartitionInfo) partitionInfo).updateSlotRef(nameToColumn);
        } else if (partitionInfo instanceof ListPartitionInfo) {
            ((ListPartitionInfo) partitionInfo).updateLiteralExprValues(idToColumn);
        }

        lastSchemaUpdateTime = new AtomicLong(-1);
    }

    public OlapTable selectiveCopy(Collection<String> reservedPartitions, boolean resetState, IndexExtState extState) {
        OlapTable copied = DeepCopy.copyWithGson(this, OlapTable.class);
        if (copied == null) {
            LOG.warn("failed to copy olap table: " + getName());
            return null;
        }
        return selectiveCopyInternal(copied, reservedPartitions, resetState, extState);
    }

    protected OlapTable selectiveCopyInternal(OlapTable copied, Collection<String> reservedPartitions,
                                              boolean resetState,
                                              IndexExtState extState) {
        if (resetState) {
            // remove shadow index from copied table
            List<MaterializedIndex> shadowIndex = copied.getPhysicalPartitions().stream().findFirst()
                    .map(p -> p.getMaterializedIndices(IndexExtState.SHADOW)).orElse(Lists.newArrayList());
            for (MaterializedIndex deleteIndex : shadowIndex) {
                LOG.debug("copied table delete shadow index : {}", deleteIndex.getId());
                copied.deleteIndexInfo(copied.getIndexNameById(deleteIndex.getId()));
            }
            copied.setState(OlapTableState.NORMAL);
            for (Partition partition : copied.getPartitions()) {
                for (PhysicalPartition physicalPartition : partition.getSubPartitions()) {
                    // remove shadow index from partition
                    for (MaterializedIndex deleteIndex : shadowIndex) {
                        physicalPartition.deleteRollupIndex(deleteIndex.getId());
                    }
                    for (MaterializedIndex idx : physicalPartition.getMaterializedIndices(extState)) {
                        idx.setState(IndexState.NORMAL);
                        if (copied.isCloudNativeTableOrMaterializedView()) {
                            continue;
                        }
                        for (Tablet tablet : idx.getTablets()) {
                            for (Replica replica : ((LocalTablet) tablet).getImmutableReplicas()) {
                                replica.setState(ReplicaState.NORMAL);
                            }
                        }
                    }
                }
                partition.setState(PartitionState.NORMAL);
            }
        }

        if (reservedPartitions == null || reservedPartitions.isEmpty()) {
            // reserve all
            return copied;
        }

        Set<String> reservedPartitionSet = Sets.newTreeSet(String.CASE_INSENSITIVE_ORDER);
        reservedPartitionSet.addAll(reservedPartitions);

        for (String partName : copied.getPartitionNames()) {
            if (!reservedPartitionSet.contains(partName)) {
                copied.dropPartitionAndReserveTablet(partName);
            }
        }

        return copied;
    }

    /*
     * this method is currently used for truncating table(partitions).
     * the new partition has new id, so we need to change all 'id-related' members
     *
     * return the old partition.
     */
    public Partition replacePartition(long dbId, Partition newPartition) {
        Partition oldPartition = nameToPartition.remove(newPartition.getName());

        // For cloud native table, add partition into recycle Bin after truncate table.
        // It is no necessary for share nothing mode because file will be deleted throught
        // tablet report in this case.
        if (this.isCloudNativeTableOrMaterializedView()) {
            RecyclePartitionInfo recyclePartitionInfo = buildRecyclePartitionInfo(dbId, oldPartition);
            recyclePartitionInfo.setRecoverable(false);
            GlobalStateMgr.getCurrentState().getRecycleBin().recyclePartition(recyclePartitionInfo);
        }

        oldPartition.getSubPartitions().forEach(physicalPartition -> {
            physicalPartitionIdToPartitionId.remove(physicalPartition.getId());
            physicalPartitionNameToPartitionId.remove(physicalPartition.getName());
        });
        idToPartition.remove(oldPartition.getId());
        idToPartition.put(newPartition.getId(), newPartition);
        newPartition.getSubPartitions().forEach(physicalPartition -> {
            physicalPartitionIdToPartitionId.put(physicalPartition.getId(), newPartition.getId());
            physicalPartitionNameToPartitionId.put(physicalPartition.getName(), newPartition.getId());
        });

        nameToPartition.put(newPartition.getName(), newPartition);

        DataProperty dataProperty = partitionInfo.getDataProperty(oldPartition.getId());
        short replicationNum = partitionInfo.getReplicationNum(oldPartition.getId());
        boolean isInMemory = partitionInfo.getIsInMemory(oldPartition.getId());
        DataCacheInfo dataCacheInfo = partitionInfo.getDataCacheInfo(oldPartition.getId());

        if (partitionInfo.isRangePartition()) {
            RangePartitionInfo rangePartitionInfo = (RangePartitionInfo) partitionInfo;
            Range<PartitionKey> range = rangePartitionInfo.getRange(oldPartition.getId());
            rangePartitionInfo.dropPartition(oldPartition.getId());
            rangePartitionInfo.addPartition(newPartition.getId(), false, range, dataProperty,
                    replicationNum, isInMemory, dataCacheInfo);
        } else if (partitionInfo.getType() == PartitionType.LIST) {
            ListPartitionInfo listPartitionInfo = (ListPartitionInfo) partitionInfo;
            List<String> values = listPartitionInfo.getIdToValues().get(oldPartition.getId());
            List<List<String>> multiValues = listPartitionInfo.getIdToMultiValues().get(oldPartition.getId());
            listPartitionInfo.dropPartition(oldPartition.getId());
            try {
                listPartitionInfo.addPartition(idToColumn, newPartition.getId(), dataProperty, replicationNum,
                        isInMemory, dataCacheInfo, values, multiValues);
            } catch (AnalysisException ex) {
                LOG.warn("failed to add list partition", ex);
                throw new SemanticException(ex.getMessage());
            }
        } else {
            partitionInfo.dropPartition(oldPartition.getId());
            partitionInfo.addPartition(newPartition.getId(), dataProperty, replicationNum, isInMemory, dataCacheInfo);
        }

        return oldPartition;
    }

    public long getDataSize() {
        long dataSize = 0;
        for (Partition partition : getAllPartitions()) {
            dataSize += partition.getDataSize();
        }
        return dataSize;
    }

    public long getReplicaCount() {
        long replicaCount = 0;
        for (Partition partition : getAllPartitions()) {
            replicaCount += partition.getReplicaCount();
        }
        return replicaCount;
    }

    public void checkStableAndNormal() throws DdlException {
        if (state != OlapTableState.NORMAL) {
            throw InvalidOlapTableStateException.of(state, getName());
        }
        // check if all tablets are healthy, and no tablet is in tablet scheduler
        long unhealthyTabletId = checkAndGetUnhealthyTablet(GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo(),
                GlobalStateMgr.getCurrentState().getTabletScheduler());
        if (unhealthyTabletId != TabletInvertedIndex.NOT_EXIST_VALUE) {
            throw new DdlException("Table [" + name + "] is not stable. "
                    + "Unhealthy (or doing balance) tablet id: " + unhealthyTabletId + ". "
                    + "Some tablets of this table may not be healthy or are being scheduled. "
                    + "You need to repair the table first or stop cluster balance.");
        }
    }

    public long checkAndGetUnhealthyTablet(SystemInfoService infoService, TabletScheduler tabletScheduler) {
        List<Long> aliveBeIdsInCluster = infoService.getBackendIds(true);
        for (Partition partition : idToPartition.values()) {
            short replicationNum = partitionInfo.getReplicationNum(partition.getId());
            for (PhysicalPartition physicalPartition : partition.getSubPartitions()) {
                long visibleVersion = physicalPartition.getVisibleVersion();
                for (MaterializedIndex mIndex : physicalPartition.getMaterializedIndices(IndexExtState.ALL)) {
                    for (Tablet tablet : mIndex.getTablets()) {
                        LocalTablet localTablet = (LocalTablet) tablet;
                        if (tabletScheduler.containsTablet(tablet.getId())) {
                            LOG.info("table {} is not stable because tablet {} is being scheduled. replicas: {}",
                                    id, tablet.getId(), localTablet.getImmutableReplicas());
                            return localTablet.getId();
                        }

                        Pair<TabletHealthStatus, TabletSchedCtx.Priority> statusPair =
                                TabletChecker.getTabletHealthStatusWithPriority(
                                        localTablet,
                                        infoService, visibleVersion, replicationNum,
                                        aliveBeIdsInCluster, getLocation());
                        if (statusPair.first != TabletHealthStatus.HEALTHY) {
                            LOG.info("table {} is not stable because tablet {} status is {}. replicas: {}",
                                    id, tablet.getId(), statusPair.first, localTablet.getImmutableReplicas());
                            return localTablet.getId();
                        }
                    }
                }
            }
        }
        return TabletInvertedIndex.NOT_EXIST_VALUE;
    }

    // arbitrarily choose a partition, and get the buckets backends sequence from
    // base index.
    public List<List<Long>> getArbitraryTabletBucketsSeq() throws DdlException {
        List<List<Long>> backendsPerBucketSeq = Lists.newArrayList();
        Optional<Partition> optionalPartition = idToPartition.values().stream().findFirst();
        if (optionalPartition.isPresent()) {
            Partition partition = optionalPartition.get();
            PhysicalPartition physicalPartition = partition.getDefaultPhysicalPartition();

            short replicationNum = partitionInfo.getReplicationNum(partition.getId());
            MaterializedIndex baseIdx = physicalPartition.getBaseIndex();
            for (Long tabletId : baseIdx.getTabletIds()) {
                LocalTablet tablet = (LocalTablet) baseIdx.getTablet(tabletId);
                List<Long> replicaBackendIds = tablet.getNormalReplicaBackendIds();
                if (replicaBackendIds.size() < replicationNum) {
                    // this should not happen, but in case, throw an exception to terminate this
                    // process
                    throw new DdlException("Normal replica number of tablet " + tabletId + " is: "
                            + replicaBackendIds.size() + ", which is less than expected: " + replicationNum);
                }
                backendsPerBucketSeq.add(replicaBackendIds.subList(0, replicationNum));
            }
        }
        return backendsPerBucketSeq;
    }

    /**
     * Get the proximate row count of this table, if you need accurate row count
     * should select count(*) from table.
     *
     * @return proximate row count
     */
    public long proximateRowCount() {
        long totalCount = 0;
        for (Partition partition : getPartitions()) {
            for (PhysicalPartition physicalPartition : partition.getSubPartitions()) {
                long version = physicalPartition.getVisibleVersion();
                for (MaterializedIndex index : physicalPartition.getMaterializedIndices(IndexExtState.VISIBLE)) {
                    for (Tablet tablet : index.getTablets()) {
                        totalCount += tablet.getRowCount(version);
                    }
                }
            }
        }
        return totalCount;
    }

    @Override
    public List<Column> getBaseSchema() {
        return getSchemaByIndexId(baseIndexId);
    }

    @Override
    protected void updateSchemaIndex() {
        Map<String, Column> newNameToColumn = Maps.newTreeMap(String.CASE_INSENSITIVE_ORDER);
        Map<ColumnId, Column> newIdToColumn = Maps.newTreeMap(ColumnId.CASE_INSENSITIVE_ORDER);
        for (Column column : this.fullSchema) {
            newNameToColumn.put(column.getName(), column);
            // For OlapTable: fullSchema contains columns from baseIndex
            // and columns from shadow indexes (when doing schema changes).
            // To avoid base columns being replaced by shadow columns,
            // put the columns with the same ColumnId once.
            if (!newIdToColumn.containsKey(column.getColumnId())) {
                newIdToColumn.put(column.getColumnId(), column);
            }
        }
        this.nameToColumn = newNameToColumn;
        this.idToColumn = newIdToColumn;
    }

    public List<Column> getBaseSchemaWithoutGeneratedColumn() {
        if (!hasGeneratedColumn()) {
            return getSchemaByIndexId(baseIndexId);
        }

        List<Column> schema = Lists.newArrayList(getBaseSchema());

        while (schema.size() > 0) {
            // check last column is whether materiazlied column or not
            if (schema.get(schema.size() - 1).isGeneratedColumn()) {
                schema.remove(schema.size() - 1);
            } else {
                break;
            }
        }

        return schema;
    }

    public Column getBaseColumn(String columnName) {
        for (Column column : getBaseSchema()) {
            if (column.getName().equalsIgnoreCase(columnName)) {
                return column;
            }
        }
        return null;
    }

    public int getKeysNum() {
        int keysNum = 0;
        for (Column column : getBaseSchema()) {
            if (column.isKey()) {
                keysNum += 1;
            }
        }
        return keysNum;
    }

    public boolean isKeySet(Set<String> keyColumns) {
        Set<String> tableKeyColumns = getKeyColumns().stream()
                .map(column -> column.getName().toLowerCase()).collect(Collectors.toSet());
        return tableKeyColumns.equals(keyColumns);
    }

    public void setReplicationNum(Short replicationNum) {
        if (tableProperty == null) {
            tableProperty = new TableProperty(new HashMap<>());
        }
        tableProperty.modifyTableProperties(PropertyAnalyzer.PROPERTIES_REPLICATION_NUM, replicationNum.toString());
        tableProperty.buildReplicationNum();
    }

    public Short getDefaultReplicationNum() {
        if (tableProperty != null) {
            return tableProperty.getReplicationNum();
        }
        return RunMode.defaultReplicationNum();
    }

    public Boolean isInMemory() {
        if (tableProperty != null) {
            return tableProperty.isInMemory();
        }
        return false;
    }

    public void setIsInMemory(boolean isInMemory) {
        if (tableProperty == null) {
            tableProperty = new TableProperty(new HashMap<>());
        }
        tableProperty
                .modifyTableProperties(PropertyAnalyzer.PROPERTIES_INMEMORY, Boolean.valueOf(isInMemory).toString());
        tableProperty.buildInMemory();
    }

    public Boolean isFileBundling() {
        if (tableProperty != null) {
            return tableProperty.isFileBundling();
        }
        return false;
    }

    public Boolean enablePersistentIndex() {
        if (tableProperty != null) {
            return tableProperty.enablePersistentIndex();
        }
        return false;
    }

    public int primaryIndexCacheExpireSec() {
        if (tableProperty != null) {
            return tableProperty.primaryIndexCacheExpireSec();
        }
        return 0;
    }

    public String getPersistentIndexTypeString() {
        if (tableProperty != null) {
            return tableProperty.getPersistentIndexTypeString();
        }
        return "";
    }

    public TPersistentIndexType getPersistentIndexType() {
        if (tableProperty != null) {
            return tableProperty.getPersistentIndexType();
        }
        return null;
    }

    // Determine which situation supports importing and automatically creating
    // partitions
    public Boolean supportedAutomaticPartition() {
        return partitionInfo.isAutomaticPartition();
    }

    public Boolean isBinlogEnabled() {
        if (tableProperty == null || tableProperty.getBinlogConfig() == null) {
            return false;
        }
        return tableProperty.getBinlogConfig().getBinlogEnable();
    }

    public long getBinlogVersion() {
        if (tableProperty == null || tableProperty.getBinlogConfig() == null) {
            return BinlogConfig.INVALID;
        }
        return tableProperty.getBinlogConfig().getVersion();
    }

    public String storageType() {
        if (tableProperty != null) {
            return tableProperty.storageType();
        }
        return PROPERTIES_STORAGE_TYPE_COLUMN;
    }

    public TStorageType getStorageType() {
        if (storageType() == null) {
            return TStorageType.COLUMN;
        }
        switch (storageType().toLowerCase()) {
            case PROPERTIES_STORAGE_TYPE_COLUMN:
                return TStorageType.COLUMN;
            case PROPERTIES_STORAGE_TYPE_COLUMN_WITH_ROW:
                return TStorageType.COLUMN_WITH_ROW;
            default:
                throw new SemanticException("getStorageType type not support: " + storageType());
        }
    }

    public void setStorageType(String storageType) {
        if (tableProperty == null) {
            tableProperty = new TableProperty(new HashMap<>());
        }
        if (storageType == null) {
            storageType = PROPERTIES_STORAGE_TYPE_COLUMN;
        }
        tableProperty.modifyTableProperties(PropertyAnalyzer.PROPERTIES_STORAGE_TYPE, storageType);
        tableProperty.buildStorageType();
    }

    public void setEnablePersistentIndex(boolean enablePersistentIndex) {
        if (tableProperty == null) {
            tableProperty = new TableProperty(new HashMap<>());
        }
        tableProperty
                .modifyTableProperties(PropertyAnalyzer.PROPERTIES_ENABLE_PERSISTENT_INDEX,
                        Boolean.valueOf(enablePersistentIndex).toString());
        tableProperty.buildEnablePersistentIndex();
    }

    public void setPrimaryIndexCacheExpireSec(int primaryIndexCacheExpireSec) {
        if (tableProperty == null) {
            tableProperty = new TableProperty(new HashMap<>());
        }
        tableProperty
                .modifyTableProperties(PropertyAnalyzer.PROPERTIES_PRIMARY_INDEX_CACHE_EXPIRE_SEC,
                        Integer.valueOf(primaryIndexCacheExpireSec).toString());
        tableProperty.buildPrimaryIndexCacheExpireSec();
    }

    public void setPersistentIndexType(TPersistentIndexType persistentIndexType) {
        if (tableProperty == null) {
            tableProperty = new TableProperty(new HashMap<>());
        }

        // only support LOCAL and CLOUD_NATIVE for now
        if (persistentIndexType == TPersistentIndexType.LOCAL || persistentIndexType == TPersistentIndexType.CLOUD_NATIVE) {
            tableProperty.modifyTableProperties(PropertyAnalyzer.PROPERTIES_PERSISTENT_INDEX_TYPE,
                    TableProperty.persistentIndexTypeToString(persistentIndexType));
        } else {
            // do nothing
            LOG.warn("Unknown TPersistentIndexType");
            return;
        }

        tableProperty.buildPersistentIndexType();
    }

    public TCompactionStrategy getCompactionStrategy() {
        if (tableProperty != null) {
            return tableProperty.getCompactionStrategy();
        }
        return TCompactionStrategy.DEFAULT;
    }

    public void setCompactionStrategy(TCompactionStrategy compactionStrategy) {
        if (tableProperty == null) {
            tableProperty = new TableProperty(new HashMap<>());
        }

        // only support DEFAULT and REAL_TIME for now
        if (compactionStrategy == TCompactionStrategy.DEFAULT || compactionStrategy == TCompactionStrategy.REAL_TIME) {
            tableProperty.modifyTableProperties(PropertyAnalyzer.PROPERTIES_COMPACTION_STRATEGY,
                    TableProperty.compactionStrategyToString(compactionStrategy));
        } else {
            // do nothing
            LOG.warn("Unknown TCompactionStrategy");
            return;
        }

        tableProperty.buildCompactionStrategy();
    }

    public Multimap<String, String> getLocation() {
        if (tableProperty != null) {
            return tableProperty.getLocation();
        }
        return null;
    }

    public void setLocation(String location) {
        if (tableProperty == null) {
            tableProperty = new TableProperty(new HashMap<>());
        }

        tableProperty.modifyTableProperties(PropertyAnalyzer.PROPERTIES_LABELS_LOCATION, location);
        tableProperty.buildLocation();
    }

    public Boolean enableReplicatedStorage() {
        if (tableProperty != null) {
            return tableProperty.enableReplicatedStorage();
        }
        return false;
    }

    public void setEnableReplicatedStorage(boolean enableReplicatedStorage) {
        if (tableProperty == null) {
            tableProperty = new TableProperty(new HashMap<>());
        }
        tableProperty
                .modifyTableProperties(PropertyAnalyzer.PROPERTIES_REPLICATED_STORAGE,
                        Boolean.valueOf(enableReplicatedStorage).toString());
        tableProperty.buildReplicatedStorage();
    }

    public Long getAutomaticBucketSize() {
        if (!(defaultDistributionInfo instanceof RandomDistributionInfo) || !Config.enable_automatic_bucket) {
            return (long) 0;
        }
        if (tableProperty != null) {
            return tableProperty.getBucketSize();
        }
        return (long) 0;
    }

    public Long getMutableBucketNum() {
        if (tableProperty != null) {
            return tableProperty.getMutableBucketNum();
        }
        return (long) 0;
    }

    public String getBaseCompactionForbiddenTimeRanges() {
        if (tableProperty != null) {
            return tableProperty.getBaseCompactionForbiddenTimeRanges();
        }
        return "";
    }

    public void setAutomaticBucketSize(long bucketSize) {
        if (tableProperty == null) {
            tableProperty = new TableProperty(new HashMap<>());
        }
        tableProperty
                .modifyTableProperties(PropertyAnalyzer.PROPERTIES_BUCKET_SIZE,
                        String.valueOf(bucketSize));
        tableProperty.buildBucketSize();
    }

    public void setMutableBucketNum(long bucketNum) {
        if (tableProperty == null) {
            tableProperty = new TableProperty(new HashMap<>());
        }
        tableProperty
                .modifyTableProperties(PropertyAnalyzer.PROPERTIES_MUTABLE_BUCKET_NUM,
                        String.valueOf(bucketNum));
        tableProperty.buildMutableBucketNum();
    }

    public Boolean enableLoadProfile() {
        if (tableProperty != null) {
            return tableProperty.enableLoadProfile();
        }
        return false;
    }

    public void setEnableLoadProfile(boolean enableLoadProfile) {
        if (tableProperty == null) {
            tableProperty = new TableProperty(new HashMap<>());
        }
        tableProperty
                .modifyTableProperties(PropertyAnalyzer.PROPERTIES_ENABLE_LOAD_PROFILE,
                        Boolean.valueOf(enableLoadProfile).toString());
        tableProperty.buildEnableLoadProfile();
    }

    public void setBaseCompactionForbiddenTimeRanges(String baseCompactionForbiddenTimeRanges) {
        if (tableProperty == null) {
            tableProperty = new TableProperty(new HashMap<>());
        }
        tableProperty
                .modifyTableProperties(PropertyAnalyzer.PROPERTIES_BASE_COMPACTION_FORBIDDEN_TIME_RANGES,
                        baseCompactionForbiddenTimeRanges);
        tableProperty.buildBaseCompactionForbiddenTimeRanges();
    }

    public void updateBaseCompactionForbiddenTimeRanges(boolean isDrop) {
        try {
            if (this.isCloudNativeTableOrMaterializedView()) {
                return;
            }
            if (getBaseCompactionForbiddenTimeRanges().isEmpty()) {
                return;
            }
            GlobalStateMgr.getCurrentState().getCompactionControlScheduler().updateTableForbiddenTimeRanges(
                    getId(), isDrop ? "" : getBaseCompactionForbiddenTimeRanges());
        } catch (Exception e) {
            LOG.warn("Failed to update base compaction forbidden time ranges for " + getName(), e);
        }
    }

    public TWriteQuorumType writeQuorum() {
        if (tableProperty != null) {
            return tableProperty.writeQuorum();
        }
        return TWriteQuorumType.MAJORITY;
    }

    public void setWriteQuorum(String writeQuorum) {
        if (tableProperty == null) {
            tableProperty = new TableProperty(new HashMap<>());
        }
        tableProperty
                .modifyTableProperties(PropertyAnalyzer.PROPERTIES_WRITE_QUORUM,
                        writeQuorum);
        tableProperty.buildWriteQuorum();
    }

    public void setStorageMedium(TStorageMedium storageMedium) {
        if (tableProperty == null) {
            tableProperty = new TableProperty(new HashMap<>());
        }
        tableProperty
                .modifyTableProperties(PropertyAnalyzer.PROPERTIES_STORAGE_MEDIUM, storageMedium.name());
    }

    public String getStorageMedium() {
        return tableProperty.getProperties().getOrDefault(PropertyAnalyzer.PROPERTIES_STORAGE_MEDIUM,
                TStorageMedium.HDD.name());
    }

    public boolean hasDelete() {
        if (tableProperty == null) {
            return false;
        }
        return tableProperty.hasDelete();
    }

    public void setHasDelete() {
        if (tableProperty == null) {
            tableProperty = new TableProperty(new HashMap<>());
        }
        tableProperty.setHasDelete(true);
    }

    public void setDataCachePartitionDuration(PeriodDuration duration) {
        if (tableProperty == null) {
            tableProperty = new TableProperty(new HashMap<>());
        }
        tableProperty.modifyTableProperties(PropertyAnalyzer.PROPERTIES_DATACACHE_PARTITION_DURATION,
                TimeUtils.toHumanReadableString(duration));
        tableProperty.buildDataCachePartitionDuration();
    }

    public void setFileBundling(boolean fileBundling) {
        if (tableProperty == null) {
            tableProperty = new TableProperty(new HashMap<>());
        }
        tableProperty.modifyTableProperties(PropertyAnalyzer.PROPERTIES_FILE_BUNDLING,
                        Boolean.valueOf(fileBundling).toString());
        tableProperty.buildFileBundling();
    }

    public void setStorageCoolDownTTL(PeriodDuration duration) {
        if (tableProperty == null) {
            tableProperty = new TableProperty(new HashMap<>());
        }
        tableProperty.modifyTableProperties(PropertyAnalyzer.PROPERTIES_STORAGE_COOLDOWN_TTL,
                TimeUtils.toHumanReadableString(duration));
        tableProperty.buildStorageCoolDownTTL();
    }

    public boolean hasForbiddenGlobalDict() {
        if (tableProperty == null) {
            return false;
        }
        return tableProperty.hasForbiddenGlobalDict();
    }

    public void setHasForbiddenGlobalDict(boolean hasForbiddenGlobalDict) {
        if (tableProperty == null) {
            tableProperty = new TableProperty(new HashMap<>());
        }
        tableProperty.setHasForbiddenGlobalDict(hasForbiddenGlobalDict);
    }

    // return true if partition with given name already exist, both in partitions
    // and temp partitions.
    // return false otherwise
    public boolean checkPartitionNameExist(String partitionName) {
        if (nameToPartition.containsKey(partitionName)) {
            return true;
        }
        return tempPartitions.hasPartition(partitionName);
    }

    // if includeTempPartition is true, check if temp partition with given name
    // exist,
    // if includeTempPartition is false, check if normal partition with given name
    // exist.
    // return true if exist, otherwise, return false;
    public boolean checkPartitionNameExist(String partitionName, boolean isTempPartition) {
        if (isTempPartition) {
            return tempPartitions.hasPartition(partitionName);
        } else {
            return nameToPartition.containsKey(partitionName);
        }
    }

    // drop temp partition. if needDropTablet is true, tablets of this temp
    // partition
    // will be dropped from tablet inverted index.
    public void dropTempPartition(String partitionName, boolean needDropTablet) {
        Partition partition = getPartition(partitionName, true);
        if (partition != null) {
            partitionInfo.dropPartition(partition.getId());
            tempPartitions.dropPartition(partitionName, needDropTablet);
            for (PhysicalPartition physicalPartition : partition.getSubPartitions()) {
                physicalPartitionIdToPartitionId.remove(physicalPartition.getId());
                physicalPartitionNameToPartitionId.remove(physicalPartition.getName());
            }
        }
    }

    public void replaceMatchPartitions(long dbId, List<String> tempPartitionNames) {
        for (String partitionName : tempPartitionNames) {
            Partition partition = tempPartitions.getPartition(partitionName);
            if (partition != null) {
                String oldPartitionName = partitionName.substring(
                        partitionName.indexOf(AnalyzerUtils.PARTITION_NAME_PREFIX_SPLIT) + 1);
                Partition oldPartition = nameToPartition.get(oldPartitionName);
                if (oldPartition != null) {
                    // drop old partition
                    dropPartition(dbId, oldPartitionName, true);
                }
                // add new partition
                addPartition(partition);
                // drop temp partition
                tempPartitions.dropPartition(partitionName, false);
                // move the range from idToTempRange to idToRange
                partitionInfo.moveRangeFromTempToFormal(partition.getId());
                // rename partition
                renamePartition(partitionName, oldPartitionName);
            }
        }

        for (Column column : getColumns()) {
            IDictManager.getInstance().removeGlobalDict(this.getId(), column.getColumnId());
        }
    }

    /*
     * replace partitions in 'partitionNames' with partitions in
     * 'tempPartitionNames'.
     * If strictRange is true, the replaced ranges must be exactly same.
     * What is "exactly same"?
     * 1. {[0, 10), [10, 20)} === {[0, 20)}
     * 2. {[0, 10), [15, 20)} === {[0, 10), [15, 18), [18, 20)}
     * 3. {[0, 10), [15, 20)} === {[0, 10), [15, 20)}
     * 4. {[0, 10), [15, 20)} !== {[0, 20)}
     *
     * If useTempPartitionName is false and replaced partition number are equal,
     * the replaced partitions' name will remain unchanged.
     * What is "remain unchange"?
     * 1. replace partition (p1, p2) with temporary partition (tp1, tp2). After
     * replacing, the partition
     * names are still p1 and p2.
     *
     */
    public void replaceTempPartitions(long dbId, List<String> partitionNames, List<String> tempPartitionNames,
                                      boolean strictRange, boolean useTempPartitionName) throws DdlException {
        if (partitionInfo instanceof RangePartitionInfo) {
            RangePartitionInfo rangeInfo = (RangePartitionInfo) partitionInfo;

            if (strictRange) {
                // check if range of partitions and temp partitions are exactly same
                List<Range<PartitionKey>> rangeList = Lists.newArrayList();
                List<Range<PartitionKey>> tempRangeList = Lists.newArrayList();
                for (String partName : partitionNames) {
                    Partition partition = nameToPartition.get(partName);
                    Preconditions.checkNotNull(partition);
                    rangeList.add(rangeInfo.getRange(partition.getId()));
                }

                for (String partName : tempPartitionNames) {
                    Partition partition = tempPartitions.getPartition(partName);
                    Preconditions.checkNotNull(partition);
                    tempRangeList.add(rangeInfo.getRange(partition.getId()));
                }
                RangeUtils.checkRangeListsMatch(rangeList, tempRangeList);
            } else {
                // check after replacing, whether the range will conflict
                Set<Long> replacePartitionIds = Sets.newHashSet();
                for (String partName : partitionNames) {
                    Partition partition = nameToPartition.get(partName);
                    Preconditions.checkNotNull(partition);
                    replacePartitionIds.add(partition.getId());
                }
                List<Range<PartitionKey>> replacePartitionRanges = Lists.newArrayList();
                for (String partName : tempPartitionNames) {
                    Partition partition = tempPartitions.getPartition(partName);
                    Preconditions.checkNotNull(partition);
                    replacePartitionRanges.add(rangeInfo.getRange(partition.getId()));
                }
                List<Range<PartitionKey>> sortedRangeList = rangeInfo.getRangeList(replacePartitionIds, false);
                RangeUtils.checkRangeConflict(sortedRangeList, replacePartitionRanges);
            }
        } else if (partitionInfo instanceof ListPartitionInfo) {
            ListPartitionInfo listInfo = (ListPartitionInfo) partitionInfo;
            List<Partition> partitionList = new ArrayList<>();
            for (String partName : partitionNames) {
                Partition partition = nameToPartition.get(partName);
                Preconditions.checkNotNull(partition);
                partitionList.add(partition);
            }
            List<Partition> tempPartitionList = new ArrayList<>();
            for (String partName : tempPartitionNames) {
                Partition tempPartition = tempPartitions.getPartition(partName);
                Preconditions.checkNotNull(tempPartition);
                tempPartitionList.add(tempPartition);
            }
            if (strictRange) {
                CatalogUtils.checkTempPartitionStrictMatch(partitionList, tempPartitionList, listInfo);
            } else {
                CatalogUtils.checkTempPartitionConflict(partitionList, tempPartitionList, listInfo);
            }
        }

        // begin to replace
        // 1. drop old partitions
        for (String partitionName : partitionNames) {
            // This will also drop all tablets of the partition from TabletInvertedIndex
            dropPartition(dbId, partitionName, true);
        }

        // 2. add temp partitions' range info to rangeInfo, and remove them from
        // tempPartitionInfo
        for (String partitionName : tempPartitionNames) {
            Partition partition = tempPartitions.getPartition(partitionName);
            // add
            addPartition(partition);
            // drop
            tempPartitions.dropPartition(partitionName, false);
            // move the range from idToTempRange to idToRange
            partitionInfo.moveRangeFromTempToFormal(partition.getId());
        }

        // change the name so that after replacing, the partition name remain unchanged
        if (!useTempPartitionName && partitionNames.size() == tempPartitionNames.size()) {
            for (int i = 0; i < tempPartitionNames.size(); i++) {
                renamePartition(tempPartitionNames.get(i), partitionNames.get(i));
            }
        }

        for (Column column : getColumns()) {
            IDictManager.getInstance().removeGlobalDict(this.getId(), column.getColumnId());
        }
    }

    // used for unpartitioned table in insert overwrite
    // replace partition with temp partition
    public void replacePartition(long dbId, String sourcePartitionName, String tempPartitionName) {
        if (partitionInfo.getType() != PartitionType.UNPARTITIONED) {
            return;
        }
        // drop source partition
        Partition srcPartition = nameToPartition.get(sourcePartitionName);
        if (srcPartition != null) {
            dropPartition(dbId, sourcePartitionName, true);
        }

        Partition partition = tempPartitions.getPartition(tempPartitionName);
        // add
        addPartition(partition);
        // drop
        tempPartitions.dropPartition(tempPartitionName, false);

        // rename partition
        renamePartition(tempPartitionName, sourcePartitionName);

        for (Column column : getColumns()) {
            IDictManager.getInstance().removeGlobalDict(this.getId(), column.getColumnId());
        }
    }

    public void addTempPartition(Partition partition) {
        tempPartitions.addPartition(partition);
        for (PhysicalPartition physicalPartition : partition.getSubPartitions()) {
            physicalPartitionIdToPartitionId.put(physicalPartition.getId(), partition.getId());
            physicalPartitionNameToPartitionId.put(physicalPartition.getName(), partition.getId());
        }
    }

    public void dropAllTempPartitions() {
        for (Partition partition : tempPartitions.getAllPartitions()) {
            partitionInfo.dropPartition(partition.getId());
            for (PhysicalPartition physicalPartition : partition.getSubPartitions()) {
                physicalPartitionIdToPartitionId.remove(physicalPartition.getId());
                physicalPartitionNameToPartitionId.remove(physicalPartition.getName());
            }
        }
        tempPartitions.dropAll();
    }

    public boolean existTempPartitions() {
        return !tempPartitions.isEmpty();
    }

    public void setCompressionType(TCompressionType compressionType) {
        if (tableProperty == null) {
            tableProperty = new TableProperty(new HashMap<>());
        }
        tableProperty.modifyTableProperties(PropertyAnalyzer.PROPERTIES_COMPRESSION, compressionType.name());
        tableProperty.buildCompressionType();
    }

    public void setCompressionLevel(int compressionLevel) {
        if (tableProperty == null) {
            tableProperty = new TableProperty(new HashMap<>());
        }
        tableProperty.setCompressionLevel(compressionLevel);
    }

    public TCompressionType getCompressionType() {
        if (tableProperty == null) {
            return TCompressionType.LZ4_FRAME;
        }
        return tableProperty.getCompressionType();
    }

    public int getCompressionLevel() {
        if (tableProperty == null) {
            return -1;
        }
        return tableProperty.getCompressionLevel();
    }

    public void setPartitionLiveNumber(int number) {
        if (tableProperty == null) {
            tableProperty = new TableProperty(new HashMap<>());
        }
        tableProperty.modifyTableProperties(PropertyAnalyzer.PROPERTIES_PARTITION_LIVE_NUMBER, String.valueOf(number));
        tableProperty.buildPartitionLiveNumber();
    }

    public Map<String, String> buildBinlogAvailableVersion() {
        Map<String, String> result = new HashMap<>();
        Collection<Partition> partitions = getPartitions();
        for (Partition partition : partitions) {
            result.put(TableProperty.BINLOG_PARTITION + partition.getId(),
                    String.valueOf(partition.getDefaultPhysicalPartition().getVisibleVersion()));
        }
        return result;
    }

    public void setBinlogAvailableVersion(Map<String, String> properties) {
        if (tableProperty == null) {
            tableProperty = new TableProperty(new HashMap<>());
        }
        tableProperty.modifyTableProperties(properties);
        tableProperty.buildBinlogAvailableVersion();
    }

    public Map<Long, Long> getBinlogAvailableVersion() {
        if (tableProperty == null) {
            return new HashMap<>();
        }
        return tableProperty.getBinlogAvailableVersions();
    }

    public void clearBinlogAvailableVersion() {
        if (tableProperty == null) {
            return;
        }
        tableProperty.clearBinlogAvailableVersion();
    }

    @Override
    public boolean hasUniqueConstraints() {
        if (keysType == KeysType.UNIQUE_KEYS || keysType == KeysType.PRIMARY_KEYS) {
            return true;
        }
        return tableProperty != null &&
                tableProperty.getUniqueConstraints() != null &&
                !tableProperty.getUniqueConstraints().isEmpty();
    }

    @Override
    public List<UniqueConstraint> getUniqueConstraints() {
        List<UniqueConstraint> uniqueConstraints = Lists.newArrayList();
        if (!hasUniqueConstraints()) {
            return uniqueConstraints;
        }
        if (keysType == KeysType.UNIQUE_KEYS || keysType == KeysType.PRIMARY_KEYS) {
            uniqueConstraints.add(
                    new UniqueConstraint(null, null, getName(), getKeyColumns()
                            .stream().map(Column::getColumnId).collect(Collectors.toList())));
        }
        if (tableProperty != null && tableProperty.getUniqueConstraints() != null) {
            uniqueConstraints.addAll(tableProperty.getUniqueConstraints());
        }
        return uniqueConstraints;
    }

    @Override
    public void setUniqueConstraints(List<UniqueConstraint> uniqueConstraints) {
        if (tableProperty == null) {
            tableProperty = new TableProperty(new HashMap<>());
        }
        Map<String, String> properties = Maps.newHashMap();
        String newProperty = uniqueConstraints.stream().map(UniqueConstraint::toString)
                .collect(Collectors.joining(";"));
        properties.put(PropertyAnalyzer.PROPERTIES_UNIQUE_CONSTRAINT, newProperty);
        tableProperty.modifyTableProperties(properties);
        tableProperty.setUniqueConstraints(uniqueConstraints);
    }

    @Override
    public List<ForeignKeyConstraint> getForeignKeyConstraints() {
        if (tableProperty == null) {
            return null;
        }
        return tableProperty.getForeignKeyConstraints();
    }

    /**
     * Return Whether MaterializedView has foreignKey constraints or not. MV's constraints come from table properties
     * and is different from normal table.
     */
    @Override
    public boolean hasForeignKeyConstraints() {
        return tableProperty != null && CollectionUtils.isNotEmpty(getForeignKeyConstraints());
    }

    @Override
    public void setForeignKeyConstraints(List<ForeignKeyConstraint> foreignKeyConstraints) {
        if (tableProperty == null) {
            tableProperty = new TableProperty(new HashMap<>());
        }
        Map<String, String> properties = Maps.newHashMap();
        String newProperty = foreignKeyConstraints
                .stream().map(ForeignKeyConstraint::toString).collect(Collectors.joining(";"));
        properties.put(PropertyAnalyzer.PROPERTIES_FOREIGN_KEY_CONSTRAINT, newProperty);
        tableProperty.modifyTableProperties(properties);
        tableProperty.setForeignKeyConstraints(foreignKeyConstraints);
    }

    public boolean getUseFastSchemaEvolution() {
        if (hasRowStorageType()) {
            // row storage type does not support fast schema evolution currently
            return false;
        }
        if (tableProperty != null) {
            return tableProperty.getUseFastSchemaEvolution();
        }
        return false;
    }

    public void setUseFastSchemaEvolution(boolean useFastSchemaEvolution) {
        if (tableProperty == null) {
            tableProperty = new TableProperty(new HashMap<>());
        }
        tableProperty.modifyTableProperties(PropertyAnalyzer.PROPERTIES_USE_FAST_SCHEMA_EVOLUTION,
                Boolean.valueOf(useFastSchemaEvolution).toString());
        tableProperty.buildUseFastSchemaEvolution();
    }

    public void setSessionId(UUID sessionId) {
        this.sessionId = sessionId;
    }

    public UUID getSessionId() {
        return sessionId;
    }

    @Override
    public void onReload() {
        analyzePartitionInfo();
        analyzeRollupIndexMeta();
        tryToAssignIndexId();
        updateBaseCompactionForbiddenTimeRanges(false);

        // register constraints from global state manager
        GlobalConstraintManager globalConstraintManager = GlobalStateMgr.getCurrentState().getGlobalConstraintManager();
        globalConstraintManager.registerConstraint(this);
    }

    @Override
    public void onCreate(Database db) {
        super.onCreate(db);

        ColocateTableIndex colocateTableIndex = GlobalStateMgr.getCurrentState().getColocateTableIndex();
        if (colocateTableIndex.isColocateTable(getId())) {
            ColocateTableIndex.GroupId groupId = colocateTableIndex.getGroup(getId());
            List<List<Long>> backendsPerBucketSeq = colocateTableIndex.getBackendsPerBucketSeq(groupId);
            ColocatePersistInfo colocatePersistInfo = ColocatePersistInfo.createForAddTable(groupId, getId(),
                    backendsPerBucketSeq);
            GlobalStateMgr.getCurrentState().getEditLog().logColocateAddTable(colocatePersistInfo);
        }

        DynamicPartitionUtil.registerOrRemovePartitionScheduleInfo(db.getId(), this);

        if (Config.dynamic_partition_enable && getTableProperty().getDynamicPartitionProperty().isEnabled()) {
            new Thread(() -> {
                try {
                    GlobalStateMgr.getCurrentState().getDynamicPartitionScheduler()
                            .executeDynamicPartitionForTable(db.getId(), getId());
                } catch (Exception ex) {
                    LOG.warn("Some problems were encountered in the process of triggering " +
                            "the execution of dynamic partitioning", ex);
                }
            }, "BackgroundDynamicPartitionThread").start();
        }

        if (isTemporaryTable()) {
            TemporaryTableMgr temporaryTableMgr = GlobalStateMgr.getCurrentState().getTemporaryTableMgr();
            temporaryTableMgr.addTemporaryTable(sessionId, db.getId(), name, id);
            LOG.debug("add temporary table, name[{}] id[{}] session[{}]", name, id, sessionId);
        }
    }

    private void analyzePartitionInfo() {
        if (!(partitionInfo instanceof ExpressionRangePartitionInfo)) {
            return;
        }
        ExpressionRangePartitionInfo expressionRangePartitionInfo = (ExpressionRangePartitionInfo) partitionInfo;
        // currently, automatic partition only supports one expression
        Expr partitionExpr = expressionRangePartitionInfo.getPartitionExprs(idToColumn).get(0);
        // for Partition slot ref, the SlotDescriptor is not serialized, so should
        // recover it here.
        // the SlotDescriptor is used by toThrift, which influences the execution
        // process.
        List<SlotRef> slotRefs = Lists.newArrayList();
        partitionExpr.collect(SlotRef.class, slotRefs);
        Preconditions.checkState(slotRefs.size() == 1);
        // schema change should update slot id
        for (int i = 0; i < fullSchema.size(); i++) {
            Column column = fullSchema.get(i);
            if (column.getName().equalsIgnoreCase(slotRefs.get(0).getColumnName())) {
                SlotDescriptor slotDescriptor = new SlotDescriptor(new SlotId(i), column.getName(),
                        column.getType(), column.isAllowNull());
                slotRefs.get(0).setDesc(slotDescriptor);
            }
        }
    }

    /**
     * Analyze defined expr of columns in rollup index meta.
     */
    private void analyzeRollupIndexMeta() {
        if (indexIdToMeta.size() <= 1) {
            return;
        }
        TableName tableName = new TableName(null, getName());
        ConnectContext session = ConnectContext.buildInner();
        Optional<SelectAnalyzer.SlotRefTableNameCleaner> visitorOpt = Optional.empty();
        for (MaterializedIndexMeta indexMeta : indexIdToMeta.values()) {
            if (indexMeta.getIndexId() == baseIndexId) {
                continue;
            }
            List<Column> columns = indexMeta.getSchema();
            // Note: Add this try-catch block to avoid the failure of analyzing rollup index meta since this
            // method will be called in replay method.
            // And the failure of analyzing rollup index meta will only affect the associated synchronized mvs and
            // will not affect the whole system.
            try {
                for (Column column : columns) {
                    Expr definedExpr = column.getDefineExpr();
                    if (definedExpr == null) {
                        continue;
                    }
                    if (visitorOpt.isEmpty()) {
                        SelectAnalyzer.SlotRefTableNameCleaner visitor = MVUtils.buildSlotRefTableNameCleaner(
                                session, this, tableName);
                        visitorOpt = Optional.of(visitor);
                    }
                    Preconditions.checkArgument(visitorOpt.isPresent(), "visitor should not be null");
                    definedExpr.accept(visitorOpt.get(), null);
                    ExpressionAnalyzer.analyzeExpression(definedExpr, new AnalyzeState(),
                            new Scope(RelationId.anonymous(),
                                    new RelationFields(this.getBaseSchema().stream()
                                            .map(col -> new Field(col.getName(), col.getType(),
                                                    tableName, null))
                                            .collect(Collectors.toList()))), session);
                }
            } catch (Exception e) {
                LOG.warn("Analyze rollup index meta failed, index id: {}, table:{}", indexMeta.getIndexId(), getName(), e);
            }
        }
    }

    // Remove all Tablets belonging to this table from TabletInvertedIndex
    public void removeTabletsFromInvertedIndex() {
        TabletInvertedIndex invertedIndex = GlobalStateMgr.getCurrentState().getTabletInvertedIndex();
        Collection<Partition> allPartitions = getAllPartitions();
        for (Partition partition : allPartitions) {
            for (PhysicalPartition subPartition : partition.getSubPartitions()) {
                for (MaterializedIndex index : subPartition.getMaterializedIndices(MaterializedIndex.IndexExtState.ALL)) {
                    for (Tablet tablet : index.getTablets()) {
                        invertedIndex.deleteTablet(tablet.getId());
                    }
                }
            }
        }
    }

    // If you are modifying this function, please check if you need to modify LakeTable.onDrop also.
    @Override
    public void onDrop(Database db, boolean force, boolean replay) {
        super.onDrop(db, force, replay);

        // drop all temp partitions of this table, so that there is no temp partitions
        // in recycle bin,
        // which make things easier.
        dropAllTempPartitions();
        if (!replay && hasAutoIncrementColumn()) {
            sendDropAutoIncrementMapTask();
        }

        updateBaseCompactionForbiddenTimeRanges(true);

        // unregister constraints from global state manager
        GlobalConstraintManager globalConstraintManager = GlobalStateMgr.getCurrentState().getGlobalConstraintManager();
        globalConstraintManager.unRegisterConstraint(this);
    }

    public void removeTableBinds(boolean isReplay) {
        GlobalStateMgr.getCurrentState().getLocalMetastore().removeAutoIncrementIdByTableId(getId(), isReplay);
        GlobalStateMgr.getCurrentState().getColocateTableIndex().removeTable(getId(), this, isReplay);
        GlobalStateMgr.getCurrentState().getStorageVolumeMgr().unbindTableToStorageVolume(getId());
    }

    @Override
    public boolean delete(long dbId, boolean isReplay) {
        removeTableBinds(isReplay);
        removeTabletsFromInvertedIndex();
        if (!isReplay) {
            TabletTaskExecutor.deleteAllReplicas(this);
        }
        return true;
    }

    @Override
    public boolean isSupported() {
        return true;
    }

    public AlterJobV2Builder alterTable() {
        return new OlapTableAlterJobV2Builder(this);
    }

    public AlterJobV2Builder rollUp() {
        return new OlapTableRollupJobBuilder(this);
    }

    public OptimizeJobV2Builder optimizeTable() {
        return new OptimizeJobV2Builder(this);
    }

    @Override
    public Map<String, String> getProperties() {
        // common properties for olap table, cloud native table
        // use TreeMap to ensure the order of keys, such as for show create table.
        Map<String, String> properties = Maps.newTreeMap();

        // common properties
        properties.putAll(getCommonProperties());

        // unique properties
        properties.putAll(getUniqueProperties());

        return properties;
    }

    /**
     * Get common properties for olap table, cloud native table.
     */
    public Map<String, String> getCommonProperties() {
        Map<String, String> properties = Maps.newTreeMap();
        // replication num
        properties.put(PropertyAnalyzer.PROPERTIES_REPLICATION_NUM, getDefaultReplicationNum().toString());

        // bloom filter
        Set<String> bfColumnNames = getBfColumnNames();
        if (bfColumnNames != null && !bfColumnNames.isEmpty()) {
            properties.put(PropertyAnalyzer.PROPERTIES_BF_COLUMNS, Joiner.on(", ").join(bfColumnNames));
        }

        // colocate group
        String colocateGroup = getColocateGroup();
        if (colocateGroup != null) {
            properties.put(PropertyAnalyzer.PROPERTIES_COLOCATE_WITH, colocateGroup);
        }

        // dynamic partition
        if (dynamicPartitionExists()) {
            properties.putAll(tableProperty.getDynamicPartitionProperty().getProperties());
        }

        // automatic bucket
        Long bucketSize = getAutomaticBucketSize();
        if (bucketSize > 0) {
            properties.put(PropertyAnalyzer.PROPERTIES_BUCKET_SIZE, bucketSize.toString());
        }

        // mutable bucket num
        Long mutableBucketNum = getMutableBucketNum();
        if (mutableBucketNum > 0) {
            properties.put(PropertyAnalyzer.PROPERTIES_MUTABLE_BUCKET_NUM, mutableBucketNum.toString());
        }

        // enable load profile
        Boolean enableLoadProfile = enableLoadProfile();
        if (enableLoadProfile) {
            properties.put(PropertyAnalyzer.PROPERTIES_ENABLE_LOAD_PROFILE, "true");
        }

        // base compaction forbidden time ranges
        if (!getBaseCompactionForbiddenTimeRanges().isEmpty()) {
            properties.put(PropertyAnalyzer.PROPERTIES_BASE_COMPACTION_FORBIDDEN_TIME_RANGES,
                    getBaseCompactionForbiddenTimeRanges());
        }

        // locations
        Multimap<String, String> locationsMap = getLocation();
        if (locationsMap != null) {
            String locations = PropertyAnalyzer.convertLocationMapToString(locationsMap);
            properties.put(PropertyAnalyzer.PROPERTIES_LABELS_LOCATION, locations);
        }

        // primary key
        if (keysType == KeysType.PRIMARY_KEYS) {
            // persistent index
            properties.put(PropertyAnalyzer.PROPERTIES_ENABLE_PERSISTENT_INDEX, enablePersistentIndex().toString());

            // index cache expire
            int indexCacheExpireSec = primaryIndexCacheExpireSec();
            if (indexCacheExpireSec > 0) {
                properties.put(PropertyAnalyzer.PROPERTIES_PRIMARY_INDEX_CACHE_EXPIRE_SEC, String.valueOf(indexCacheExpireSec));
            }
        }

        if (isFileBundling() && isCloudNativeTable()) {
            properties.put(PropertyAnalyzer.PROPERTIES_FILE_BUNDLING, isFileBundling().toString());
        }

        if (getCompactionStrategy() != TCompactionStrategy.DEFAULT) {
            properties.put(PropertyAnalyzer.PROPERTIES_COMPACTION_STRATEGY, 
                                TableProperty.compactionStrategyToString(getCompactionStrategy()));
        }

        Map<String, String> tableProperties = tableProperty != null ? tableProperty.getProperties() : Maps.newLinkedHashMap();
        // partition live number
        String partitionLiveNumber = tableProperties.get(PropertyAnalyzer.PROPERTIES_PARTITION_LIVE_NUMBER);
        if (partitionLiveNumber != null) {
            properties.put(PropertyAnalyzer.PROPERTIES_PARTITION_LIVE_NUMBER, partitionLiveNumber);
        }

        // partition ttl
        if (tableProperties.containsKey(PropertyAnalyzer.PROPERTIES_PARTITION_TTL)) {
            properties.put(
                    PropertyAnalyzer.PROPERTIES_PARTITION_TTL, tableProperties.get(PropertyAnalyzer.PROPERTIES_PARTITION_TTL));
        }

        // compression type
        TCompressionType compressionType = getCompressionType();
        if (compressionType == TCompressionType.LZ4_FRAME) {
            compressionType = TCompressionType.LZ4;
        }
        int compressionLevel = getCompressionLevel();
        String compressionTypeName = compressionType.name();
        if (compressionLevel != -1) {
            compressionTypeName = compressionTypeName + "(" + String.valueOf(compressionLevel) + ")";
        }
        properties.put(PropertyAnalyzer.PROPERTIES_COMPRESSION, compressionTypeName);

        // unique constraint
        String uniqueConstraint = tableProperties.get(PropertyAnalyzer.PROPERTIES_UNIQUE_CONSTRAINT);
        if (!Strings.isNullOrEmpty(uniqueConstraint)) {
            properties.put(PropertyAnalyzer.PROPERTIES_UNIQUE_CONSTRAINT,
                    UniqueConstraint.getShowCreateTableConstraintDesc(getTableProperty().getUniqueConstraints(), this));
        }

        // foreign key constraint
        String foreignKeyConstraint = tableProperties.get(PropertyAnalyzer.PROPERTIES_FOREIGN_KEY_CONSTRAINT);
        if (!Strings.isNullOrEmpty(foreignKeyConstraint) && hasForeignKeyConstraints()) {
            properties.put(PropertyAnalyzer.PROPERTIES_FOREIGN_KEY_CONSTRAINT,
                    ForeignKeyConstraint.getShowCreateTableConstraintDesc(this, getForeignKeyConstraints()));
        }
        String timeDriftConstraintSpec = tableProperties.get(PropertyAnalyzer.PROPERTIES_TIME_DRIFT_CONSTRAINT);
        if (!Strings.isNullOrEmpty(timeDriftConstraintSpec)) {
            properties.put(PropertyAnalyzer.PROPERTIES_TIME_DRIFT_CONSTRAINT, timeDriftConstraintSpec);
        }

        // partition_retention_condition
        String partitionRetentionCondition = tableProperties.get(PropertyAnalyzer.PROPERTIES_PARTITION_RETENTION_CONDITION);
        if (!Strings.isNullOrEmpty(partitionRetentionCondition)) {
            properties.put(PropertyAnalyzer.PROPERTIES_PARTITION_RETENTION_CONDITION, partitionRetentionCondition);
        }
        return properties;
    }

    // unique properties for olap table, cloud native table
    public Map<String, String> getUniqueProperties() {
        Map<String, String> properties = Maps.newHashMap();
        Map<String, String> tableProperties = tableProperty != null ? tableProperty.getProperties() : Maps.newLinkedHashMap();

        // storage medium
        String storageMedium = tableProperties.get(PropertyAnalyzer.PROPERTIES_STORAGE_MEDIUM);
        if (storageMedium != null) {
            properties.put(PropertyAnalyzer.PROPERTIES_STORAGE_MEDIUM, storageMedium);
        }

        // storage cooldown ttl
        String storageCooldownTtl = tableProperties.get(PropertyAnalyzer.PROPERTIES_STORAGE_COOLDOWN_TTL);
        if (storageCooldownTtl != null) {
            properties.put(PropertyAnalyzer.PROPERTIES_STORAGE_COOLDOWN_TTL, storageCooldownTtl);
        }

        // replicated storage
        properties.put(PropertyAnalyzer.PROPERTIES_REPLICATED_STORAGE, enableReplicatedStorage().toString());

        // binlog
        if (containsBinlogConfig()) {
            // binlog_version
            BinlogConfig binlogConfig = getCurBinlogConfig();
            properties.put(PropertyAnalyzer.PROPERTIES_BINLOG_VERSION, String.valueOf(binlogConfig.getVersion()));
            // binlog_enable
            properties.put(PropertyAnalyzer.PROPERTIES_BINLOG_ENABLE, String.valueOf(binlogConfig.getBinlogEnable()));
            // binlog_ttl
            properties.put(PropertyAnalyzer.PROPERTIES_BINLOG_TTL, String.valueOf(binlogConfig.getBinlogTtlSecond()));
            // binlog_max_size
            properties.put(PropertyAnalyzer.PROPERTIES_BINLOG_MAX_SIZE, String.valueOf(binlogConfig.getBinlogMaxSize()));
        }

        // write quorum
        TWriteQuorumType writeQuorumType = writeQuorum();
        if (writeQuorumType != TWriteQuorumType.MAJORITY) {
            properties.put(PropertyAnalyzer.PROPERTIES_WRITE_QUORUM, WriteQuorum.writeQuorumToName(writeQuorumType));
        }

        // fast schema evolution
        boolean useFastSchemaEvolution = getUseFastSchemaEvolution();
        properties.put(PropertyAnalyzer.PROPERTIES_USE_FAST_SCHEMA_EVOLUTION, String.valueOf(useFastSchemaEvolution));

        // storage type
        if (storageType() != null && !PropertyAnalyzer.PROPERTIES_STORAGE_TYPE_COLUMN.equalsIgnoreCase(storageType())) {
            properties.put(PropertyAnalyzer.PROPERTIES_STORAGE_TYPE, storageType());
        }

        // flat json enable
        String flatJsonEnable = tableProperties.get(PropertyAnalyzer.PROPERTIES_FLAT_JSON_ENABLE);
        if (!Strings.isNullOrEmpty(flatJsonEnable)) {
            properties.put(PropertyAnalyzer.PROPERTIES_FLAT_JSON_ENABLE, flatJsonEnable);
        }

        // flat json null factor
        String flatJsonNullFactor = tableProperties.get(PropertyAnalyzer.PROPERTIES_FLAT_JSON_NULL_FACTOR);
        if (!Strings.isNullOrEmpty(flatJsonNullFactor)) {
            properties.put(PropertyAnalyzer.PROPERTIES_FLAT_JSON_NULL_FACTOR, flatJsonNullFactor);
        }

        // flat json sparsity factor
        String flatJsonSparsityFactor = tableProperties.get(PropertyAnalyzer.PROPERTIES_FLAT_JSON_SPARSITY_FACTOR);
        if (!Strings.isNullOrEmpty(flatJsonSparsityFactor)) {
            properties.put(PropertyAnalyzer.PROPERTIES_FLAT_JSON_SPARSITY_FACTOR, flatJsonSparsityFactor);
        }

        // flat json column max
        String flatJsonColumnMax = tableProperties.get(PropertyAnalyzer.PROPERTIES_FLAT_JSON_COLUMN_MAX);
        if (!Strings.isNullOrEmpty(flatJsonColumnMax)) {
            properties.put(PropertyAnalyzer.PROPERTIES_FLAT_JSON_COLUMN_MAX, flatJsonColumnMax);
        }

        return properties;
    }

    @Override
    public boolean supportsUpdate() {
        return getKeysType() == KeysType.PRIMARY_KEYS;
    }

    @Override
    public boolean supportInsert() {
        return true;
    }

    public boolean hasRowStorageType() {
        return TStorageType.ROW == getStorageType() || TStorageType.COLUMN_WITH_ROW == getStorageType();
    }

    // ------ for lake table and lake materialized view start ------
    @Nullable
    public FilePathInfo getDefaultFilePathInfo() {
        StorageInfo storageInfo = tableProperty != null ? tableProperty.getStorageInfo() : null;
        return storageInfo != null ? storageInfo.getFilePathInfo() : null;
    }

    @Nullable
    public FilePathInfo getPartitionFilePathInfo(long physicalPartitionId) {
        FilePathInfo pathInfo = getDefaultFilePathInfo();
        if (pathInfo != null) {
            return StarOSAgent.allocatePartitionFilePathInfo(pathInfo, physicalPartitionId);
        }
        return null;
    }

    public FileCacheInfo getPartitionFileCacheInfo(long partitionId) {
        throw new SemanticException("getPartitionFileCacheInfo is not supported");
    }

    public void setStorageInfo(FilePathInfo pathInfo, DataCacheInfo dataCacheInfo) {
        throw new SemanticException("setStorageInfo is not supported");
    }

    /**
     * Check if data cache is allowed for the specified partition's data:
     * - If the partition is NOT partitioned by DATE or DATETIME, data cache is allowed
     * - If the partition is partitioned by DATE or DATETIME:
     * - if the partition's end value (of type DATE/DATETIME) is within the last "datacache.partition_duration"
     * duration, allow data cache for the partition.
     * - otherwise, disallow the data cache for the partition
     *
     * @param partition the partition to check. the partition must belong to this table.
     * @return true if the partition is enabled for the data cache, false otherwise
     */
    public boolean isEnableFillDataCache(Partition partition) {
        try {
            return isEnableFillDataCacheImpl(Objects.requireNonNull(partition, "partition is null"));
        } catch (AnalysisException ignored) {
            return true;
        }
    }

    // Read indexes and assign indexId for some compatible reasons when upgrade from old version
    private void tryToAssignIndexId() {
        if (this.indexes != null && !this.indexes.getIndexes().isEmpty()) {
            this.maxIndexId = Math.max(
                    this.indexes.getIndexes().stream()
                            .filter(index -> IndexType.isCompatibleIndex(index.getIndexType()))
                            .mapToLong(Index::getIndexId).max().orElse(-1),
                    this.maxIndexId);
            this.indexes.getIndexes().stream()
                    .filter(index -> IndexType.isCompatibleIndex(index.getIndexType()) && index.getIndexId() < 0)
                    .forEach(index -> index.setIndexId(this.incAndGetMaxIndexId()));

        }
    }

    private boolean isEnableFillDataCacheImpl(Partition partition) throws AnalysisException {
        if (tableProperty == null) {
            return true;
        }

        PeriodDuration cacheDuration = tableProperty.getDataCachePartitionDuration();
        if (cacheDuration == null) {
            return true;
        }

        if (getPartitionInfo().isRangePartition()) {
            RangePartitionInfo rangePartitionInfo = (RangePartitionInfo) getPartitionInfo();
            Range<PartitionKey> partitionRange = rangePartitionInfo.getRange(partition.getId());
            Range<PartitionKey> dataCacheRange;
            if (rangePartitionInfo.isPartitionedBy(this, PrimitiveType.DATETIME) ||
                    rangePartitionInfo.isPartitionedBy(this, PrimitiveType.DATE)) {
                try {
                    LocalDateTime upper = LocalDateTime.now();
                    LocalDateTime lower = upper.minus(cacheDuration);
                    dataCacheRange = Range.openClosed(PartitionKey.ofDateTime(lower), PartitionKey.ofDateTime(upper));
                    return partitionRange.isConnected(dataCacheRange);
                } catch (Exception e) {
                    LOG.warn("Table name: {}, Partition name: {}, Datacache.partiton_duration: {}, Failed to check the " +
                                    " validaity of range partition. Error: {}.", super.name, partition.getName(),
                            cacheDuration.toString(), e.getMessage());
                    return false;
                }
            }
        } else if (getPartitionInfo().isListPartition()) {
            ListPartitionInfo listPartitionInfo = (ListPartitionInfo) getPartitionInfo();
            List<Column> columns = partitionInfo.getPartitionColumns(this.idToColumn);
            int dateTypeColumnIdx = ListUtils.indexOf(columns, column -> column.getPrimitiveType().isDateType());

            if (dateTypeColumnIdx == -1) {
                // List partition has no date type column.
                return true;
            }

            LocalDateTime upper = LocalDateTime.now();
            LocalDateTime lower = upper.minus(cacheDuration);
            List<List<String>> multiValues = listPartitionInfo.getIdToMultiValues().get(partition.getId());
            List<String> values = listPartitionInfo.getIdToValues().get(partition.getId());
            try {
                if (multiValues != null) {
                    for (List<String> multivalue : multiValues) {
                        LocalDateTime partitionTime = DateUtils.parseDatTimeString(multivalue.get(dateTypeColumnIdx));
                        if (lower.isBefore(partitionTime) && (partitionTime.isBefore(upper) || partitionTime.isEqual(upper))) {
                            return true;
                        }
                    }
                }
                if (values != null) {
                    for (String value : values) {
                        LocalDateTime partitionTime = DateUtils.parseDatTimeString(value);
                        if (lower.isBefore(partitionTime) && (partitionTime.isBefore(upper) || partitionTime.isEqual(upper))) {
                            return true;
                        }
                    }
                }
                return false;
            } catch (Exception e) {
                LOG.warn("Table name: {}, Partition name: {}, Datacache.partiton_duration: {}, Failed to check the " +
                                "validaity of list partition. Error: {}.", super.name, partition.getName(),
                        cacheDuration.toString(), e.getMessage());
                return false;
            }
        }

        return true;
    }
    // ------ for lake table and lake materialized view end ------

    public void lockCreatePartition(String partitionName) {
        Lock locker = null;
        synchronized (createPartitionLocks) {
            locker = createPartitionLocks.get(partitionName);
            if (locker == null) {
                locker = new ReentrantLock();
                createPartitionLocks.put(partitionName, locker);
            }
        }
        locker.lock();
    }

    public void unlockCreatePartition(String partitionName) {
        Lock locker = null;
        synchronized (createPartitionLocks) {
            locker = createPartitionLocks.get(partitionName);
        }
        if (locker != null) {
            locker.unlock();
        }
    }

    public int getPartitionsCount() {
        return physicalPartitionIdToPartitionId.size();
    }

    public PhysicalPartition getPartitionSample() {
        if (!idToPartition.isEmpty()) {
            return idToPartition.values().iterator().next().getSubPartitions().iterator().next();
        } else {
            return null;
        }
    }

    public long getLastCollectProfileTime() {
        return lastCollectProfileTime;
    }

    public void updateLastCollectProfileTime() {
        this.lastCollectProfileTime = System.currentTimeMillis();
    }

    /**
     * Return partition name and associate partition cell with specific partition columns.
     * If partitionColumnsOpt is empty, return partition cell with all partition columns.
     */
    public Map<String, PCell> getPartitionCells(Optional<List<Column>> partitionColumnsOpt) {
        PartitionInfo partitionInfo = this.getPartitionInfo();
        if (partitionInfo.isUnPartitioned()) {
            return null;
        }
        if (partitionInfo.isRangePartition()) {
            Preconditions.checkArgument(partitionColumnsOpt.isEmpty() || partitionColumnsOpt.get().size() == 1);
            Map<String, Range<PartitionKey>> rangeMap = getRangePartitionMap();
            if (rangeMap == null) {
                return null;
            }
            return rangeMap.entrySet().stream().collect(Collectors.toMap(x -> x.getKey(), x -> new PRangeCell(x.getValue())));
        } else if (partitionInfo.isListPartition()) {
            Map<String, PListCell> listMap = getListPartitionItems(partitionColumnsOpt);
            if (listMap == null) {
                return null;
            }
            return listMap.entrySet().stream().collect(Collectors.toMap(x -> x.getKey(), x -> x.getValue()));
        }
        return null;
    }

    public boolean allowUpdateFileBundling() {
        for (Partition partition : getPartitions()) {
            for (PhysicalPartition physicalPartition : partition.getSubPartitions()) {
                if (physicalPartition.getMetadataSwitchVersion() != 0) {
                    return false;
                }
            }
        }
        return true;
    }
}

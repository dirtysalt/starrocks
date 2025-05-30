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

package com.starrocks.sql.optimizer.rule.transformation;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.DistributionInfo;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.HashDistributionInfo;
import com.starrocks.catalog.MaterializedIndexMeta;
import com.starrocks.catalog.OlapTable;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.Projection;
import com.starrocks.sql.optimizer.operator.logical.LogicalAggregationOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.CastOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rewrite.ReplaceColumnRefRewriter;
import com.starrocks.sql.optimizer.rewrite.ScalarOperatorRewriter;
import com.starrocks.sql.optimizer.rewrite.scalar.ReduceCastRule;
import com.starrocks.sql.optimizer.rule.RuleType;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

// for Aggregation on Aggregate type table or index,
// if group keys contain all aggregate keys, partition keys and distribution keys,
// and the aggregation functions are the same as the aggregate type of the target value columns in table
// then the Aggregation can be remove and the preAggregate should be off for OlapScanOperator
public class RemoveAggregationFromAggTable extends TransformationRule {
    private static final List<String> UNSUPPORTED_FUNCTION_NAMES =
            ImmutableList.of(FunctionSet.BITMAP_UNION,
                    FunctionSet.BITMAP_UNION_COUNT,
                    FunctionSet.HLL_UNION,
                    FunctionSet.HLL_UNION_AGG,
                    FunctionSet.PERCENTILE_UNION);

    public RemoveAggregationFromAggTable() {
        super(RuleType.TF_REMOVE_AGGREGATION_BY_AGG_TABLE,
                Pattern.create(OperatorType.LOGICAL_AGGR).addChildren(Pattern.create(
                        OperatorType.LOGICAL_OLAP_SCAN)));
    }

    @Override
    public boolean check(OptExpression input, OptimizerContext context) {
        OptExpression scanExpression = input.getInputs().get(0);
        LogicalOlapScanOperator scanOperator = (LogicalOlapScanOperator) scanExpression.getOp();
        if (scanOperator.getProjection() != null) {
            return false;
        }
        OlapTable olapTable = (OlapTable) scanOperator.getTable();

        MaterializedIndexMeta materializedIndexMeta = olapTable.getIndexMetaByIndexId(scanOperator.getSelectedIndexId());
        // must be Aggregation
        if (!materializedIndexMeta.getKeysType().isAggregationFamily()) {
            return false;
        }
        // random distribution table cannot remove aggregations
        DistributionInfo distributionInfo = olapTable.getDefaultDistributionInfo();
        if (distributionInfo == null || !(distributionInfo instanceof HashDistributionInfo)) {
            return false;
        }

        Set<String> keyColumnNames = Sets.newHashSet();
        List<Column> indexSchema = materializedIndexMeta.getSchema();
        for (Column column : indexSchema) {
            if (column.isKey()) {
                keyColumnNames.add(column.getName().toLowerCase());
            }
        }

        // check whether every aggregation function on column is the same as the AggregationType of the column
        LogicalAggregationOperator aggregationOperator = (LogicalAggregationOperator) input.getOp();
        for (Map.Entry<ColumnRefOperator, CallOperator> entry : aggregationOperator.getAggregations().entrySet()) {
            if (UNSUPPORTED_FUNCTION_NAMES.contains(entry.getValue().getFnName().toLowerCase())) {
                return false;
            }
            CallOperator callOperator = entry.getValue();
            if (callOperator.getChildren().size() != 1) {
                return false;
            }
            ScalarOperator argument = callOperator.getChild(0);
            if (!(argument instanceof ColumnRefOperator)) {
                return false;
            }
            ColumnRefOperator columnRefOperator = (ColumnRefOperator) argument;
            Optional<Column> columnOptional = indexSchema.stream()
                    .filter(column -> column.getName().equalsIgnoreCase(columnRefOperator.getName())).findFirst();
            if (!columnOptional.isPresent()) {
                return false;
            }
            Column column = columnOptional.get();
            if (column.getAggregationType() == null) {
                return false;
            }
            if (!column.getAggregationType().toString().equalsIgnoreCase(entry.getValue().getFnName())) {
                return false;
            }
        }

        // group by keys contain partition columns and distribution columns
        Set<String> groupKeyColumns = aggregationOperator.getGroupingKeys().stream()
                .map(columnRefOperator -> columnRefOperator.getName().toLowerCase()).collect(Collectors.toSet());
        Set<String> partitionColumnNames = olapTable.getPartitionInfo().getPartitionColumns(olapTable.getIdToColumn())
                .stream().map(column -> column.getName().toLowerCase()).collect(Collectors.toSet());
        Set<String> distributionColumnNames = olapTable.getDistributionColumnNames().stream()
                .map(String::toLowerCase).collect(Collectors.toSet());
        return groupKeyColumns.containsAll(keyColumnNames)
                && groupKeyColumns.containsAll(partitionColumnNames)
                && groupKeyColumns.containsAll(distributionColumnNames);
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        LogicalAggregationOperator aggregationOperator = (LogicalAggregationOperator) input.getOp();
        Map<ColumnRefOperator, ScalarOperator> projectMap = Maps.newHashMap();
        aggregationOperator.getGroupingKeys().forEach(g -> projectMap.put(g, g));
        for (Map.Entry<ColumnRefOperator, CallOperator> entry : aggregationOperator.getAggregations().entrySet()) {
            // in this case, CallOperator must have only one child ColumnRefOperator
            //
            // in AggTable,may metric definition is `col int sum`, but in query sum(col)'s type is bigint,
            // so after replace sum(col) wit col, we must guarantee the same ColumnRefOperator points to
            // the same type expr, so we must cast col as bigint, otherwise during executing, ExchangeSink would
            // send serialize the column as int column while the ExchangeSource would derserialize it in bigint
            // column.
            ColumnRefOperator columnRef = entry.getKey();
            CallOperator aggFunc = entry.getValue();
            ScalarOperator aggFuncArg = aggFunc.getChild(0);
            if (aggFunc.getType().equals(aggFuncArg.getType())) {
                projectMap.put(columnRef, aggFuncArg);
            } else {
                ScalarOperator newExpr = new ScalarOperatorRewriter().rewrite(
                        new CastOperator(aggFunc.getType(), aggFuncArg, true),
                        Lists.newArrayList(new ReduceCastRule()));
                projectMap.put(columnRef, newExpr);
            }
        }

        Map<ColumnRefOperator, ScalarOperator> newProjectMap = Maps.newHashMap();
        ReplaceColumnRefRewriter rewriter = new ReplaceColumnRefRewriter(projectMap);

        OptExpression newChildOpt = input.inputAt(0);
        if (aggregationOperator.getPredicate() != null) {
            // rewrite the having predicate. replace the aggFunc by the columnRef
            ScalarOperator newPredicate = rewriter.rewrite(aggregationOperator.getPredicate());
            LogicalOlapScanOperator scanOperator = (LogicalOlapScanOperator) newChildOpt.getOp();
            LogicalOlapScanOperator.Builder builder = new LogicalOlapScanOperator.Builder();
            List<ScalarOperator> pushDownPredicates = Lists.newArrayList();
            if (scanOperator.getPredicate() != null) {
                pushDownPredicates.add(scanOperator.getPredicate());
            }
            pushDownPredicates.add(newPredicate);
            scanOperator = builder.withOperator(scanOperator)
                    .setPredicate(Utils.compoundAnd(pushDownPredicates))
                    .build();
            newChildOpt = OptExpression.create(scanOperator, newChildOpt.getInputs());
        }

        if (aggregationOperator.getProjection() != null) {
            for (Map.Entry<ColumnRefOperator, ScalarOperator> entry :
                    aggregationOperator.getProjection().getColumnRefMap().entrySet()) {
                // rewrite the projection of this agg. replace the aggFunc by the columnRef
                ScalarOperator rewrittenOperator = rewriter.rewrite(entry.getValue());
                newProjectMap.put(entry.getKey(), rewrittenOperator);
            }
        } else {
            newProjectMap = projectMap;
        }

        newChildOpt.getOp().setProjection(new Projection(newProjectMap));
        return Lists.newArrayList(newChildOpt);
    }
}

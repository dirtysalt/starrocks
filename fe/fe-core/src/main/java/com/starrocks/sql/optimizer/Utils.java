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

package com.starrocks.sql.optimizer;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.starrocks.analysis.JoinOperator;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Function;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.KeysType;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.ScalarType;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Type;
import com.starrocks.common.Pair;
import com.starrocks.common.util.DebugUtil;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.base.LogicalProperty;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.Projection;
import com.starrocks.sql.optimizer.operator.logical.LogicalAggregationOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalDeltaLakeScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalHiveScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalHudiScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalIcebergScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalJoinOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalPaimonScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalSetOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalTreeAnchorOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalHashAggregateOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.CastOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.CompoundPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rewrite.ReplaceColumnRefRewriter;
import com.starrocks.sql.optimizer.rewrite.ScalarOperatorRewriter;
import com.starrocks.sql.optimizer.statistics.ColumnStatistic;
import com.starrocks.sql.optimizer.statistics.StatisticsCalculator;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.ListUtils;
import org.apache.commons.collections4.SetUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.roaringbitmap.RoaringBitmap;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static com.starrocks.qe.SessionVariableConstants.AggregationStage.AUTO;
import static com.starrocks.qe.SessionVariableConstants.AggregationStage.ONE_STAGE;
import static java.util.function.Function.identity;

public class Utils {
    private static final Logger LOG = LogManager.getLogger(Utils.class);

    public static List<ScalarOperator> extractConjuncts(ScalarOperator root) {
        LinkedList<ScalarOperator> list = new LinkedList<>();
        if (null == root) {
            return list;
        }
        extractConjunctsImpl(root, list);
        return list;
    }

    public static Set<ScalarOperator> extractConjunctSet(ScalarOperator root) {
        Set<ScalarOperator> list = Sets.newHashSet();
        if (null == root) {
            return list;
        }
        extractConjunctsImpl(root, list);
        return list;
    }

    private static void extractConjunctsImpl(ScalarOperator root, Collection<ScalarOperator> result) {
        if (!OperatorType.COMPOUND.equals(root.getOpType())) {
            result.add(root);
            return;
        }

        CompoundPredicateOperator cpo = (CompoundPredicateOperator) root;
        if (!cpo.isAnd()) {
            result.add(root);
            return;
        }
        extractConjunctsImpl(cpo.getChild(0), result);
        extractConjunctsImpl(cpo.getChild(1), result);
    }

    public static List<ScalarOperator> extractDisjunctive(ScalarOperator root) {
        LinkedList<ScalarOperator> list = new LinkedList<>();
        if (null == root) {
            return list;
        }
        extractDisjunctiveImpl(root, list);
        return list;
    }

    private static void extractDisjunctiveImpl(ScalarOperator root, List<ScalarOperator> result) {
        if (!OperatorType.COMPOUND.equals(root.getOpType())) {
            result.add(root);
            return;
        }

        CompoundPredicateOperator cpo = (CompoundPredicateOperator) root;
        if (!cpo.isOr()) {
            result.add(root);
            return;
        }
        extractDisjunctiveImpl(cpo.getChild(0), result);
        extractDisjunctiveImpl(cpo.getChild(1), result);
    }

    public static List<ColumnRefOperator> extractColumnRef(ScalarOperator root) {
        if (null == root) {
            return new LinkedList<>();
        }

        return root.getColumnRefs();
    }

    public static int countColumnRef(ScalarOperator root) {
        if (null == root) {
            return 0;
        }

        if (OperatorType.VARIABLE.equals(root.getOpType())) {
            return 1;
        }

        int count = 0;
        for (ScalarOperator child : root.getChildren()) {
            count += countColumnRef(child);
        }

        return count;
    }

    public static int countOptExpressionNodes(OptExpression node) {
        int count = 1;
        for (OptExpression child : node.getInputs()) {
            count += countOptExpressionNodes(child);
        }
        return count;
    }

    public static void extractOlapScanOperator(GroupExpression groupExpression, List<LogicalOlapScanOperator> list) {
        extractOperator(groupExpression, list, p -> OperatorType.LOGICAL_OLAP_SCAN.equals(p.getOpType()));
    }

    public static List<PhysicalOlapScanOperator> extractPhysicalOlapScanOperator(OptExpression root) {
        List<PhysicalOlapScanOperator> list = Lists.newArrayList();
        extractOperator(root, list, op -> OperatorType.PHYSICAL_OLAP_SCAN.equals(op.getOpType()));
        return list;
    }

    public static <E extends Operator> void extractOperator(OptExpression root, List<E> list,
                                                            Predicate<Operator> lambda) {
        if (lambda.test(root.getOp())) {
            list.add((E) root.getOp());
            return;
        }

        List<OptExpression> inputs = root.getInputs();
        for (OptExpression input : inputs) {
            extractOperator(input, list, lambda);
        }
    }

    private static <E extends Operator> void extractOperator(GroupExpression root, List<E> list,
                                                             Predicate<Operator> lambda) {
        if (lambda.test(root.getOp())) {
            list.add((E) root.getOp());
            return;
        }

        List<Group> groups = root.getInputs();
        for (Group group : groups) {
            GroupExpression expression = group.getFirstLogicalExpression();
            extractOperator(expression, list, lambda);
        }
    }

    public static boolean containAnyColumnRefs(List<ColumnRefOperator> refs, ScalarOperator operator) {
        if (refs.isEmpty() || null == operator) {
            return false;
        }

        if (operator.isColumnRef()) {
            return refs.contains(operator);
        }

        for (ScalarOperator so : operator.getChildren()) {
            if (containAnyColumnRefs(refs, so)) {
                return true;
            }
        }

        return false;
    }

    public static boolean containColumnRef(ScalarOperator operator, String column) {
        if (null == column || null == operator) {
            return false;
        }

        if (operator.isColumnRef()) {
            return ((ColumnRefOperator) operator).getName().equalsIgnoreCase(column);
        }

        for (ScalarOperator so : operator.getChildren()) {
            if (containColumnRef(so, column)) {
                return true;
            }
        }

        return false;
    }

    public static ScalarOperator compoundOr(Collection<ScalarOperator> nodes) {
        return createCompound(CompoundPredicateOperator.CompoundType.OR, nodes);
    }

    public static ScalarOperator compoundOr(ScalarOperator... nodes) {
        return createCompound(CompoundPredicateOperator.CompoundType.OR, Arrays.asList(nodes));
    }

    public static ScalarOperator compoundAnd(Collection<ScalarOperator> nodes) {
        return createCompound(CompoundPredicateOperator.CompoundType.AND, nodes);
    }

    public static ScalarOperator compoundAnd(ScalarOperator... nodes) {
        return createCompound(CompoundPredicateOperator.CompoundType.AND, Arrays.asList(nodes));
    }

    // Build a compound tree by bottom up
    //
    // Example: compoundType.OR
    // Initial state:
    //  a b c d e
    //
    // First iteration:
    //  or    or
    //  /\    /\   e
    // a  b  c  d
    //
    // Second iteration:
    //     or   e
    //    / \
    //  or   or
    //  /\   /\
    // a  b c  d
    //
    // Last iteration:
    //       or
    //      / \
    //     or  e
    //    / \
    //  or   or
    //  /\   /\
    // a  b c  d
    public static ScalarOperator createCompound(CompoundPredicateOperator.CompoundType type,
                                                Collection<ScalarOperator> nodes) {
        LinkedList<ScalarOperator> link =
                nodes.stream().filter(Objects::nonNull).collect(Collectors.toCollection(Lists::newLinkedList));

        if (link.size() < 1) {
            return null;
        }

        // for and predicate, filter redundant true
        if (type == CompoundPredicateOperator.CompoundType.AND) {
            link = link.stream()
                    .filter(so -> !ConstantOperator.TRUE.equals(so))
                    .collect(Collectors.toCollection(Lists::newLinkedList));
            if (link.isEmpty()) {
                return ConstantOperator.TRUE;
            }
        }
        if (link.size() == 1) {
            return link.get(0);
        }

        while (link.size() > 1) {
            LinkedList<ScalarOperator> buffer = new LinkedList<>();

            // combine pairs of elements
            while (link.size() >= 2) {
                buffer.add(new CompoundPredicateOperator(type, link.poll(), link.poll()));
            }

            // if there's and odd number of elements, just append the last one
            if (!link.isEmpty()) {
                buffer.add(link.remove());
            }

            // continue processing the pairs that were just built
            link = buffer;
        }
        return link.remove();
    }

    public static int countJoinNodeSize(OptExpression root, Set<JoinOperator> joinTypes) {
        int count = 0;
        Operator operator = root.getOp();
        for (OptExpression child : root.getInputs()) {
            if (isSuitableJoin(operator, joinTypes)) {
                count += countJoinNodeSize(child, joinTypes);
            } else {
                count = Math.max(count, countJoinNodeSize(child, joinTypes));
            }
        }

        if (isSuitableJoin(operator, joinTypes)) {
            count += 1;
        }
        return count;
    }

    private static boolean isSuitableJoin(Operator operator, Set<JoinOperator> joinTypes) {
        if (operator instanceof LogicalJoinOperator) {
            LogicalJoinOperator joinOperator = (LogicalJoinOperator) operator;
            return joinTypes.contains(joinOperator.getJoinType()) && joinOperator.getJoinHint().isEmpty();
        }
        return false;
    }

    public static boolean capableOuterReorder(OptExpression root, int threshold) {
        boolean[] hasOuterOrSemi = {false};
        int totalJoinNodes = countJoinNode(root, hasOuterOrSemi);
        return totalJoinNodes < threshold && hasOuterOrSemi[0];
    }

    private static int countJoinNode(OptExpression root, boolean[] hasOuterOrSemi) {
        int count = 0;
        Operator operator = root.getOp();
        for (OptExpression child : root.getInputs()) {
            if (operator instanceof LogicalJoinOperator && ((LogicalJoinOperator) operator).getJoinHint().isEmpty()) {
                count += countJoinNode(child, hasOuterOrSemi);
            } else {
                count = Math.max(count, countJoinNode(child, hasOuterOrSemi));
            }
        }

        if (operator instanceof LogicalJoinOperator && ((LogicalJoinOperator) operator).getJoinHint().isEmpty()) {
            count += 1;
            if (!hasOuterOrSemi[0]) {
                LogicalJoinOperator joinOperator = (LogicalJoinOperator) operator;
                if (joinOperator.getJoinType().isOuterJoin() || joinOperator.getJoinType().isSemiAntiJoin()) {
                    hasOuterOrSemi[0] = true;
                }
            }
        }
        return count;
    }

    public static boolean hasPrunableJoin(OptExpression expression) {
        if (expression.getOp() instanceof LogicalJoinOperator) {
            LogicalJoinOperator joinOp = expression.getOp().cast();
            JoinOperator joinType = joinOp.getJoinType();
            return joinType.isInnerJoin() || joinType.isCrossJoin() ||
                    joinType.isLeftOuterJoin() || joinType.isRightOuterJoin();
        }
        return expression.getInputs().stream().anyMatch(Utils::hasPrunableJoin);
    }

    public static boolean hasUnknownColumnsStats(OptExpression root) {
        Operator operator = root.getOp();
        if (operator instanceof LogicalScanOperator) {
            LogicalScanOperator scanOperator = (LogicalScanOperator) operator;
            List<String> colNames =
                    scanOperator.getColRefToColumnMetaMap().values().stream().map(Column::getName).collect(
                            Collectors.toList());
            if (operator instanceof LogicalOlapScanOperator) {
                Table table = scanOperator.getTable();
                if (table instanceof OlapTable) {
                    if (KeysType.AGG_KEYS.equals(((OlapTable) table).getKeysType())) {
                        List<String> keyColumnNames =
                                scanOperator.getColRefToColumnMetaMap().values().stream().filter(Column::isKey)
                                        .map(Column::getName)
                                        .collect(Collectors.toList());
                        List<ColumnStatistic> keyColumnStatisticList =
                                GlobalStateMgr.getCurrentState().getStatisticStorage().getColumnStatistics(table, keyColumnNames);
                        return keyColumnStatisticList.stream().anyMatch(ColumnStatistic::isUnknown);
                    }
                }
                List<ColumnStatistic> columnStatisticList =
                        GlobalStateMgr.getCurrentState().getStatisticStorage().getColumnStatistics(table, colNames);
                return columnStatisticList.stream().anyMatch(ColumnStatistic::isUnknown);
            } else if (operator instanceof LogicalHiveScanOperator || operator instanceof LogicalHudiScanOperator) {
                if (ConnectContext.get().getSessionVariable().enableHiveColumnStats()) {
                    if (operator instanceof LogicalHiveScanOperator) {
                        return ((LogicalHiveScanOperator) operator).hasUnknownColumn();
                    } else {
                        return ((LogicalHudiScanOperator) operator).hasUnknownColumn();
                    }
                }
                return true;
            } else if (operator instanceof LogicalIcebergScanOperator) {
                return ((LogicalIcebergScanOperator) operator).hasUnknownColumn();
            } else if (operator instanceof LogicalDeltaLakeScanOperator)  {
                return ((LogicalDeltaLakeScanOperator) operator).hasUnknownColumn();
            } else if (operator instanceof LogicalPaimonScanOperator) {
                return ((LogicalPaimonScanOperator) operator).hasUnknownColumn();
            } else {
                // For other scan operators, we do not know the column statistics.
                return true;
            }
        }

        return root.getInputs().stream().anyMatch(Utils::hasUnknownColumnsStats);
    }

    public static long getLongFromDateTime(LocalDateTime dateTime) {
        return dateTime.atZone(ZoneId.systemDefault()).toInstant().getEpochSecond();
    }

    public static LocalDateTime getDatetimeFromLong(long dateTime) {
        return LocalDateTime.ofInstant(Instant.ofEpochSecond(dateTime), ZoneId.systemDefault());
    }

    public static long convertBitSetToLong(BitSet bitSet, int length) {
        long gid = 0;
        for (int b = 0; b < length; ++b) {
            gid = gid * 2 + (bitSet.get(b) ? 1 : 0);
        }
        return gid;
    }

    public static ColumnRefOperator findSmallestColumnRefFromTable(Map<ColumnRefOperator, Column> colRefToColumnMetaMap,
                                                                   Table table) {
        Set<Column> baseSchema = new HashSet<>(table.getBaseSchema());
        List<ColumnRefOperator> visibleColumnRefs = colRefToColumnMetaMap.entrySet().stream()
                .filter(e -> baseSchema.contains(e.getValue()))
                .map(Map.Entry::getKey)
                .collect(Collectors.toList());
        return findSmallestColumnRef(visibleColumnRefs);
    }

    public static ColumnRefOperator findSmallestColumnRef(List<ColumnRefOperator> columnRefOperatorList) {
        if (CollectionUtils.isEmpty(columnRefOperatorList)) {
            return null;
        }
        ColumnRefOperator smallestColumnRef = columnRefOperatorList.get(0);
        int smallestColumnLength = Integer.MAX_VALUE;
        for (ColumnRefOperator columnRefOperator : columnRefOperatorList) {
            Type columnType = columnRefOperator.getType();
            if (columnType.isScalarType() && !columnType.isInvalid() && !columnType.isUnknown()) {
                int columnLength = columnType.getTypeSize();
                if (columnLength < smallestColumnLength) {
                    smallestColumnRef = columnRefOperator;
                    smallestColumnLength = columnLength;
                }
            }
        }
        return smallestColumnRef;
    }

    public static boolean isEqualBinaryPredicate(ScalarOperator predicate) {
        if (predicate instanceof BinaryPredicateOperator) {
            BinaryPredicateOperator binaryPredicate = (BinaryPredicateOperator) predicate;
            return binaryPredicate.getBinaryType().isEquivalence();
        }
        if (predicate instanceof CompoundPredicateOperator) {
            CompoundPredicateOperator compoundPredicate = (CompoundPredicateOperator) predicate;
            if (compoundPredicate.isAnd()) {
                return isEqualBinaryPredicate(compoundPredicate.getChild(0)) &&
                        isEqualBinaryPredicate(compoundPredicate.getChild(1));
            }
            return false;
        }
        return false;
    }

    /**
     * Try cast op to descType, return empty if failed
     */
    public static Optional<ScalarOperator> tryCastConstant(ScalarOperator op, Type descType) {
        // Forbidden cast float, because behavior isn't same with before
        if (!op.isConstantRef() || op.getType().matchesType(descType) || Type.FLOAT.equals(op.getType())
                || descType.equals(Type.FLOAT)) {
            return Optional.empty();
        }

        if (((ConstantOperator) op).isNull()) {
            return Optional.of(ConstantOperator.createNull(descType));
        }

        Optional<ConstantOperator> result = ((ConstantOperator) op).castToStrictly(descType);
        if (result.isEmpty()) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("invalid value: {} to type {}", op, descType);
            }
            return Optional.empty();
        }
        if (result.get().toString().equalsIgnoreCase(op.toString())) {
            return Optional.of(result.get());
        } else if (descType.isDate() && (op.getType().isIntegerType() || op.getType().isStringType())) {
            if (op.toString().equalsIgnoreCase(result.get().toString().replaceAll("-", ""))) {
                return Optional.of(result.get());
            }
        }
        return Optional.empty();
    }

    // tryDecimalCastConstant is employed by ReduceCastRule to reduce BinaryPredicateOperator involving DecimalV3
    // ReduceCastRule try to reduce 'CAST(Expr<T> as U) BINOP LITERAL<S>' to
    // 'EXPR<T> BINOP CAST(LITERAL<S> as T>', only T->U casting and S->T casting are both legal, then this
    // reduction is legal, so for numerical types, S is not wider than T and T is not wider than U. for examples:
    //     CAST(IntLiteral(100,TINYINT) as DECIMAL32(9,9)) < IntLiteral(0x7f50, SMALLINT) cannot be reduced.
    //     CAST(IntLiteral(100,SMALLINT) as DECIMAL64(13,10)) < IntLiteral(101, TINYINT) can be reduced.
    public static Optional<ScalarOperator> tryDecimalCastConstant(CastOperator lhs, ConstantOperator rhs) {
        Type lhsType = lhs.getType();
        Type rhsType = rhs.getType();
        Type childType = lhs.getChild(0).getType();

        // Only handle Integer or DecimalV3 types
        if (!lhsType.isExactNumericType() ||
                !rhsType.isExactNumericType() ||
                !childType.isExactNumericType()) {
            return Optional.empty();
        }
        // Guarantee that both childType casting to lhsType and rhsType casting to childType are
        // lossless
        if (!Type.isAssignable2Decimal((ScalarType) lhsType, (ScalarType) childType)) {
            return Optional.empty();
        }

        if (rhs.isNull()) {
            return Optional.of(ConstantOperator.createNull(childType));
        }

        Optional<ConstantOperator> result = rhs.castTo(childType);
        if (result.isEmpty()) {
            return Optional.empty();
        }
        if (Type.isAssignable2Decimal((ScalarType) childType, (ScalarType) rhsType)) {
            return Optional.of(result.get());
        } else if (result.get().toString().equalsIgnoreCase(rhs.toString())) {
            // check lossless
            return Optional.of(result.get());
        }
        return Optional.empty();
    }

    public static ScalarOperator transTrue2Null(ScalarOperator predicates) {
        if (ConstantOperator.TRUE.equals(predicates)) {
            return null;
        }
        return predicates;
    }

    public static <T extends ScalarOperator> List<T> collect(ScalarOperator root, Class<T> clazz) {
        List<T> output = Lists.newArrayList();
        collect(root, clazz, output);
        return output;
    }

    private static <T extends ScalarOperator> void collect(ScalarOperator root, Class<T> clazz, List<T> output) {
        if (clazz.isInstance(root)) {
            output.add(clazz.cast(root));
        }

        root.getChildren().forEach(child -> collect(child, clazz, output));
    }

    /**
     * Compute the maximal power-of-two number which is less than or equal to the given number.
     */
    public static int computeMaxLEPower2(int num) {
        num |= (num >>> 1);
        num |= (num >>> 2);
        num |= (num >>> 4);
        num |= (num >>> 8);
        num |= (num >>> 16);
        return num - (num >>> 1);
    }

    /**
     * Compute the maximal power-of-two number which is less than or equal to the given number.
     */
    public static int computeMinGEPower2(int num) {
        num -= 1;
        num |= (num >>> 1);
        num |= (num >>> 2);
        num |= (num >>> 4);
        num |= (num >>> 8);
        num |= (num >>> 16);
        return num < 0 ? 1 : num + 1;
    }

    public static int log2(int n) {
        return 31 - Integer.numberOfLeadingZeros(n);
    }

    public static long log2(long n) {
        return 63 - Long.numberOfLeadingZeros(n);
    }

    /**
     * Check the input expression is not nullable or not.
     * @param nullOutputColumnOps the nullable column reference operators.
     * @param expression the input expression.
     * @return true if the expression is not nullable, otherwise false.
     */
    public static boolean canEliminateNull(Set<ColumnRefOperator> nullOutputColumnOps, ScalarOperator expression) {
        try {
            Map<ColumnRefOperator, ScalarOperator> m = nullOutputColumnOps.stream()
                    .map(op -> new ColumnRefOperator(op.getId(), op.getType(), op.getName(), true))
                    .collect(Collectors.toMap(identity(), col -> ConstantOperator.createNull(col.getType())));
            for (ScalarOperator e : Utils.extractConjuncts(expression)) {
                ScalarOperator nullEval = new ReplaceColumnRefRewriter(m).rewrite(e);
                ScalarOperatorRewriter scalarRewriter = new ScalarOperatorRewriter();
                // Call the ScalarOperatorRewriter function to perform constant folding
                nullEval = scalarRewriter.rewrite(nullEval, ScalarOperatorRewriter.DEFAULT_REWRITE_RULES);
                // Only result is `null` or false, which means the expression "xxx is null" can never be true,
                // it can eliminate null.
                if (nullEval.isConstantRef() && ((ConstantOperator) nullEval).isNull()) {
                    return true;
                } else if (nullEval.equals(ConstantOperator.FALSE)) {
                    return true;
                }
            }
        } catch (Throwable e) {
            LOG.warn("[query_id={}] Failed to eliminate null: {}",
                    DebugUtil.getSessionQueryId(), DebugUtil.getStackTrace(e));
            return false;
        }
        return false;
    }

    public static boolean isNotAlwaysNullResultWithNullScalarOperator(ScalarOperator scalarOperator) {
        for (ScalarOperator child : scalarOperator.getChildren()) {
            if (isNotAlwaysNullResultWithNullScalarOperator(child)) {
                return true;
            }
        }

        if (scalarOperator.isColumnRef() || scalarOperator.isConstantRef() || scalarOperator instanceof CastOperator) {
            return false;
        } else if (scalarOperator instanceof CallOperator) {
            Function fn = ((CallOperator) scalarOperator).getFunction();
            if (fn == null) {
                return true;
            }
            if (!GlobalStateMgr.getCurrentState()
                    .isNotAlwaysNullResultWithNullParamFunction(fn.getFunctionName().getFunction())
                    && !fn.isUdf()
                    && !FunctionSet.ASSERT_TRUE.equals(fn.getFunctionName().getFunction())) {
                return false;
            }
        }
        return true;
    }

    // RoaringBitmap can be considered as a Set<Integer> contains only unsigned integers,
    // so getIntStream() resembles to Set<Integer>::stream()
    public static Stream<Integer> getIntStream(RoaringBitmap bitmap) {
        Spliterator<Integer> iter = Spliterators.spliteratorUnknownSize(bitmap.iterator(), Spliterator.ORDERED);
        return StreamSupport.stream(iter, false);
    }

    public static Set<Pair<ColumnRefOperator, ColumnRefOperator>> getJoinEqualColRefPairs(OptExpression joinOp) {
        Pair<List<BinaryPredicateOperator>, List<ScalarOperator>> onPredicates =
                JoinHelper.separateEqualPredicatesFromOthers(joinOp);
        List<BinaryPredicateOperator> eqOnPredicates = onPredicates.first;
        List<ScalarOperator> otherOnPredicates = onPredicates.second;

        if (!otherOnPredicates.isEmpty() || eqOnPredicates.isEmpty()) {
            return Collections.emptySet();
        }
        Set<Pair<ColumnRefOperator, ColumnRefOperator>> eqColumnRefPairs = Sets.newHashSet();
        for (BinaryPredicateOperator eqPredicate : eqOnPredicates) {
            ColumnRefOperator leftCol = eqPredicate.getChild(0).cast();
            ColumnRefOperator rightCol = eqPredicate.getChild(1).cast();
            eqColumnRefPairs.add(Pair.create(leftCol, rightCol));
        }
        return eqColumnRefPairs;
    }

    public static Map<ColumnRefOperator, ColumnRefOperator> makeEqColumRefMapFromSameTables(
            LogicalScanOperator lhsScanOp, LogicalScanOperator rhsScanOp) {
        Preconditions.checkArgument(lhsScanOp.getTable().getId() == rhsScanOp.getTable().getId());
        Set<Column> lhsColumns = lhsScanOp.getColumnMetaToColRefMap().keySet();
        Set<Column> rhsColumns = rhsScanOp.getColumnMetaToColRefMap().keySet();
        Preconditions.checkArgument(lhsColumns.equals(rhsColumns));
        Map<ColumnRefOperator, ColumnRefOperator> eqColumnRefs = Maps.newHashMap();
        for (Column column : lhsColumns) {
            ColumnRefOperator lhsColRef = lhsScanOp.getColumnMetaToColRefMap().get(column);
            ColumnRefOperator rhsColRef = rhsScanOp.getColumnMetaToColRefMap().get(column);
            eqColumnRefs.put(Objects.requireNonNull(lhsColRef), Objects.requireNonNull(rhsColRef));
        }
        return eqColumnRefs;
    }

    public static boolean couldGenerateMultiStageAggregate(LogicalProperty inputLogicalProperty,
                                                           Operator inputOp, Operator childOp) {
        // 1. check if must generate multi stage aggregate.
        if (mustGenerateMultiStageAggregate(inputOp, childOp)) {
            return true;
        }

        // 2. Respect user hint
        int aggStage = ConnectContext.get().getSessionVariable().getNewPlannerAggStage();
        if (aggStage == ONE_STAGE.ordinal() ||
                (aggStage == AUTO.ordinal() && inputLogicalProperty.oneTabletProperty().supportOneTabletOpt)) {
            return false;
        }

        return true;
    }

    public static boolean mustGenerateMultiStageAggregate(Operator inputOp, Operator childOp) {
        // Must do two stage aggregate if child operator is RepeatOperator
        // If the repeat node is used as the input node of the Exchange node.
        // Will cause the node to be unable to confirm whether it is const during serialization
        // (BE does this for efficiency reasons).
        // Therefore, it is forcibly ensured that no one-stage aggregation nodes are generated
        // on top of the repeat node.
        if (OperatorType.LOGICAL_REPEAT.equals(childOp.getOpType()) || OperatorType.PHYSICAL_REPEAT.equals(childOp.getOpType())) {
            return true;
        }

        Map<ColumnRefOperator, CallOperator> aggs = Maps.newHashMap();
        if (OperatorType.LOGICAL_AGGR.equals(inputOp.getOpType())) {
            aggs = ((LogicalAggregationOperator) inputOp).getAggregations();
        } else if (OperatorType.PHYSICAL_HASH_AGG.equals(inputOp.getOpType())) {
            aggs = ((PhysicalHashAggregateOperator) inputOp).getAggregations();
        }

        for (CallOperator callOperator : aggs.values()) {
            if (callOperator.isDistinct()) {
                String fnName = callOperator.getFnName();
                List<ScalarOperator> children = callOperator.getChildren();
                if (children.size() > 1 || children.stream().anyMatch(c -> c.getType().isComplexType())) {
                    return true;
                }
                if (FunctionSet.GROUP_CONCAT.equalsIgnoreCase(fnName) || FunctionSet.AVG.equalsIgnoreCase(fnName)) {
                    return true;
                } else if (FunctionSet.ARRAY_AGG.equalsIgnoreCase(fnName))  {
                    if (children.size() > 1 || children.get(0).getType().isDecimalOfAnyVersion()) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    // 1. without distinct function, the common distinctCols is an empty list.
    // 2. If has some distinct function, but distinct columns are not exactly same, ruturn Optional.empty
    // 3. If has some distinct function and distinct columns are exactly same or only one distinct function, return Optional.of(distinctCols)
    public static Optional<List<ColumnRefOperator>> extractCommonDistinctCols(
            Collection<CallOperator> aggCallOperators) {
        Set<ColumnRefOperator> distinctChildren = Sets.newHashSet();
        for (CallOperator callOperator : aggCallOperators) {
            if (callOperator.isDistinct()) {
                if (distinctChildren.isEmpty()) {
                    distinctChildren = Sets.newHashSet(callOperator.getColumnRefs());
                } else {
                    Set<ColumnRefOperator> nextDistinctChildren = Sets.newHashSet(callOperator.getColumnRefs());
                    if (!SetUtils.isEqualSet(distinctChildren, nextDistinctChildren)) {
                        return Optional.empty();
                    }
                }
            }
        }
        return Optional.of(Lists.newArrayList(distinctChildren));
    }

    // like select array_agg(distinct LO_REVENUE), count(distinct LO_REVENUE) will return true
    public static Boolean hasMultipleDistinctFuncShareSameDistinctColumns(Collection<CallOperator> aggCallOperators) {
        List<CallOperator> distinctFuncs =
                aggCallOperators.stream().filter(CallOperator::isDistinct).collect(Collectors.toList());
        if (distinctFuncs.size() <= 1) {
            return false;
        }
        Set<ColumnRefOperator> distinctChildren = Sets.newHashSet();
        for (CallOperator callOperator : aggCallOperators) {
            if (distinctChildren.isEmpty()) {
                distinctChildren = Sets.newHashSet(callOperator.getColumnRefs());
            } else {
                Set<ColumnRefOperator> nextDistinctChildren = Sets.newHashSet(callOperator.getColumnRefs());
                if (!SetUtils.isEqualSet(distinctChildren, nextDistinctChildren)) {
                    return false;
                }
            }
        }

        return true;
    }

    public static boolean hasNonDeterministicFunc(ScalarOperator operator) {
        for (ScalarOperator child : operator.getChildren()) {
            if (child instanceof CallOperator) {
                CallOperator call = (CallOperator) child;
                String fnName = call.getFnName();
                if (FunctionSet.nonDeterministicFunctions.contains(fnName)) {
                    return true;
                }
            }

            if (hasNonDeterministicFunc(child)) {
                return true;
            }
        }
        return false;
    }

    public static void calculateStatistics(OptExpression expr, OptimizerContext context) {
        for (OptExpression child : expr.getInputs()) {
            calculateStatistics(child, context);
        }
        // Do not calculate statistics for LogicalTreeAnchorOperator
        if (expr.getOp() instanceof LogicalTreeAnchorOperator) {
            return;
        }

        ExpressionContext expressionContext = new ExpressionContext(expr);
        StatisticsCalculator statisticsCalculator = new StatisticsCalculator(
                expressionContext, context.getColumnRefFactory(), context);
        try {
            statisticsCalculator.estimatorStats();
        } catch (Exception e) {
            LOG.warn("[query={}] Failed to calculate statistics for expression: {}",
                    DebugUtil.getSessionQueryId(), expr, e);
            return;
        }

        expr.setStatistics(expressionContext.getStatistics());
    }

    /**
     * Add new project into input, merge input's existing project if input has one.
     * @param input input expression
     * @param newProjectionMap new project map to be pushed down into input
     * @return a new expression with new project
     */
    public static OptExpression mergeProjection(OptExpression input,
                                                Map<ColumnRefOperator, ScalarOperator> newProjectionMap) {
        if (newProjectionMap == null || newProjectionMap.isEmpty()) {
            return input;
        }
        Operator newOp = input.getOp();
        if (newOp.getProjection() == null || newOp.getProjection().getColumnRefMap().isEmpty()) {
            newOp.setProjection(new Projection(newProjectionMap));
        } else {
            // merge two projections
            ReplaceColumnRefRewriter rewriter = new ReplaceColumnRefRewriter(newOp.getProjection().getColumnRefMap());
            Map<ColumnRefOperator, ScalarOperator> resultMap = Maps.newHashMap();
            for (Map.Entry<ColumnRefOperator, ScalarOperator> entry : newProjectionMap.entrySet()) {
                ScalarOperator result = rewriter.rewrite(entry.getValue());
                resultMap.put(entry.getKey(), result);
            }
            newOp.setProjection(new Projection(resultMap));
        }
        return input;
    }

    /**
     * Check if the optExpression has applied the rule in recursively
     * @param optExpression input optExpression to be checked
     * @param ruleMask specific rule mask
     * @return true if the optExpression or its children have applied the rule, false otherwise
     */
    public static boolean isOptHasAppliedRule(OptExpression optExpression, int ruleMask) {
        return isOptHasAppliedRule(optExpression, op -> op.isOpRuleBitSet(ruleMask));
    }

    public static boolean isOptHasAppliedRule(OptExpression optExpression, Predicate<Operator> pred) {
        if (optExpression == null) {
            return false;
        }
        if (pred.test(optExpression.getOp())) {
            return true;
        }
        for (OptExpression child : optExpression.getInputs()) {
            if (isOptHasAppliedRule(child, pred)) {
                return true;
            }
        }
        return false;
    }

    @SuppressWarnings("unchecked")
    public static <T, S extends T> Optional<S> downcast(T obj, Class<S> klass) {
        Preconditions.checkArgument(obj != null);
        if (obj.getClass().equals(Objects.requireNonNull(klass))) {
            return Optional.of((S) obj);
        } else {
            return Optional.empty();
        }
    }

    public static <T, S extends T> S mustCast(T obj, Class<S> klass) {
        return downcast(obj, klass)
                .orElseThrow(() -> new IllegalArgumentException("Cannot cast " + obj.getClass() + " to " + klass));
    }

    // this method is useful when map is small, but key is very complex  like compound predicate with 1000 OR
    // in which case key's hashCode() can be super slow because of bad time complexity
    // so we can use equals' short-circuit logic to help us find whether key is in map quickly
    // which means key's type is not same as map's key's types
    public static <K, V> V getValueIfExists(Map<K, V> map, K key) {
        V value = null;

        if (map.size() < 4) {
            for (Map.Entry<K, V> entry : map.entrySet()) {
                if (entry.getKey().equals(key)) {
                    value = entry.getValue();
                    break;
                }
            }
        } else {
            value = map.get(key);
        }

        return value;
    }

    /**
     * Recursively resolve the column ref for complicated expressions
     * Throw exception if that ref cannot be found in the operator expression
     *
     * @param ref     column ref
     * @param factory column factory
     * @param expr    operator expression
     * @return all resolved columns
     */
    public static List<Pair<Table, Column>> resolveColumnRefRecursive(ColumnRefOperator ref, ColumnRefFactory factory,
                                                                      OptExpression expr) {
        // consult the factory
        Pair<Table, Column> tableAndColumn = factory.getTableAndColumn(ref);
        if (tableAndColumn != null) {
            return List.of(tableAndColumn);
        }

        // When deriving stats, the OptExpression is not exist but only a GroupExpression. We cannot resolve the
        // ColumnRef from it
        if (expr == null) {
            return null;
        }

        // Consult the projection
        if (expr.getOp().getProjection() != null) {
            ScalarOperator impl = expr.getOp().getProjection().resolveColumnRef(ref);
            if (impl != null) {
                List<ColumnRefOperator> subRefs = Utils.extractColumnRef(impl);
                if (impl instanceof ColumnRefOperator) {
                    subRefs.remove(impl);
                }
                List<Pair<Table, Column>> subColumns = Lists.newArrayList();
                for (ColumnRefOperator subRef : subRefs) {
                    subColumns.addAll(ListUtils.emptyIfNull(resolveColumnRefRecursive(subRef, factory, expr)));
                }
                return subColumns;
            }
        }

        // Consult the corresponding children
        if (expr.getOp() instanceof LogicalSetOperator setOp) {
            List<ColumnRefOperator> childrenRefs = setOp.resolveColumnRef(ref);
            if (CollectionUtils.isNotEmpty(childrenRefs)) {
                List<Pair<Table, Column>> result = Lists.newArrayList();
                for (int i = 0; i < expr.getInputs().size(); i++) {
                    List<Pair<Table, Column>> pairs =
                            resolveColumnRefRecursive(childrenRefs.get(i), factory, expr.getInputs().get(i));
                    if (CollectionUtils.isNotEmpty(pairs)) {
                        result.addAll(pairs);
                    }
                }
                return result;
            }

        }

        // consult children operators
        for (OptExpression child : expr.getInputs()) {
            List<Pair<Table, Column>> children = resolveColumnRefRecursive(ref, factory, child);
            if (CollectionUtils.isNotEmpty(children)) {
                return children;
            }
        }

        return null;
    }

    public static Pair<Map<ColumnRefOperator, ConstantOperator>, List<ScalarOperator>> separateEqualityPredicates(
            ScalarOperator predicate) {
        List<ScalarOperator> conjunctivePredicates = extractConjuncts(predicate);
        Map<ColumnRefOperator, ConstantOperator> columnConstMap = new HashMap<>();
        List<ScalarOperator> otherPredicates = new ArrayList<>();

        for (ScalarOperator op : conjunctivePredicates) {
            if (ScalarOperator.isColumnEqualConstant(op)) {
                BinaryPredicateOperator binaryOp = (BinaryPredicateOperator) op;
                ColumnRefOperator column = (ColumnRefOperator) binaryOp.getChild(0);
                ConstantOperator constant = (ConstantOperator) binaryOp.getChild(1);
                columnConstMap.put(column, constant);
            } else {
                otherPredicates.add(op);
            }
        }

        return new Pair<>(columnConstMap, otherPredicates);
    }
}

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

package com.starrocks.sql.analyzer;

import com.starrocks.analysis.CastExpr;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.StringLiteral;
import com.starrocks.catalog.Function;
import com.starrocks.catalog.PrimitiveType;
import com.starrocks.catalog.ScalarType;
import com.starrocks.catalog.Type;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.plan.PlanTestBase;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

public class VariantFunctionsTest extends PlanTestBase {

    @Test
    public void testVariantQueryFunctionSignature() {
        // Test that variant_query function exists and has correct signature
        Type variantType = ScalarType.createType(PrimitiveType.VARIANT);
        Type varcharType = ScalarType.createType(PrimitiveType.VARCHAR);

        Type[] argumentTypes = {variantType, varcharType};
        Function fnVariantQuery = Expr.getBuiltinFunction("variant_query", argumentTypes,
                Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF);

        Assertions.assertThat(fnVariantQuery)
                .as("variant_query function should exist")
                .isNotNull();
        Assertions.assertThat(fnVariantQuery.functionName())
                .isEqualTo("variant_query");
        Assertions.assertThat(fnVariantQuery.getReturnType())
                .isEqualTo(variantType);
        Assertions.assertThat(fnVariantQuery.getArgs())
                .hasSize(2);
        Assertions.assertThat(fnVariantQuery.getArgs()[0])
                .isEqualTo(variantType);
        Assertions.assertThat(fnVariantQuery.getArgs()[1])
                .isEqualTo(varcharType);
    }

    @Test
    public void testVariantTypeCastingRestrictions() {
        // Test the core issue: varchar cannot be cast to VARIANT
        Type varcharType = ScalarType.createVarchar(1048576);
        Type variantType = ScalarType.createType(PrimitiveType.VARIANT);
        Type nullType = Type.NULL;
        Type intType = ScalarType.createType(PrimitiveType.INT);

        // These should NOT be allowed (current behavior causing the error)
        Assertions.assertThat(Type.canCastTo(varcharType, variantType))
                .as("VARCHAR should not be castable to VARIANT")
                .isFalse();
        Assertions.assertThat(Type.canCastTo(intType, variantType))
                .as("INT should not be castable to VARIANT")
                .isFalse();

        // These SHOULD be allowed
        Assertions.assertThat(Type.canCastTo(nullType, variantType))
                .as("NULL should be castable to VARIANT")
                .isTrue();
        Assertions.assertThat(Type.canCastTo(variantType, variantType))
                .as("VARIANT should be castable to itself")
                .isTrue();
        Assertions.assertThat(Type.canCastTo(variantType, nullType))
                .as("VARIANT should be castable to NULL")
                .isTrue();
    }

    @Test
    public void testVariantCastExpressionError() {
        // Reproduce the exact error scenario from the stack trace
        Type varcharType = ScalarType.createVarchar(1048576);
        Type variantType = ScalarType.createType(PrimitiveType.VARIANT);

        StringLiteral varcharLiteral = new StringLiteral("test_statistics_data");
        varcharLiteral.setType(varcharType);

        CastExpr castExpr = new CastExpr(variantType, varcharLiteral);
        ExpressionAnalyzer.Visitor visitor = new ExpressionAnalyzer.Visitor(
                new AnalyzeState(), new ConnectContext());

        // This should throw the exact SemanticException mentioned in the error
        Assertions.assertThatThrownBy(() -> visitor.visitCastExpr(castExpr,
                        new Scope(RelationId.anonymous(), new RelationFields())))
                .isInstanceOf(SemanticException.class)
                .hasMessageContaining("Invalid type cast")
                .hasMessageContaining("varchar")
                .hasMessageContaining("variant");
    }

    @Test
    public void testVariantQueryWithNullInputs() {
        // Test variant_query function behavior with NULL inputs
        Type variantType = ScalarType.createType(PrimitiveType.VARIANT);
        Type varcharType = ScalarType.createType(PrimitiveType.VARCHAR);
        Type nullType = Type.NULL;

        // Test with NULL variant argument
        Type[] argsNullVariant = {nullType, varcharType};
        Function fnNullVariant = Expr.getBuiltinFunction("variant_query", argsNullVariant,
                Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF);
        Assertions.assertThat(fnNullVariant)
                .as("Should handle NULL variant input")
                .isNotNull();

        // Test with NULL path argument
        Type[] argsNullPath = {variantType, nullType};
        Function fnNullPath = Expr.getBuiltinFunction("variant_query", argsNullPath,
                Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF);
        Assertions.assertThat(fnNullPath)
                .as("Should handle NULL path input")
                .isNotNull();
    }

    @Test
    public void testVariantQueryErrorScenarios() {
        // Test invalid argument combinations for variant_query
        Type variantType = ScalarType.createType(PrimitiveType.VARIANT);
        Type intType = ScalarType.createType(PrimitiveType.INT);

        // Wrong number of arguments
        Type[] wrongArgCount = {variantType};
        Function fnWrongArgs = Expr.getBuiltinFunction("variant_query", wrongArgCount,
                Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF);
        Assertions.assertThat(fnWrongArgs)
                .as("Should not find function with wrong argument count")
                .isNull();

        // Wrong argument types
        Type[] wrongArgTypes = {intType, intType};
        Function fnWrongTypes = Expr.getBuiltinFunction("variant_query", wrongArgTypes,
                Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF);
        Assertions.assertThat(fnWrongTypes)
                .as("Should not find function with wrong argument types")
                .isNull();
    }
}

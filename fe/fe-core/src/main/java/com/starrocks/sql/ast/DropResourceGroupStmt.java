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

package com.starrocks.sql.ast;

import com.starrocks.catalog.ResourceGroup;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.parser.NodePosition;

// Drop ResourceGroup specified by name
// DROP RESOURCE GROUP <name>
public class DropResourceGroupStmt extends DdlStmt {
    private final boolean ifExists;
    private final String name;

    public DropResourceGroupStmt(String name, boolean ifExists) {
        this(name, NodePosition.ZERO, ifExists);
    }

    public DropResourceGroupStmt(String name, NodePosition pos, boolean ifExists) {
        super(pos);
        this.name = name;
        this.ifExists = ifExists;
    }

    public String getName() {
        return name;
    }

    public boolean isIfExists() {
        return ifExists;
    }

    public void analyze() {
        if (ResourceGroup.BUILTIN_WG_NAMES.contains(name)) {
            throw new SemanticException(String.format("cannot drop builtin resource group [%s]", name));
        }
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitDropResourceGroupStatement(this, context);
    }
}

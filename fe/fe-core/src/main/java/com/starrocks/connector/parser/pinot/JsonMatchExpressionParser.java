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
package com.starrocks.connector.parser.pinot;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class JsonMatchExpressionParser {
    public static class JsonMatchCondition {
        private String jsonPath;
        private String operator;
        private Object value;
        private boolean isList;
        private List<String> valueList;
        private boolean isStringValue;

        public String getJsonPath() {
            return jsonPath;
        }

        public String getOperator() {
            return operator;
        }

        public Object getValue() {
            return value;
        }

        public boolean isList() {
            return isList;
        }

        public List<String> getValueList() {
            return valueList;
        }

        public boolean isStringValue() {
            return isStringValue;
        }

        @Override
        public String toString() {
            return "JsonMatchCondition{" +
                    "jsonPath='" + jsonPath + '\'' +
                    ", operator='" + operator + '\'' +
                    ", value=" + (isList ? valueList : value) +
                    ", isList=" + isList +
                    ", isStringValue=" + isStringValue +
                    '}';
        }
    }

    public static JsonMatchCondition parseJsonMatchExpression(String expression) {
        JsonMatchCondition condition = new JsonMatchCondition();

        // Extract the jsonPath (always starts with "$." or ends with "[*]")
        Pattern jsonPathPattern = Pattern.compile("\"(\\$\\.[^\"]+|[^\"]+\\[\\*\\])\"");
        Matcher jsonPathMatcher = jsonPathPattern.matcher(expression);
        if (jsonPathMatcher.find()) {
            condition.jsonPath = jsonPathMatcher.group(1);
        } else {
            throw new IllegalArgumentException("Invalid JSON path in expression: " + expression);
        }

        String lowerExpression = expression.toLowerCase();

        if (lowerExpression.contains("is not null")) {
            condition.operator = "IS NOT NULL";
        } else if (lowerExpression.contains("is null")) {
            condition.operator = "IS NULL";
        } else if (lowerExpression.contains("not in")) {
            condition.operator = "not in";
        } else if (lowerExpression.contains(" in ")) {
            condition.operator = "in";
        } else if (lowerExpression.contains("!=")) {
            condition.operator = "!=";
        } else if (lowerExpression.contains("=")) {
            condition.operator = "=";
        } else {
            throw new IllegalArgumentException("No valid operator found in expression: " + expression);
        }

        // Extract the value part based on operator
        if ("in".equals(condition.operator) || "not in".equals(condition.operator)) {
            // Handle list values for IN and NOT IN
            condition.isList = true;
            Pattern listPattern = Pattern.compile("\\(([^)]+)\\)");
            Matcher listMatcher = listPattern.matcher(expression);
            if (listMatcher.find()) {
                String listStr = listMatcher.group(1);
                // Check if values are enclosed in single quotes (strings)
                if (listStr.contains("'")) {
                    condition.isStringValue = true;
                    // Split by comma and handle escaped quotes
                    String[] items = listStr.split(",");
                    condition.valueList = new ArrayList<>();
                    for (String item : items) {
                        // Remove single quotes and trim
                        String cleaned = item.trim().replace("'", "");
                        condition.valueList.add(cleaned);
                    }
                } else {
                    // Numeric values
                    condition.isStringValue = false;
                    String[] items = listStr.split(",");
                    condition.valueList = new ArrayList<>();
                    for (String item : items) {
                        condition.valueList.add(item.trim());
                    }
                }
            } else {
                throw new IllegalArgumentException("Invalid list format in expression: " + expression);
            }
        } else {
            // Handle single values for = and !=
            condition.isList = false;
            if (expression.contains("'")) {
                // String value enclosed in double single quotes
                Pattern valuePattern = Pattern.compile(condition.operator + "\\s*'([^']*)'");
                Matcher valueMatcher = valuePattern.matcher(expression);
                if (valueMatcher.find()) {
                    condition.value = valueMatcher.group(1);
                    condition.isStringValue = true;
                }
            } else {
                // Numeric value
                Pattern valuePattern = Pattern.compile(condition.operator + "\\s*([0-9]+)");
                Matcher valueMatcher = valuePattern.matcher(expression);
                if (valueMatcher.find()) {
                    try {
                        condition.value = Integer.parseInt(valueMatcher.group(1));
                        condition.isStringValue = false;
                    } catch (NumberFormatException e) {
                        condition.value = valueMatcher.group(1);
                        condition.isStringValue = true;
                    }
                }
            }
        }
        return condition;
    }
}

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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.starrocks.analysis.ArithmeticExpr;
import com.starrocks.analysis.BinaryPredicate;
import com.starrocks.analysis.BinaryType;
import com.starrocks.analysis.CastExpr;
import com.starrocks.analysis.CompoundPredicate;
import com.starrocks.analysis.DecimalLiteral;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.FunctionCallExpr;
import com.starrocks.analysis.FunctionParams;
import com.starrocks.analysis.InPredicate;
import com.starrocks.analysis.IntLiteral;
import com.starrocks.analysis.IsNullPredicate;
import com.starrocks.analysis.StringLiteral;
import com.starrocks.analysis.TimestampArithmeticExpr;
import com.starrocks.catalog.ArrayType;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.Type;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.ArrayExpr;
import com.starrocks.sql.ast.IntervalLiteral;
import com.starrocks.sql.ast.UnitIdentifier;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ComplexFunctionCallTransformer {
    public static Expr transform(String functionName, Expr... args) {
        if (functionName.equalsIgnoreCase("datetimeconvert")) {
            List<Expr> argumentsList = Arrays.asList(args);
            if (argumentsList.size() < 4 || argumentsList.size() > 5) {
                throw new SemanticException("The datetimeconvert function must include between 4 and 5 parameters, inclusive.");
            }
            // DATETIMECONVERT(columnName, inputFormat, outputFormat, outputGranularity)
            // DATETIMECONVERT(columnName, inputFormat, outputFormat, outputGranularity, timeZone)
            // format is <time size>:<time unit>:<time format>:<pattern>, only works for time zie to be 1
            StringLiteral outputGranularity = (StringLiteral) args[3];
            List<String> granularity = PinotParserUtils.parseTime(outputGranularity.getValue());
            StringLiteral outputFormat = (StringLiteral) args[2];
            List<String> outputFormatList = PinotParserUtils.parseFormat(outputFormat.getValue());
            String timeFormat = outputFormatList.get(2);

            if (timeFormat.contains("EPOCH")) {
                IntervalLiteral intervalLiteral = new IntervalLiteral(new IntLiteral(Integer.parseInt(granularity.get(0))),
                        new UnitIdentifier(granularity.get(1)));
                FunctionCallExpr timeSlice = new FunctionCallExpr(FunctionSet.TIME_SLICE,
                        getArgumentsForTimeSlice(argumentsList.get(0),
                                intervalLiteral.getValue(), intervalLiteral.getUnitIdentifier().getDescription().toLowerCase(),
                                "floor"));

                FunctionCallExpr timeSliceTZ = timeSlice;
                if (argumentsList.size() == 5) {
                    timeSliceTZ = new FunctionCallExpr(FunctionSet.CONVERT_TZ,
                            new FunctionParams(ImmutableList.of(timeSlice, argumentsList.get(4), new StringLiteral("UTC"))));
                }

                FunctionCallExpr unixTimestamp = new FunctionCallExpr(FunctionSet.UNIX_TIMESTAMP,
                        new FunctionParams(ImmutableList.of(timeSliceTZ)));
                ArithmeticExpr outputTimeUnit = new ArithmeticExpr(ArithmeticExpr.Operator.MULTIPLY, unixTimestamp,
                        new DecimalLiteral(BigDecimal.valueOf(PinotParserUtils.getMultiplier(outputFormatList.get(1)))));
                return new FunctionCallExpr(FunctionSet.FLOOR, new FunctionParams(ImmutableList.of(outputTimeUnit)));

            } else {
                //parse the time pattern of the output
                String[] timePattern = PinotParserUtils.parseDateFormat(outputFormatList.get(3));
                String formatValue = PinotParserUtils.convertToStrftimeFormat(timePattern[0]);
                String timeZone = timePattern[1] == null ? "UTC" : timePattern[1];
                FunctionCallExpr convertTz = new FunctionCallExpr(FunctionSet.CONVERT_TZ,
                        new FunctionParams(ImmutableList.of(argumentsList.get(0),
                                new StringLiteral("UTC"), new StringLiteral(timeZone))));

                IntervalLiteral intervalLiteral = new IntervalLiteral(new IntLiteral(Integer.parseInt(granularity.get(0))),
                        new UnitIdentifier(granularity.get(1)));
                FunctionCallExpr timeSlice = new FunctionCallExpr(FunctionSet.TIME_SLICE,
                        getArgumentsForTimeSlice(convertTz,
                                intervalLiteral.getValue(), intervalLiteral.getUnitIdentifier().getDescription().toLowerCase(),
                                "floor"));

                FunctionCallExpr timeSliceTZ = timeSlice;
                if (argumentsList.size() == 5) {
                    timeSliceTZ = new FunctionCallExpr(FunctionSet.CONVERT_TZ,
                            new FunctionParams(ImmutableList.of(timeSlice, argumentsList.get(4), new StringLiteral("UTC"))));
                }

                return new FunctionCallExpr(FunctionSet.DATE_FORMAT,
                        new FunctionParams(ImmutableList.of(timeSliceTZ, new StringLiteral(formatValue))));
            }
        } else if (functionName.equalsIgnoreCase("datetrunc")) {
            if (args.length < 2 || args.length > 5) {
                throw new SemanticException("The datetrunc function must include between 2 and 5 parameters, inclusive.");
            }
            // DATETRUNC(unit, timeValue)  or DATETRUNC(unit, timeValue, inputTimeUnitStr) output is milliseconds -->  unix_timestamp(date_trunc('day',event_timestamp)) * 1000
            // DATETRUNC(unit, timeValue, inputTimeUnitStr, timeZone) output is milliseconds   -->  unix_timestamp(convertz_tz(date_trunc('day', event_timestamp), "UTC", timeZone)) * 1000
            // DATETRUNC(unit, timeValue, inputTimeUnitStr, timeZone, outputTimeUnitStr)  -→ unix_timestamp(convertz_tz(date_trunc('day', event_timestamp), "UTC", timeZone)),
            // output can be NANOSECONDS, MICROSECONDS, MILLISECONDS, SECONDS, MINUTES, HOURS, DAYS
            if (args.length < 4) {
                FunctionCallExpr dateTrunc = new FunctionCallExpr(FunctionSet.DATE_TRUNC,
                        new FunctionParams(ImmutableList.of(args[0], args[1])));
                FunctionCallExpr unixTimestamp = new FunctionCallExpr(FunctionSet.UNIX_TIMESTAMP,
                        new FunctionParams(ImmutableList.of(dateTrunc)));
                return new ArithmeticExpr(ArithmeticExpr.Operator.MULTIPLY, unixTimestamp,
                        new IntLiteral(1000));
            } else if (args.length == 4) {
                FunctionCallExpr dateTrunc = new FunctionCallExpr(FunctionSet.DATE_TRUNC,
                        new FunctionParams(ImmutableList.of(args[0], args[1])));
                FunctionCallExpr convertTz = new FunctionCallExpr(FunctionSet.CONVERT_TZ,
                        new FunctionParams(ImmutableList.of(dateTrunc, args[3],
                            new StringLiteral("UTC"))));
                FunctionCallExpr unixTimestamp = new FunctionCallExpr(FunctionSet.UNIX_TIMESTAMP,
                        new FunctionParams(ImmutableList.of(convertTz)));
                return new ArithmeticExpr(ArithmeticExpr.Operator.MULTIPLY, unixTimestamp,
                        new IntLiteral(1000));
            } else if (args.length == 5) {
                FunctionCallExpr dateTrunc = new FunctionCallExpr(FunctionSet.DATE_TRUNC,
                        new FunctionParams(ImmutableList.of(args[0], args[1])));
                FunctionCallExpr convertTz = new FunctionCallExpr(FunctionSet.CONVERT_TZ,
                        new FunctionParams(ImmutableList.of(dateTrunc, args[3],
                            new StringLiteral("UTC"))));
                FunctionCallExpr unixTimestamp = new FunctionCallExpr(FunctionSet.UNIX_TIMESTAMP,
                        new FunctionParams(ImmutableList.of(convertTz)));
                StringLiteral outputTimeUnitStr = (StringLiteral) args[4];
                ArithmeticExpr outputTimeUnit = new ArithmeticExpr(ArithmeticExpr.Operator.MULTIPLY, unixTimestamp,
                        new DecimalLiteral(BigDecimal.valueOf(PinotParserUtils.getMultiplier(outputTimeUnitStr.getStringValue()
                        ))));
                return new FunctionCallExpr(FunctionSet.FLOOR, new FunctionParams(ImmutableList.of(outputTimeUnit)));
            }
        } else if (functionName.equalsIgnoreCase("text_match")) {
            List<Expr> argumentsList = Arrays.asList(args);
            if (args.length < 2) {
                throw new SemanticException("The text_match function must include at least 2 parameters.");
            }
            List<String> parsedInput = parseInputString(((StringLiteral) args[1]).getValue());
            return buildPredicate(parsedInput, argumentsList);
        } else if (functionName.replace("_", "").equalsIgnoreCase("jsonextractscalar")) {
            List<Expr> argumentsList = Arrays.asList(args);
            if (args.length < 3) {
                throw new SemanticException("The jsonextractscalar function must include at least 3 parameters.");
            }
            if (args.length == 3) {
                FunctionCallExpr jsonQuery = new FunctionCallExpr(FunctionSet.JSON_QUERY,
                        new FunctionParams(ImmutableList.of(argumentsList.get(0), argumentsList.get(1))));
                jsonQuery.setType(Type.JSON);
                StringLiteral resultsType = (StringLiteral) argumentsList.get(2);
                return new CastExpr(PinotParserUtils.getScalarType(resultsType.getValue()), jsonQuery);
            } else if (args.length == 4) {
                FunctionCallExpr jsonQuery = new FunctionCallExpr(FunctionSet.JSON_QUERY,
                        new FunctionParams(ImmutableList.of(argumentsList.get(0), argumentsList.get(1))));
                jsonQuery.setType(Type.JSON);
                StringLiteral resultsType = (StringLiteral) argumentsList.get(2);

                CastExpr castExpr = new CastExpr(PinotParserUtils.getScalarType(resultsType.getValue()), jsonQuery);
                if (resultsType.getValue().toUpperCase().contains("ARRAY")) {
                    // if the 4th parameter is empty, it means that the result type is array
                    // so we need to cast the result to array type
                    // and the default value is empty array
                    if ((argumentsList.get(3) instanceof StringLiteral) &&
                            ((StringLiteral) argumentsList.get(3)).getValue().isEmpty()) {
                        return new FunctionCallExpr(FunctionSet.IFNULL,
                                new FunctionParams(ImmutableList.of(castExpr, new ArrayExpr(new ArrayType(
                                        PinotParserUtils.getScalarType(resultsType.getValue().split("_")[1])),
                                        Lists.newArrayList()))));
                    } else {
                        argumentsList.get(3).setType(PinotParserUtils.getScalarType(resultsType.getValue()));
                        return new FunctionCallExpr(FunctionSet.IFNULL,
                                new FunctionParams(ImmutableList.of(castExpr, argumentsList.get(3))));
                    }
                } else {
                    return new FunctionCallExpr(FunctionSet.IFNULL,
                            new FunctionParams(ImmutableList.of(castExpr, argumentsList.get(3))));
                }
            }
        } else if (functionName.replace("_", "").equalsIgnoreCase("jsonmatch")) {
            List<Expr> argumentsList = Arrays.asList(args);
            if (args.length < 2) {
                throw new SemanticException("The jsonmatch function must include at least 2 parameters.");
            }
            StringLiteral paramLiteral = (StringLiteral) args[1];
            JsonMatchExpressionParser.JsonMatchCondition jsonMatchCondition =
                    JsonMatchExpressionParser.parseJsonMatchExpression(paramLiteral.getValue());
            String operator = jsonMatchCondition.getOperator().toLowerCase();
            String jsonPath = jsonMatchCondition.getJsonPath();
            Object value = jsonMatchCondition.getValue();
            List<String> valueList = jsonMatchCondition.getValueList();
            List<Expr> valueExprList = new ArrayList<>();
            if (jsonMatchCondition.isList()) {
                for (String valueStr : valueList) {
                    valueExprList.add(new StringLiteral(valueStr));
                }
            }

            if (operator.equals("is null")) {
                FunctionCallExpr jsonQuery = new FunctionCallExpr(FunctionSet.JSON_QUERY,
                        new FunctionParams(ImmutableList.of(argumentsList.get(0), new StringLiteral(jsonPath))));
                return new IsNullPredicate(jsonQuery, false);
            } else if (operator.equals("is not null")) {
                FunctionCallExpr jsonQuery = new FunctionCallExpr(FunctionSet.JSON_QUERY,
                        new FunctionParams(ImmutableList.of(argumentsList.get(0), new StringLiteral(jsonPath))));
                return new IsNullPredicate(jsonQuery, true);
            } else if (operator.equals("in")) {
                FunctionCallExpr jsonQuery = new FunctionCallExpr(FunctionSet.JSON_QUERY,
                        new FunctionParams(ImmutableList.of(argumentsList.get(0), new StringLiteral(jsonPath))));
                CastExpr castExpr = new CastExpr(Type.STRING, jsonQuery);

                return new InPredicate(castExpr, valueExprList, false);
            } else if (operator.equals("not in")) {
                FunctionCallExpr jsonQuery = new FunctionCallExpr(FunctionSet.JSON_QUERY,
                        new FunctionParams(ImmutableList.of(argumentsList.get(0), new StringLiteral(jsonPath))));

                CastExpr castExpr = new CastExpr(Type.STRING, jsonQuery);
                return new InPredicate(castExpr, valueExprList, true);
            } else if (operator.equals("=")) {
                FunctionCallExpr jsonQuery = new FunctionCallExpr(FunctionSet.JSON_QUERY,
                        new FunctionParams(ImmutableList.of(argumentsList.get(0), new StringLiteral(jsonPath))));

                CastExpr castExpr = new CastExpr(PinotParserUtils.getScalarTypeFromObject(value), jsonQuery);
                return new BinaryPredicate(BinaryType.EQ, castExpr, PinotParserUtils.getLiteralTypeFromObject(value));
            } else if (operator.equals("!=")) {
                FunctionCallExpr jsonQuery = new FunctionCallExpr(FunctionSet.JSON_QUERY,
                        new FunctionParams(ImmutableList.of(argumentsList.get(0), new StringLiteral(jsonPath))));

                CastExpr castExpr = new CastExpr(PinotParserUtils.getScalarTypeFromObject(value), jsonQuery);
                return new BinaryPredicate(BinaryType.NE, castExpr, PinotParserUtils.getLiteralTypeFromObject(value));
            }
        } else if (functionName.equalsIgnoreCase("substr")) {
            List<Expr> argumentsList = Arrays.asList(args);
            if (args.length < 3) {
                throw new SemanticException("The substr function must include at least 3 parameters.");
            }
            IntLiteral startIndex = (IntLiteral) args[1];
            IntLiteral endIndex = (IntLiteral) args[2];
            long starRocksStart = startIndex.getValue() + 1;

            if (endIndex.getValue() == -1) {
                return new FunctionCallExpr(FunctionSet.SUBSTRING,
                        new FunctionParams(ImmutableList.of(argumentsList.get(0), new IntLiteral(starRocksStart))));
            } else {
                long length = endIndex.getValue() - startIndex.getValue();
                return new FunctionCallExpr(FunctionSet.SUBSTRING,
                        new FunctionParams(ImmutableList.of(argumentsList.get(0),
                                new IntLiteral(starRocksStart), new IntLiteral(length))));
            }
        } else if (functionName.equalsIgnoreCase("contains")) {
            List<Expr> argumentsList = Arrays.asList(args);
            if (args.length < 2) {
                throw new SemanticException("The contains function must include 2 parameters.");
            }

            StringLiteral paramLiteral = (StringLiteral) args[1];
            FunctionCallExpr inStr = new FunctionCallExpr(FunctionSet.INSTR,
                    new FunctionParams(ImmutableList.of(argumentsList.get(0), paramLiteral)));
            return new BinaryPredicate(BinaryType.GT, inStr, new IntLiteral(0));
        } else if (functionName.equalsIgnoreCase("ago")) {
            DurationInfo durationInfo = DurationParser.parse(((StringLiteral) args[0]).getValue());
            FunctionCallExpr now = new FunctionCallExpr(FunctionSet.NOW, new FunctionParams(ImmutableList.of()));

            Expr finalFunc = null;

            if (durationInfo.days != 0) {
                IntervalLiteral intervalLiteral = new IntervalLiteral(new IntLiteral(durationInfo.days),
                        new UnitIdentifier("DAY"));
                finalFunc = new TimestampArithmeticExpr(ArithmeticExpr.Operator.SUBTRACT, now, intervalLiteral.getValue(),
                        intervalLiteral.getUnitIdentifier().getDescription(), false);
            }
            if (durationInfo.hours != 0) {
                IntervalLiteral intervalLiteral = new IntervalLiteral(new IntLiteral(durationInfo.hours),
                        new UnitIdentifier("HOUR"));
                finalFunc = new TimestampArithmeticExpr(ArithmeticExpr.Operator.SUBTRACT, finalFunc != null ? finalFunc : now,
                        intervalLiteral.getValue(), intervalLiteral.getUnitIdentifier().getDescription(), false);
            }
            if (durationInfo.minutes != 0) {
                IntervalLiteral intervalLiteral = new IntervalLiteral(new IntLiteral(durationInfo.minutes),
                        new UnitIdentifier("MINUTE"));
                finalFunc = new TimestampArithmeticExpr(ArithmeticExpr.Operator.SUBTRACT, finalFunc != null ? finalFunc : now,
                        intervalLiteral.getValue(), intervalLiteral.getUnitIdentifier().getDescription(), false);
            }
            if (durationInfo.seconds != 0) {
                IntervalLiteral intervalLiteral = new IntervalLiteral(new DecimalLiteral(
                        BigDecimal.valueOf(durationInfo.seconds)), new UnitIdentifier("SECOND"));
                finalFunc = new TimestampArithmeticExpr(ArithmeticExpr.Operator.SUBTRACT, finalFunc != null ? finalFunc : now,
                        intervalLiteral.getValue(), intervalLiteral.getUnitIdentifier().getDescription(), false);
            }

            FunctionCallExpr unixTimestamp = new FunctionCallExpr(FunctionSet.UNIX_TIMESTAMP,
                    new FunctionParams(ImmutableList.of(finalFunc)));
            return new ArithmeticExpr(ArithmeticExpr.Operator.MULTIPLY, unixTimestamp,
                    new IntLiteral(1000));

        }


        return null;
    }

    public static Expr buildPredicate(List<String> parsedList, List<Expr> argumentsList) {
        Expr current = null;

        for (int i = 0; i < parsedList.size(); i++) {
            String element = parsedList.get(i);

            if (!element.equals("AND") && !element.equals("OR")) {
                FunctionCallExpr regexFunc = new FunctionCallExpr(FunctionSet.REGEXP,
                        new FunctionParams(ImmutableList.of(argumentsList.get(0),
                            new StringLiteral(element))));
                if (current == null) {
                    current = regexFunc;
                } else {
                    // The default logic is OR
                    current = new CompoundPredicate(CompoundPredicate.Operator.OR, current, regexFunc);
                }
            } else if (element.equals("AND") || element.equals("OR")) {
                String operator = element;
                String nextElement = parsedList.get(++i);
                FunctionCallExpr rightFunction =  new FunctionCallExpr(FunctionSet.REGEXP,
                        new FunctionParams(ImmutableList.of(argumentsList.get(0), new StringLiteral(nextElement))));
                current = new CompoundPredicate(operator.equals("AND") ? CompoundPredicate.Operator.AND :
                        CompoundPredicate.Operator.OR, current, rightFunction);
            }
        }

        return current;
    }

    public static List<String> parseInputString(String input) {
        List<String> result = new ArrayList<>();

        // to separate the phrase and term
        Pattern pattern = Pattern.compile("\"[^\"]+\"|\\S+");
        Matcher matcher = pattern.matcher(input);

        while (matcher.find()) {
            result.add(matcher.group());
        }

        List<String> finalResult = new ArrayList<>();
        if (input.matches(".*\\b(AND|OR)\\b.*")) {
            for (int i = 0; i < result.size(); i++) {
                String token = result.get(i);
                if (token.equalsIgnoreCase("AND") || token.equalsIgnoreCase("OR")) {
                    finalResult.add(token.toUpperCase());
                } else {
                    addTokenToList(finalResult, token.trim());
                }
            }
        } else {
            for (int i = 0; i < result.size(); i++) {
                if (i > 0) {
                    finalResult.add("OR");
                }
                addTokenToList(finalResult, result.get(i).trim());
            }
        }

        return finalResult;
    }

    private static void addTokenToList(List<String> list, String token) {
        if (token.startsWith("\"") && token.endsWith("\"")) {
            list.add("\\b" + token.substring(1, token.length() - 1) + "\\b");
        } else if (token.endsWith("*")) {
            list.add("^" + token.replace("*", ".*"));
        } else if (token.startsWith("/") && token.endsWith("/")) {
            list.add(token.substring(1, token.length() - 1));
        } else {
            list.add("\\b" + token + "\\b");
        }
    }

    private static List<Expr> getArgumentsForTimeSlice(Expr time, Expr value, String ident, String boundary) {
        List<Expr> exprs = Lists.newLinkedList();
        exprs.add(time);
        addArgumentUseTypeInt(value, exprs);
        exprs.add(new StringLiteral(ident));
        exprs.add(new StringLiteral(boundary));

        return exprs;
    }

    private static void addArgumentUseTypeInt(Expr value, List<Expr> exprs) {
        // IntLiteral may use TINYINT/SMALLINT/INT/BIGINT type
        // but time_slice only support INT type when executed in BE
        try {
            if (value instanceof IntLiteral) {
                exprs.add(new IntLiteral(((IntLiteral) value).getValue(), Type.INT));
            } else {
                exprs.add(value);
            }
        } catch (Exception e) {
            throw new IllegalArgumentException(String.format("Cast argument %s to int type failed.", value.toSql()));
        }
    }
}
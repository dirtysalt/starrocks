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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class DurationParser {

    public static DurationInfo parse(String input) {
        DurationInfo info = new DurationInfo();

        if (input == null || input.isEmpty()) {
            throw new IllegalArgumentException("Input duration string is null or empty.");
        }

        boolean overallNegative = input.startsWith("-");
        String trimmed = overallNegative ? input.substring(1) : input;

        if (!trimmed.startsWith("P")) {
            throw new IllegalArgumentException("Invalid duration format: " + input);
        }

        trimmed = trimmed.substring(1); // Remove 'P'
        trimmed = trimmed.replace("T", "");

        // Match value + unit, with possible +/– before each number
        Pattern pattern = Pattern.compile("([+-]?\\d+(?:\\.\\d+)?)([DHMS])");
        Matcher matcher = pattern.matcher(trimmed);

        while (matcher.find()) {
            double value = Double.parseDouble(matcher.group(1));
            String unit = matcher.group(2);

            if (overallNegative) {
                value = -value;
            }

            switch (unit) {
                case "D":
                    info.days = (int) value;
                    break;
                case "H":
                    info.hours = (int) value;
                    break;
                case "M":
                    info.minutes = (int) value;
                    break;
                case "S":
                    info.seconds = value;
                    break;
            }
        }

        return info;
    }

    public static void main(String[] args) {
        String[] inputs = {
                "PT20.345S",
                "PT15M",
                "PT10H",
                "P2D",
                "P2DT3H4M",
                "P-6H3M",
                "-P6H3M",
                "-P-6H+3M",
                "-P+2D-T3H-4M20S"
        };

        for (String input : inputs) {
            try {
                DurationInfo info = parse(input);
                System.out.println(input + " => " + info);
            } catch (Exception e) {
                System.out.println("Failed to parse " + input + ": " + e.getMessage());
            }
        }
    }
}

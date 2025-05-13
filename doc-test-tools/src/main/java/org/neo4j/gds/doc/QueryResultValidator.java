/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.gds.doc;

import org.jetbrains.annotations.NotNull;
import org.neo4j.graphdb.Result;

import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.OptionalInt;
import java.util.TreeMap;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

public record QueryResultValidator(String query, Result result, List<String> resultColumns, List<List<String>> expectedResult, OptionalInt maxFloatPrecision) {

    public void validate() {
        assertThat(resultColumns).containsExactlyElementsOf(result.columns());

        var actualResults = new ArrayList<List<String>>();

        var floatFormat = getFloatFormat();
        while (result.hasNext()) {
            var actualResultRow = result.next();
            var actualResultValues = resultColumns
                .stream()
                .map(column -> valueToString(actualResultRow.get(column), floatFormat))
                .collect(Collectors.toList());
            actualResults.add(actualResultValues);
        }
        var expectedResults = reducePrecisionOfDoubles(expectedResult, floatFormat);
        assertThat(actualResults)
            .as(query)
            .containsExactlyElementsOf(expectedResults);
    }

    private String valueToString(Object value, NumberFormat floatFormat) {
        if (value == null) {
            return "null";
        } else if (value instanceof String) {
            return "\"" + value + "\"";
        } else if (value instanceof Double || value instanceof Float) {
            return floatFormat.format(value);
        } else if (value instanceof double[]) {
            // Some values are read as arrays of primitives rather than lists
            var doubleArrays = Arrays.stream((double[]) value)
                .boxed()
                .collect(Collectors.toList());
            return valueToString(
                doubleArrays,
                floatFormat
            );
        } else if (value instanceof List<?>) {
            return ((List<?>) value).stream()
                .map(v -> valueToString(v, floatFormat))
                .collect(Collectors.toList())
                .toString();
        } else if (value instanceof Map<?, ?>) {
            var mappedMap = ((Map<?, ?>) value).entrySet()
                .stream()
                .collect(
                    Collectors.toMap(
                        entry -> entry.getKey().toString(),
                        entry -> valueToString(entry.getValue(), floatFormat)
                    )
                );
            return new TreeMap<>(mappedMap).toString();
        } else {
            return value.toString();
        }
    }

    private List<List<String>> reducePrecisionOfDoubles(Collection<List<String>> resultsFromDoc, NumberFormat floatFormat) {
        return resultsFromDoc
            .stream()
            .map(list -> list.stream().map(string -> {
                    try {
                        if (string.startsWith("[")) {
                            return formatListOfNumbers(string, floatFormat);
                        }
                        if (string.contains(".")) {
                            return floatFormat.format(Double.parseDouble(string));
                        } else {
                            return string;
                        }
                    } catch (NumberFormatException e) {
                        return string;
                    }
                }).collect(Collectors.toList())
            )
            .collect(Collectors.toList());
    }

    @NotNull
    private String formatListOfNumbers(String string, NumberFormat floatFormat) {
        var withoutBrackets = string.substring(1, string.length() - 1);
        var commaSeparator = ", ";
        var parts = withoutBrackets.split(commaSeparator);
        var builder = new StringBuilder("[");
        var separator = "";
        for (var part : parts) {
            builder.append(separator);
            String formattedPart = part.contains(".")
                ? floatFormat.format(Double.parseDouble(part))
                : part;

            builder.append(formattedPart);
            separator = commaSeparator;
        }
        builder.append("]");

        return builder.toString();
    }

    private NumberFormat getFloatFormat() {
        var decimalFormat = DecimalFormat.getInstance(Locale.ENGLISH);
        decimalFormat.setMaximumFractionDigits(maxFloatPrecision.orElse(DocumentationTestToolsConstants.FLOAT_MAXIMUM_PRECISION));
        decimalFormat.setMinimumFractionDigits(DocumentationTestToolsConstants.FLOAT_MINIMUM_PRECISION);
        decimalFormat.setGroupingUsed(false);

        return decimalFormat;
    }
}

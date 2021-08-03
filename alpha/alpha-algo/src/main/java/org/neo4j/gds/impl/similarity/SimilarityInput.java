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
package org.neo4j.gds.impl.similarity;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;
import static org.neo4j.graphalgo.core.concurrency.ParallelUtil.parallelStream;

public interface SimilarityInput {
    long getId();

    static int[] indexes(long[] inputIds, List<Long> idsToFind) {
        int[] indexes = new int[idsToFind.size()];
        List<Long> missingIds = new ArrayList<>();

        int indexesFound = 0;
        for (long idToFind : idsToFind) {
            int index = Arrays.binarySearch(inputIds, idToFind);
            if (index < 0) {
                missingIds.add(idToFind);
            } else {
                indexes[indexesFound] = index;
                indexesFound++;
            }
        }

        if (!missingIds.isEmpty()) {
            throw new IllegalArgumentException(formatWithLocale("Node ids %s do not exist in node ids list", missingIds));
        }

        return indexes;
    }

    static long[] extractInputIds(SimilarityInput[] inputs, int concurrency) {
        return parallelStream(Arrays.stream(inputs),  concurrency, stream -> stream.mapToLong(SimilarityInput::getId).toArray());
    }

    static int[] indexesFor(long[] inputIds, List<Long> sourceIds, String key) {
        try {
            return indexes(inputIds, sourceIds);
        } catch(IllegalArgumentException exception) {
            String message = formatWithLocale("%s: %s", formatWithLocale("Missing node ids in '%s' list ", key), exception.getMessage());
            throw new RuntimeException(new IllegalArgumentException(message));
        }
    }


    static List<Number> extractValues(Object rawValues) {
        if (rawValues == null) {
            return List.of();
        }

        List<Number> valueList;
        if (rawValues instanceof long[]) {
            long[] values = (long[]) rawValues;
            valueList = new ArrayList<>(values.length);
            for (long value : values) {
                valueList.add(value);
            }
        } else if (rawValues instanceof double[]) {
            double[] values = (double[]) rawValues;
            valueList = new ArrayList<>(values.length);
            for (double value : values) {
                valueList.add(value);
            }
        } else if (rawValues instanceof float[]) {
            float[] values = (float[]) rawValues;
            valueList = new ArrayList<>(values.length);
            for (float value : values) {
                valueList.add(value);
            }
        } else if (rawValues instanceof List) {
            var values = (List<?>) rawValues;
            int index = 0;
            for (Object value : values) {
                if (value != null && !(value instanceof Number)) {
                    throw new IllegalArgumentException(formatWithLocale(
                        "The weight input contains a non-numeric value at index %d: %s",
                        index,
                        value
                    ));
                }
                ++index;
            }
            // We did check all elements of the list before, cast is safe now
            // noinspection unchecked
            valueList = (List<Number>) values;
        } else {
            throw new IllegalArgumentException(formatWithLocale(
                "The weight input is not a list of numeric values, found instead: %s",
                rawValues.getClass().getName()
            ));
        }
        return valueList;
    }
}

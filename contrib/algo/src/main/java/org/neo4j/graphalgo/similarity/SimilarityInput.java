/*
 * Copyright (c) 2017-2019 "Neo4j,"
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
package org.neo4j.graphalgo.similarity;

import org.neo4j.graphalgo.core.ProcedureConfiguration;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

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
            throw new IllegalArgumentException(String.format("Node ids %s do not exist in node ids list", missingIds));
        }

        return indexes;
    }

    static long[] extractInputIds(SimilarityInput[] inputs) {
        return Arrays.stream(inputs).parallel().mapToLong(SimilarityInput::getId).toArray();
    }

    static int[] indexesFor(long[] inputIds, ProcedureConfiguration configuration, String key) {
        List<Long> sourceIds = configuration.get(key, Collections.emptyList());
        try {
            return indexes(inputIds, sourceIds);
        } catch(IllegalArgumentException exception) {
            String message = String.format("%s: %s", String.format("Missing node ids in '%s' list ", key), exception.getMessage());
            throw new RuntimeException(new IllegalArgumentException(message));
        }
    }


    static List<Number> extractValues(Object rawValues) {
        if (rawValues == null) {
            return Collections.emptyList();
        }

        List<Number> valueList = new ArrayList<>();
        if (rawValues instanceof long[]) {
            long[] values = (long[]) rawValues;
            for (long value : values) {
                valueList.add(value);
            }
        } else if (rawValues instanceof double[]) {
            double[] values = (double[]) rawValues;
            for (double value : values) {
                valueList.add(value);
            }
        } else {
            valueList = (List<Number>) rawValues;
        }
        return valueList;
    }
}

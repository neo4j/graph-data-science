/*
 * Copyright (c) 2017-2020 "Neo4j,"
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
package org.neo4j.graphalgo.core.utils;

import com.carrotsearch.hppc.BitSet;
import com.carrotsearch.hppc.LongObjectMap;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public class BitSetBuilder {

    private final Map<String, BitSet> bitSets;

    public BitSetBuilder(Map<String, BitSet> bitSets) {
        this.bitSets = bitSets;
    }

    public final boolean bulkAdd(long startIndex, int batchLength, LongObjectMap<List<String>> labelMapping, long[][] labelIds) {
        int cappedBatchLength = Math.min(labelIds.length, batchLength);
        for (int i = 0; i < cappedBatchLength; i++) {
            long[] labeldsForNode = labelIds[i];
            for (long labelId : labeldsForNode) {
                List<String> elementIdentifiers = labelMapping.getOrDefault(labelId, Collections.emptyList());
                for (String elementIdentifier : elementIdentifiers) {
                    bitSets.get(elementIdentifier).set(startIndex + i);
                }
            }
        }

        return true;
    }
}

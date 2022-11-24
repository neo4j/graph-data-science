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
package org.neo4j.gds.core.loading;

import com.carrotsearch.hppc.sorting.IndirectSort;
import org.neo4j.gds.core.Aggregation;
import org.neo4j.gds.core.utils.AscendingLongComparator;

public final class AdjacencyPreAggregation {

    // Used to replace target ids for aggregated relationships.
    // If pre-aggregation is applied, the target array is not
    // resized, but the target ids are replaced by this value.
    //
    // The value is used during compression to filter targets.
    public static final long IGNORE_VALUE = Long.MIN_VALUE;

    static int preAggregate(
        long[] targetIds,
        long[][] propertiesList,
        int startOffset,
        int endOffset,
        Aggregation[] aggregations
    ) {


        // Step 1: Sort the targetIds (indirectly)
        var order = IndirectSort.mergesort(
            startOffset,
            endOffset - startOffset,
            new AscendingLongComparator(targetIds)
        );
        
        // Step 2: Aggregate the properties into the first property list of each distinct value
        //         Every subsequent instance of any value is set to LONG.MIN_VALUE
        int targetIndex = order[0];
        long lastSeenTargetId = targetIds[targetIndex];
        var distinctValues = 1;

        for (int orderIndex = 1; orderIndex < order.length; orderIndex++) {
            int currentIndex = order[orderIndex];

            if (targetIds[currentIndex] != lastSeenTargetId) {
                targetIndex = currentIndex;
                lastSeenTargetId = targetIds[currentIndex];
                distinctValues++;
            } else {
                for (int propertyId = 0; propertyId < propertiesList.length; propertyId++) {
                    long[] properties = propertiesList[propertyId];
                    double runningTotal = Double.longBitsToDouble(properties[targetIndex]);
                    double value = Double.longBitsToDouble(propertiesList[propertyId][currentIndex]);

                    double updatedProperty = aggregations[propertyId].merge(
                        runningTotal,
                        value
                    );
                    propertiesList[propertyId][targetIndex] = Double.doubleToLongBits(updatedProperty);
                }

                targetIds[currentIndex] = IGNORE_VALUE;
            }
        }

        return distinctValues;
    }


    private AdjacencyPreAggregation() {
    }
}

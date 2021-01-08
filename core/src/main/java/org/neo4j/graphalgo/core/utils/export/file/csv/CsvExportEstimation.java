/*
 * Copyright (c) 2017-2021 "Neo4j,"
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
package org.neo4j.graphalgo.core.utils.export.file.csv;

import org.neo4j.graphalgo.api.GraphStore;
import org.neo4j.graphalgo.core.utils.export.file.GraphStoreToFileExporter;
import org.neo4j.graphalgo.core.utils.mem.MemoryEstimation;
import org.neo4j.graphalgo.core.utils.mem.MemoryEstimations;

import java.util.Collection;
import java.util.stream.Stream;

public final class CsvExportEstimation {

    // assuming utf-8
    private static final int BYTES_PER_WRITTEN_CHARACTER = 1;

    //This is a magic value, assuming that values are usually numeric and use in average 5 digits.
    private static final long DIGITS_PER_PROPERTY_VALUE = 5;

    public static MemoryEstimation estimate(GraphStore graphStore) {
        return MemoryEstimations
            .builder(GraphStoreToFileExporter.class)
        .fixed("Node data", estimateNodes(graphStore))
        .fixed("Relationship data", estimateRelationships(graphStore))
        .build();
    }

    private static long estimateNodes(GraphStore graphStore) {
        // node id estimate
        long nodeIdsEstimate = getIdEstimate(graphStore);

        Stream<String> propertyKeys = graphStore.nodePropertyKeys().values().stream().flatMap(Collection::stream);
        Long nodePropertiesEstimate = propertyKeys.map(propertyKey -> {
            var nodeProperties = graphStore.nodePropertyValues(propertyKey);
            return nodeProperties.size() * DIGITS_PER_PROPERTY_VALUE * BYTES_PER_WRITTEN_CHARACTER;
        }).reduce(0L, Long::sum);

        long lineBreakEstimates = graphStore.nodeCount();

        return nodeIdsEstimate + nodePropertiesEstimate + lineBreakEstimates;
    }

    private static long estimateRelationships(GraphStore graphStore) {
        long avgBytesPerNodeId = getIdEstimate(graphStore) / graphStore.nodeCount();

        return graphStore.relationshipTypes().stream().map(type -> {
            long relationshipCount = graphStore.getGraph(type).relationshipCount();

            long relationshipIdEstimate = relationshipCount * 2 * avgBytesPerNodeId;

            long relationshipPropertiesEstimate = relationshipCount * DIGITS_PER_PROPERTY_VALUE * BYTES_PER_WRITTEN_CHARACTER;

            return relationshipIdEstimate + relationshipPropertiesEstimate;
        }).reduce(0L, Long::sum);
    }

    private static long getIdEstimate(GraphStore graphStore) {
        long maxNumberOfDigits = (long) Math.floor(Math.log10(graphStore.nodeCount()));
        long nodeIdEstimate = 0;
        long consideredNumbers = 0;

        for (long digits = 1; digits < maxNumberOfDigits; digits++) {
            long numbersWithDigitX =(10 ^ digits) - consideredNumbers;
            consideredNumbers += numbersWithDigitX;

            nodeIdEstimate += numbersWithDigitX * digits * BYTES_PER_WRITTEN_CHARACTER;
        }

        nodeIdEstimate += (graphStore.nodeCount() - consideredNumbers) * maxNumberOfDigits * BYTES_PER_WRITTEN_CHARACTER;
        return nodeIdEstimate;
    }

    private CsvExportEstimation() {}
}

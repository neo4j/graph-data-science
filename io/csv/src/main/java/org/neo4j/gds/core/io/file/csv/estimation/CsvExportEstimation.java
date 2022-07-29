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
package org.neo4j.gds.core.io.file.csv.estimation;

import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.core.io.file.GraphStoreToFileExporter;
import org.neo4j.gds.core.utils.mem.MemoryEstimation;
import org.neo4j.gds.core.utils.mem.MemoryEstimations;

public final class CsvExportEstimation {

    // assuming utf-8
    private static final int BYTES_PER_WRITTEN_CHARACTER = 1;

    public static MemoryEstimation estimate(GraphStore graphStore, double samplingFactor) {
        return MemoryEstimations
            .builder(GraphStoreToFileExporter.class)
            .fixed("Node data", estimateNodes(graphStore, samplingFactor))
            .fixed("Relationship data", estimateRelationships(graphStore, samplingFactor))
            .build();
    }

    private static long estimateNodes(GraphStore graphStore, double samplingFactor) {
        long nodeIdsEstimate = getIdEstimate(graphStore);
        long nodePropertiesEstimate = sampleNodeProperties(graphStore, samplingFactor);

        return nodeIdsEstimate + nodePropertiesEstimate;
    }

    private static long estimateRelationships(GraphStore graphStore, double samplingFactor) {
        long avgBytesPerNodeId = getIdEstimate(graphStore) / graphStore.nodeCount() + 1;

        long sourceTargetIdEstimate = avgBytesPerNodeId * 2 * graphStore.relationshipCount();
        var relationshipPropertiesEstimate = sampleRelationshipProperties(graphStore, samplingFactor);

        return sourceTargetIdEstimate + relationshipPropertiesEstimate;
    }

    private static long getIdEstimate(GraphStore graphStore) {
        long maxNumberOfDigits = (long) Math.floor(Math.log10(graphStore.nodeCount()));
        long nodeIdEstimate = 0;
        long consideredNumbers = 0;

        // count all nodes from 1 - log(nodeCount)
        // 1 digit  -> 10
        // 2 digits -> 100 - 10
        // 3 digits -> 1000 - 100 - 10
        // ...
        for (long digits = 1; digits < maxNumberOfDigits; digits++) {
            long numbersWithDigitX = (10 ^ digits) - consideredNumbers;
            nodeIdEstimate += numbersWithDigitX * digits * BYTES_PER_WRITTEN_CHARACTER;
            consideredNumbers += numbersWithDigitX;
        }

        // count the nodes with max digit count
        nodeIdEstimate += (graphStore.nodeCount() - consideredNumbers) * maxNumberOfDigits * BYTES_PER_WRITTEN_CHARACTER;

        return nodeIdEstimate;
    }

    private static long sampleNodeProperties(GraphStore graphStore, double samplingFactor) {
        return graphStore.nodeCount() * NodePropertySampler.sample(graphStore, samplingFactor);
    }

    private static long sampleRelationshipProperties(GraphStore graphStore, double samplingFactor) {
        return graphStore.relationshipCount() * RelationshipPropertySampler.sample(graphStore, samplingFactor);
    }

    private CsvExportEstimation() {}
}

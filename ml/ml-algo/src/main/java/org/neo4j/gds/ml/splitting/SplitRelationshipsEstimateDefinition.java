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
package org.neo4j.gds.ml.splitting;

import org.neo4j.gds.mem.MemoryEstimateDefinition;
import org.neo4j.gds.mem.MemoryEstimation;
import org.neo4j.gds.mem.MemoryEstimations;
import org.neo4j.gds.mem.MemoryRange;

public class SplitRelationshipsEstimateDefinition implements MemoryEstimateDefinition {
    private final SplitRelationshipsEstimateParameters estimateParameters;

    public SplitRelationshipsEstimateDefinition(SplitRelationshipsEstimateParameters estimateParameters) {
        this.estimateParameters = estimateParameters;
    }

    @Override
    public MemoryEstimation memoryEstimation() {
        // we cannot assume any compression of the relationships
        var pessimisticSizePerRel = estimateParameters.hasRelationshipWeightProperty
            ? Double.BYTES + 2 * Long.BYTES
            : 2 * Long.BYTES;

        return MemoryEstimations.builder("Relationship splitter")
            .perGraphDimension("Selected relationships", (graphDimensions, threads) -> {
                var positiveRelCount = graphDimensions.estimatedRelCount(estimateParameters.relationshipTypes) * estimateParameters.holdoutFraction;
                var negativeRelCount = positiveRelCount * estimateParameters.negativeSamplingRatio;
                long selectedRelCount = (long) (positiveRelCount + negativeRelCount);

                // Whether the graph is undirected or directed
                return MemoryRange.of(selectedRelCount / 2, selectedRelCount).times(pessimisticSizePerRel);
            })
            .perGraphDimension("Remaining relationships", (graphDimensions, threads) -> {
                long remainingRelCount = (long) (graphDimensions.estimatedRelCount(estimateParameters.relationshipTypes) * (1 - estimateParameters.holdoutFraction));
                // remaining relationships are always undirected
                return MemoryRange.of(remainingRelCount * pessimisticSizePerRel);
            })
            .build();
    }
}

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
package org.neo4j.gds.procedures.algorithms.pathfinding;

import org.neo4j.gds.result.AbstractResultBuilder;

import java.util.Map;

public final class SpanningTreeMutateResult extends SpanningTreeStatsResult {
    public final long mutateMillis;
    public final long relationshipsWritten;

    public SpanningTreeMutateResult(
        long preProcessingMillis,
        long computeMillis,
        long mutateMillis,
        long effectiveNodeCount,
        long relationshipsWritten,
        double totalCost,
        Map<String, Object> configuration
    ) {
        super(preProcessingMillis, computeMillis, effectiveNodeCount, totalCost, configuration);
        this.mutateMillis = mutateMillis;
        this.relationshipsWritten = relationshipsWritten;
    }

    public static class Builder extends AbstractResultBuilder<SpanningTreeMutateResult> {
        long effectiveNodeCount;
        double totalWeight;

        public void withEffectiveNodeCount(long effectiveNodeCount) {
            this.effectiveNodeCount = effectiveNodeCount;
        }

        public void withTotalWeight(double totalWeight) {
            this.totalWeight = totalWeight;
        }

        @Override
        public SpanningTreeMutateResult build() {
            return new SpanningTreeMutateResult(
                preProcessingMillis,
                computeMillis,
                mutateMillis,
                effectiveNodeCount,
                relationshipsWritten,
                totalWeight,
                config.toMap()
            );
        }
    }
}

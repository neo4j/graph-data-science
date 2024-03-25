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

public final class SpanningTreeWriteResult extends SpanningTreeStatsResult {
    public final long writeMillis;
    public final long relationshipsWritten;

    private SpanningTreeWriteResult(
        long preProcessingMillis,
        long computeMillis,
        long writeMillis,
        long effectiveNodeCount,
        long relationshipsWritten,
        double totalCost,
        Map<String, Object> configuration
    ) {
        super(preProcessingMillis, computeMillis, effectiveNodeCount, totalCost, configuration);
        this.writeMillis = writeMillis;
        this.relationshipsWritten = relationshipsWritten;
    }

    public static class Builder extends AbstractResultBuilder<SpanningTreeWriteResult> {

        long effectiveNodeCount;
        double totalWeight;

        public Builder withEffectiveNodeCount(long effectiveNodeCount) {
            this.effectiveNodeCount = effectiveNodeCount;
            return this;
        }

        public void withTotalWeight(double totalWeight) {
            this.totalWeight = totalWeight;
        }

        @Override
        public SpanningTreeWriteResult build() {
            return new SpanningTreeWriteResult(
                preProcessingMillis,
                computeMillis,
                writeMillis,
                effectiveNodeCount,
                relationshipsWritten,
                totalWeight,
                config.toMap()
            );
        }
    }
}

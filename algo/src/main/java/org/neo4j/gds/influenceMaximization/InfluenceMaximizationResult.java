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
package org.neo4j.gds.influenceMaximization;

import org.neo4j.gds.result.AbstractResultBuilder;

public class InfluenceMaximizationResult {
    public final long nodeId;
    public final double spread;

    InfluenceMaximizationResult(long nodeId, double spread) {
        this.nodeId = nodeId;
        this.spread = spread;
    }

    @Override
    public String toString() {
        return "InfluenceMaximizationResult{nodeId=" + nodeId + ", spread=" + spread + "}";
    }

    @SuppressWarnings("unused")
    public static final class Stats {
        public final long nodes;
        public final long computeMillis;

        public Stats(
            long nodes,
            long computeMillis
        ) {
            this.nodes = nodes;
            this.computeMillis = computeMillis;
        }

        public static final class Builder extends AbstractResultBuilder<InfluenceMaximizationResult.Stats> {

            @Override
            public InfluenceMaximizationResult.Stats build() {
                return new InfluenceMaximizationResult.Stats(
                    nodeCount,
                    computeMillis
                );
            }
        }
    }
}

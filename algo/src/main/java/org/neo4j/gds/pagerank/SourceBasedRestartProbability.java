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
package org.neo4j.gds.pagerank;


import java.util.Map;

public class SourceBasedRestartProbability implements InitialProbabilityProvider {

    private final double alpha;
    private final Map<Long, Double> sourceNodesWithPropertyMap;


    SourceBasedRestartProbability(double alpha, Map<Long, Double> sourceNodesWithPropertyMap) {
        this.alpha = alpha;
        if (sourceNodesWithPropertyMap.values().stream().anyMatch(x -> x < 0)) {
            throw new IllegalArgumentException("Negative values are not supported for the source node bias.");
        }
        this.sourceNodesWithPropertyMap = sourceNodesWithPropertyMap;
    }

    @Override
    public double provideInitialValue(long nodeId) {
        return alpha * sourceNodesWithPropertyMap.getOrDefault(nodeId,0d);
    }

}

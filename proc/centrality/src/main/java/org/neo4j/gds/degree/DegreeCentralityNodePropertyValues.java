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
package org.neo4j.gds.degree;

import org.neo4j.gds.api.properties.nodes.DoubleNodePropertyValues;
import org.neo4j.gds.executor.ComputationResult;

final class DegreeCentralityNodePropertyValues implements DoubleNodePropertyValues {

    private final long nodeCount;
    private final DegreeCentrality.DegreeFunction degreeFunction;

    static DegreeCentralityNodePropertyValues from(ComputationResult<DegreeCentrality, DegreeCentrality.DegreeFunction, ? extends DegreeCentralityConfig> computationResult) {
        return computationResult.result()
            .map(result -> new DegreeCentralityNodePropertyValues(computationResult.graph().nodeCount(), result))
            .orElseGet(() -> new DegreeCentralityNodePropertyValues(0, (nodeId) -> -1L));
    }

    private DegreeCentralityNodePropertyValues(
        long nodeCount,
        DegreeCentrality.DegreeFunction degreeFunction
    ) {
        this.nodeCount = nodeCount;
        this.degreeFunction = degreeFunction;
    }

    @Override
    public double doubleValue(long nodeId) {
        return degreeFunction.get(nodeId);
    }

    @Override
    public long nodeCount() {
        return nodeCount;
    }
}

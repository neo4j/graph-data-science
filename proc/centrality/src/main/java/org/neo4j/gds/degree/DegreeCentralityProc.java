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

import org.neo4j.gds.GraphAlgorithmFactory;
import org.neo4j.gds.api.properties.nodes.DoubleNodePropertyValues;
import org.neo4j.gds.api.properties.nodes.NodePropertyValues;
import org.neo4j.gds.executor.ComputationResult;
import org.neo4j.gds.result.AbstractCentralityResultBuilder;

public final class DegreeCentralityProc {

    static final String DEGREE_CENTRALITY_DESCRIPTION = "Degree centrality measures the number of incoming and outgoing relationships from a node.";

    private DegreeCentralityProc() {}

    static <CONFIG extends DegreeCentralityConfig> GraphAlgorithmFactory<DegreeCentrality, CONFIG> algorithmFactory() {
        return new DegreeCentralityFactory<>();
    }

    static <PROC_RESULT, CONFIG extends DegreeCentralityConfig> AbstractCentralityResultBuilder<PROC_RESULT> resultBuilder(
        AbstractCentralityResultBuilder<PROC_RESULT> procResultBuilder,
        ComputationResult<DegreeCentrality, DegreeCentrality.DegreeFunction, CONFIG> computeResult
    ) {
        var result = computeResult.result();
        procResultBuilder
            .withCentralityFunction(!computeResult.isGraphEmpty() ? result::get : null);

        return procResultBuilder;
    }

    static NodePropertyValues nodeProperties(ComputationResult<DegreeCentrality, DegreeCentrality.DegreeFunction, ? extends DegreeCentralityConfig> computationResult) {
        var size = computationResult.graph().nodeCount();
        var degrees = computationResult.result();

        return new DoubleNodePropertyValues() {

            @Override
            public long nodeCount() {
                return size;
            }

            @Override
            public double doubleValue(long nodeId) {
                return degrees.get(nodeId);
            }
        };
    }
}

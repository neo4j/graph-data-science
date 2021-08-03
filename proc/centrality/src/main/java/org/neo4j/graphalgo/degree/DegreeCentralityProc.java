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
package org.neo4j.graphalgo.degree;

import org.neo4j.gds.AlgorithmFactory;
import org.neo4j.gds.degree.DegreeCentrality;
import org.neo4j.gds.degree.DegreeCentralityConfig;
import org.neo4j.gds.degree.DegreeCentralityFactory;
import org.neo4j.gds.result.AbstractCentralityResultBuilder;
import org.neo4j.graphalgo.AlgoBaseProc;
import org.neo4j.graphalgo.api.NodeProperties;
import org.neo4j.graphalgo.api.nodeproperties.DoubleNodeProperties;

public final class DegreeCentralityProc {

    static final String DEGREE_CENTRALITY_DESCRIPTION = "Degree centrality measures the number of incoming and outgoing relationships from a node.";

    private DegreeCentralityProc() {}

    static <CONFIG extends DegreeCentralityConfig> AlgorithmFactory<DegreeCentrality, CONFIG> algorithmFactory() {
        return new DegreeCentralityFactory<>();
    }

    static <PROC_RESULT, CONFIG extends DegreeCentralityConfig> AbstractCentralityResultBuilder<PROC_RESULT> resultBuilder(
        AbstractCentralityResultBuilder<PROC_RESULT> procResultBuilder,
        AlgoBaseProc.ComputationResult<DegreeCentrality, DegreeCentrality.DegreeFunction, CONFIG> computeResult
    ) {
        var result = computeResult.result();
        procResultBuilder
            .withCentralityFunction(!computeResult.isGraphEmpty() ? result::get : null);

        return procResultBuilder;
    }

    static NodeProperties nodeProperties(AlgoBaseProc.ComputationResult<DegreeCentrality, DegreeCentrality.DegreeFunction, ? extends DegreeCentralityConfig> computationResult) {
        var size = computationResult.graph().nodeCount();
        var degrees = computationResult.result();

        return new DoubleNodeProperties() {
            @Override
            public long size() {
                return size;
            }

            @Override
            public double doubleValue(long nodeId) {
                return degrees.get(nodeId);
            }
        };
    }
}

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

import org.neo4j.gds.AlgorithmFactory;
import org.neo4j.gds.StreamProc;
import org.neo4j.gds.common.CentralityStreamResult;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.results.MemoryEstimateResult;
import org.neo4j.gds.api.NodeProperties;
import org.neo4j.gds.config.GraphCreateConfig;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static org.neo4j.gds.degree.DegreeCentralityProc.DEGREE_CENTRALITY_DESCRIPTION;
import static org.neo4j.procedure.Mode.READ;

public class DegreeCentralityStreamProc extends StreamProc<DegreeCentrality, DegreeCentrality.DegreeFunction, CentralityStreamResult, DegreeCentralityStreamConfig> {

    @Procedure(value = "gds.degree.stream", mode = READ)
    @Description(DEGREE_CENTRALITY_DESCRIPTION)
    public Stream<CentralityStreamResult> stream(
        @Name(value = "graphName") Object graphNameOrConfig,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        return stream(compute(graphNameOrConfig, configuration));
    }

    @Procedure(value = "gds.degree.stream.estimate", mode = READ)
    @Description(DEGREE_CENTRALITY_DESCRIPTION)
    public Stream<MemoryEstimateResult> estimate(
        @Name(value = "graphName") Object graphNameOrConfig,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        return computeEstimate(graphNameOrConfig, configuration);
    }

    @Override
    protected DegreeCentralityStreamConfig newConfig(
        String username,
        Optional<String> graphName,
        Optional<GraphCreateConfig> maybeImplicitCreate,
        CypherMapWrapper config
    ) {
        return DegreeCentralityStreamConfig.of(username, graphName, maybeImplicitCreate, config);
    }

    @Override
    protected AlgorithmFactory<DegreeCentrality, DegreeCentralityStreamConfig> algorithmFactory() {
        return DegreeCentralityProc.algorithmFactory();
    }

    @Override
    protected CentralityStreamResult streamResult(
        long originalNodeId, long internalNodeId, NodeProperties nodeProperties
    ) {
        return new CentralityStreamResult(originalNodeId, nodeProperties.doubleValue(internalNodeId));
    }

    @Override
    protected NodeProperties nodeProperties(ComputationResult<DegreeCentrality, DegreeCentrality.DegreeFunction, DegreeCentralityStreamConfig> computationResult) {
        return DegreeCentralityProc.nodeProperties(computationResult);
    }
}

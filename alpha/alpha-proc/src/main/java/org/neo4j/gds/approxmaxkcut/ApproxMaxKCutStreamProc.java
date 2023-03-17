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
package org.neo4j.gds.approxmaxkcut;

import org.neo4j.gds.GraphAlgorithmFactory;
import org.neo4j.gds.StreamProc;
import org.neo4j.gds.api.properties.nodes.NodePropertyValues;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.executor.ComputationResult;
import org.neo4j.gds.impl.approxmaxkcut.ApproxMaxKCut;
import org.neo4j.gds.impl.approxmaxkcut.MaxKCutResult;
import org.neo4j.gds.impl.approxmaxkcut.config.ApproxMaxKCutStreamConfig;
import org.neo4j.gds.results.MemoryEstimateResult;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Map;
import java.util.stream.Stream;

import static org.neo4j.gds.impl.approxmaxkcut.ApproxMaxKCut.APPROX_MAX_K_CUT_DESCRIPTION;
import static org.neo4j.procedure.Mode.READ;

public class ApproxMaxKCutStreamProc extends StreamProc<ApproxMaxKCut, MaxKCutResult, ApproxMaxKCutStreamProc.StreamResult, ApproxMaxKCutStreamConfig> {

    @Procedure(value = "gds.alpha.maxkcut.stream", mode = READ)
    @Description(APPROX_MAX_K_CUT_DESCRIPTION)
    public Stream<StreamResult> stream(
        @Name(value = "graphName") String graphName,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        return stream(compute(graphName, configuration));
    }

    @Procedure(value = "gds.alpha.maxkcut.stream.estimate", mode = READ)
    @Description(APPROX_MAX_K_CUT_DESCRIPTION)
    public Stream<MemoryEstimateResult> estimate(
        @Name(value = "graphNameOrConfiguration") Object graphNameOrConfiguration,
        @Name(value = "algoConfiguration") Map<String, Object> algoConfiguration
    ) {
        return computeEstimate(graphNameOrConfiguration, algoConfiguration);
    }

    @Override
    protected ApproxMaxKCutStreamConfig newConfig(String username, CypherMapWrapper config) {
        return ApproxMaxKCutStreamConfig.of(config);
    }

    @Override
    public GraphAlgorithmFactory<ApproxMaxKCut, ApproxMaxKCutStreamConfig> algorithmFactory() {
        return ApproxMaxKCutProc.algorithmFactory();
    }

    @Override
    protected StreamResult streamResult(
        long originalNodeId, long internalNodeId, NodePropertyValues nodePropertyValues
    ) {
        return new StreamResult(originalNodeId, nodePropertyValues.longValue(internalNodeId));
    }

    @Override
    protected NodePropertyValues nodeProperties(ComputationResult<ApproxMaxKCut, MaxKCutResult, ApproxMaxKCutStreamConfig> computationResult) {
        return ApproxMaxKCutProc.nodeProperties(computationResult);
    }

    @SuppressWarnings("unused")
    public static class StreamResult {

        public final long nodeId;
        public final long communityId;

        public StreamResult(long nodeId, long communityId) {
            this.nodeId = nodeId;
            this.communityId = communityId;
        }
    }
}

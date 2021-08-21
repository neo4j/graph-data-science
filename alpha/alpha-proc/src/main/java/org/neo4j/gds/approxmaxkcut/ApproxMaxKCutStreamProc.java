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

import org.neo4j.gds.AlgorithmFactory;
import org.neo4j.gds.StreamProc;
import org.neo4j.gds.api.NodeProperties;
import org.neo4j.gds.config.GraphCreateConfig;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.impl.approxmaxkcut.ApproxMaxKCut;
import org.neo4j.gds.impl.approxmaxkcut.ApproxMaxKCutStreamConfig;
import org.neo4j.gds.results.MemoryEstimateResult;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static org.neo4j.gds.approxmaxkcut.ApproxMaxKCutProc.APPROX_MAX_K_CUT_DESCRIPTION;
import static org.neo4j.procedure.Mode.READ;

public class ApproxMaxKCutStreamProc extends StreamProc<ApproxMaxKCut, ApproxMaxKCut.CutResult, ApproxMaxKCutStreamProc.StreamResult, ApproxMaxKCutStreamConfig> {

    @Procedure(value = "gds.alpha.maxkcut.stream", mode = READ)
    @Description(APPROX_MAX_K_CUT_DESCRIPTION)
    public Stream<StreamResult> stream(
        @Name(value = "graphName") Object graphNameOrConfig,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        return stream(compute(graphNameOrConfig, configuration));
    }

    @Procedure(value = "gds.alpha.maxkcut.stream.estimate", mode = READ)
    @Description(APPROX_MAX_K_CUT_DESCRIPTION)
    public Stream<MemoryEstimateResult> estimate(
        @Name(value = "graphName") Object graphNameOrConfig,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        return computeEstimate(graphNameOrConfig, configuration);
    }

    @Override
    protected ApproxMaxKCutStreamConfig newConfig(
        String username,
        Optional<String> graphName,
        Optional<GraphCreateConfig> maybeImplicitCreate,
        CypherMapWrapper config
    ) {
        return ApproxMaxKCutStreamConfig.of(username, graphName, maybeImplicitCreate, config);
    }

    @Override
    protected AlgorithmFactory<ApproxMaxKCut, ApproxMaxKCutStreamConfig> algorithmFactory() {
        return ApproxMaxKCutProc.algorithmFactory();
    }

    @Override
    protected StreamResult streamResult(
        long originalNodeId, long internalNodeId, NodeProperties nodeProperties
    ) {
        return new StreamResult(originalNodeId, nodeProperties.longValue(internalNodeId));
    }

    @Override
    protected NodeProperties nodeProperties(ComputationResult<ApproxMaxKCut, ApproxMaxKCut.CutResult, ApproxMaxKCutStreamConfig> computationResult) {
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

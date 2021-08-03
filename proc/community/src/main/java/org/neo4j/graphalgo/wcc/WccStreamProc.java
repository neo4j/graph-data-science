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
package org.neo4j.graphalgo.wcc;

import org.neo4j.gds.AlgorithmFactory;
import org.neo4j.gds.results.MemoryEstimateResult;
import org.neo4j.gds.wcc.Wcc;
import org.neo4j.gds.wcc.WccStreamConfig;
import org.neo4j.graphalgo.StreamProc;
import org.neo4j.graphalgo.api.NodeProperties;
import org.neo4j.graphalgo.api.nodeproperties.LongNodeProperties;
import org.neo4j.graphalgo.config.GraphCreateConfig;
import org.neo4j.graphalgo.core.CypherMapWrapper;
import org.neo4j.graphalgo.core.utils.paged.dss.DisjointSetStruct;
import org.neo4j.graphalgo.nodeproperties.ConsecutiveLongNodeProperties;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static org.neo4j.graphalgo.wcc.WccProc.WCC_DESCRIPTION;
import static org.neo4j.procedure.Mode.READ;

public class WccStreamProc extends StreamProc<
    Wcc,
    DisjointSetStruct,
    WccStreamProc.StreamResult,
    WccStreamConfig> {

    @Procedure(value = "gds.wcc.stream", mode = READ)
    @Description(WCC_DESCRIPTION)
    public Stream<WccStreamProc.StreamResult> stream(
        @Name(value = "graphName") Object graphNameOrConfig,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        ComputationResult<Wcc, DisjointSetStruct, WccStreamConfig> computationResult = compute(
            graphNameOrConfig,
            configuration
        );
        return stream(computationResult);
    }

    @Procedure(value = "gds.wcc.stream.estimate", mode = READ)
    @Description(ESTIMATE_DESCRIPTION)
    public Stream<MemoryEstimateResult> streamEstimate(
        @Name(value = "graphName") Object graphNameOrConfig,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        return computeEstimate(graphNameOrConfig, configuration);
    }

    @Override
    protected WccStreamConfig newConfig(
        String username,
        Optional<String> graphName,
        Optional<GraphCreateConfig> maybeImplicitCreate,
        CypherMapWrapper config
    ) {
        return WccStreamConfig.of(username, graphName, maybeImplicitCreate, config);
    }

    @Override
    protected AlgorithmFactory<Wcc, WccStreamConfig> algorithmFactory() {
        return WccProc.algorithmFactory();
    }

    @Override
    protected StreamResult streamResult(
        long originalNodeId, long internalNodeId, NodeProperties nodeProperties
    ) {
        return new WccStreamProc.StreamResult(originalNodeId, nodeProperties.longValue(internalNodeId));
    }

    @Override
    protected NodeProperties nodeProperties(ComputationResult<Wcc, DisjointSetStruct, WccStreamConfig> computationResult) {
        DisjointSetStruct dss = computationResult.result();
        LongNodeProperties simpleNodeProperties = dss.asNodeProperties();

        return computationResult.config().consecutiveIds()
            ? new ConsecutiveLongNodeProperties(simpleNodeProperties, computationResult.graph().nodeCount(), allocationTracker())
            : simpleNodeProperties;
    }

    @SuppressWarnings("unused")
    public static class StreamResult {

        public final long nodeId;

        public final long componentId;

        public StreamResult(long nodeId, long componentId) {
            this.nodeId = nodeId;
            this.componentId = componentId;
        }
    }
}

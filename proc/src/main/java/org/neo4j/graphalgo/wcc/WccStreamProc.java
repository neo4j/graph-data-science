/*
 * Copyright (c) 2017-2020 "Neo4j,"
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

import org.neo4j.graphalgo.AlgorithmFactory;
import org.neo4j.graphalgo.StreamProc;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.IdMapping;
import org.neo4j.graphalgo.config.GraphCreateConfig;
import org.neo4j.graphalgo.core.CypherMapWrapper;
import org.neo4j.graphalgo.core.utils.paged.dss.DisjointSetStruct;
import org.neo4j.graphalgo.core.write.PropertyTranslator;
import org.neo4j.graphalgo.results.MemoryEstimateResult;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Map;
import java.util.Optional;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static org.neo4j.graphalgo.wcc.WccProc.WCC_DESCRIPTION;
import static org.neo4j.procedure.Mode.READ;
import static org.neo4j.procedure.Mode.WRITE;

public class WccStreamProc extends StreamProc<
    Wcc,
    DisjointSetStruct,
    WccStreamProc.StreamResult,
    WccStreamConfig> {

    @Procedure(value = "gds.wcc.stream", mode = WRITE)
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
    protected AlgorithmFactory<Wcc, WccStreamConfig> algorithmFactory(WccStreamConfig config) {
        return WccProc.algorithmFactory();
    }

    @Override
    protected WccStreamProc.StreamResult streamResult(long originalNodeId, DisjointSetStruct computationResult) {
        throw new UnsupportedOperationException("gds.wcc.stream overrides StreamProc#stream");
    }

    @Override
    protected Stream<WccStreamProc.StreamResult> stream(ComputationResult<Wcc, DisjointSetStruct, WccStreamConfig> computationResult) {
        if (computationResult.isGraphEmpty()) {
            return Stream.empty();
        }

        DisjointSetStruct dss = computationResult.result();

        PropertyTranslator.OfLong<DisjointSetStruct> propertyTranslator = computationResult.config().consecutiveIds()
            ? new WccProc.ConsecutivePropertyTranslator(dss, computationResult.tracker())
            : (data, nodeId) -> dss.setIdOf(nodeId);

        Graph graph = computationResult.graph();
        return LongStream
            .range(IdMapping.START_NODE_ID, graph.nodeCount())
            .mapToObj(nodeId -> new WccStreamProc.StreamResult(
                    graph.toOriginalNodeId(nodeId),
                    propertyTranslator.toLong(dss, nodeId)
                )
            );
    }

    public static class StreamResult {

        public final long nodeId;

        public final long componentId;

        public StreamResult(long nodeId, long componentId) {
            this.nodeId = nodeId;
            this.componentId = componentId;
        }
    }
}

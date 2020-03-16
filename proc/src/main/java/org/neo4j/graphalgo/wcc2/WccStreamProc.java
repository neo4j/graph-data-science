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
package org.neo4j.graphalgo.wcc2;

import org.neo4j.graphalgo.AlgorithmFactory;
import org.neo4j.graphalgo.base2.StreamProc;
import org.neo4j.graphalgo.config.GraphCreateConfig;
import org.neo4j.graphalgo.core.CypherMapWrapper;
import org.neo4j.graphalgo.core.utils.paged.dss.DisjointSetStruct;
import org.neo4j.graphalgo.core.write.PropertyTranslator;
import org.neo4j.graphalgo.results.MemoryEstimateResult;
import org.neo4j.graphalgo.wcc.Wcc;
import org.neo4j.graphalgo.wcc.WccFactory;
import org.neo4j.graphalgo.wcc.WccStreamConfig;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static org.neo4j.graphalgo.wcc2.WccProc.WCC_DESCRIPTION;
import static org.neo4j.procedure.Mode.READ;
import static org.neo4j.procedure.Mode.WRITE;

public class WccStreamProc extends StreamProc<
    Wcc,
    DisjointSetStruct,
    WccStreamProc.StreamResult,
    PropertyTranslator.OfLong<DisjointSetStruct>,
    WccStreamConfig> {

    @Procedure(value = "gds.wcc.stream", mode = WRITE)
    @Description(WCC_DESCRIPTION)
    public Stream<StreamResult> stream(
        @Name(value = "graphName") Object graphNameOrConfig,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        ComputationResult2<Wcc, DisjointSetStruct, WccStreamConfig> computationResult = compute(
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
        return new WccFactory<>();
    }

    @Override
    protected StreamResult streamResult(
        long originalNodeId,
        DisjointSetStruct disjointSetStruct,
        PropertyTranslator.OfLong<DisjointSetStruct> nodePropertyTranslator
    ) {
        return new WccStreamProc.StreamResult(
            originalNodeId,
            nodePropertyTranslator.toLong(disjointSetStruct, originalNodeId)
        );
    }

    @Override
    protected PropertyTranslator.OfLong<DisjointSetStruct> nodePropertyTranslator(
        ComputationResult2<Wcc, DisjointSetStruct, WccStreamConfig> computationResult
    ) {
        return computationResult.config().consecutiveIds()
            ? new WccProc.ConsecutivePropertyTranslator(computationResult.result(), computationResult.tracker())
            : (data, nodeId) -> computationResult.result().setIdOf(nodeId);
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

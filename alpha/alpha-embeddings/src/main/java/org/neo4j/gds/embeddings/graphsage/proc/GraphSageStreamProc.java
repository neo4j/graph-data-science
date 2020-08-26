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
package org.neo4j.gds.embeddings.graphsage.proc;

import org.neo4j.gds.embeddings.graphsage.algo.GraphSage;
import org.neo4j.gds.embeddings.graphsage.algo.GraphSageStreamConfig;
import org.neo4j.graphalgo.AlgorithmFactory;
import org.neo4j.graphalgo.StreamProc;
import org.neo4j.graphalgo.api.NodeProperties;
import org.neo4j.graphalgo.config.GraphCreateConfig;
import org.neo4j.graphalgo.core.CypherMapWrapper;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import java.util.stream.Stream;

public class GraphSageStreamProc extends StreamProc<GraphSage, GraphSage.GraphSageResult, GraphSageStreamProc.GraphSageStreamResult, GraphSageStreamConfig> {

    static final String GRAPHSAGE_DESCRIPTION = "The GraphSage algorithm inductively computes embeddings for nodes based on a their features and neighborhoods.";

    @Description(GRAPHSAGE_DESCRIPTION)
    @Procedure(name = "gds.alpha.graphSage.stream", mode = Mode.READ)
    public Stream<GraphSageStreamResult> stream(
        @Name(value = "graphName") Object graphNameOrConfig,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        return stream(compute(graphNameOrConfig, configuration));
    }

    @Override
    protected Stream<GraphSageStreamResult> stream(ComputationResult<GraphSage, GraphSage.GraphSageResult, GraphSageStreamConfig> computationResult) {
        return runWithExceptionLogging("GraphSage streaming failed", () -> {
            var graph = computationResult.graph();
            var result = computationResult.result();

            if (result == null) {
                return Stream.empty();
            }

            return LongStream.range(0, graph.nodeCount())
                .mapToObj(i -> new GraphSageStreamResult(
                    graph.toOriginalNodeId(i),
                    result.embeddings().get(i)
                ));
        });
    }


    @Override
    protected GraphSageStreamConfig newConfig(
        String username,
        Optional<String> graphName,
        Optional<GraphCreateConfig> maybeImplicitCreate,
        CypherMapWrapper config
    ) {
        return GraphSageStreamConfig.of(
            username,
            graphName,
            maybeImplicitCreate,
            config
        );
    }

    @Override
    protected AlgorithmFactory<GraphSage, GraphSageStreamConfig> algorithmFactory() {
        return new GraphSageAlgorithmFactory<>();
    }

    @Override
    protected GraphSageStreamResult streamResult(
        long originalNodeId, long internalNodeId, NodeProperties nodeProperties
    ) {
        throw new UnsupportedOperationException("GraphSage handles result building individually.");
    }

    public static class GraphSageStreamResult {
        public long nodeId;
        public List<Double> embedding;

        GraphSageStreamResult(long nodeId, double[] embeddings) {
            this.nodeId = nodeId;
            this.embedding = Arrays.stream(embeddings).boxed().collect(Collectors.toList());
        }
    }
}

/*
 * Copyright (c) 2017-2019 "Neo4j,"
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
package org.neo4j.graphalgo.pagerank;

import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.CypherMapWrapper;
import org.neo4j.graphalgo.impl.pagerank.PageRank;
import org.neo4j.graphalgo.impl.results.MemoryEstimateResult;
import org.neo4j.graphalgo.newapi.GraphCreateConfig;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Map;
import java.util.Optional;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static org.neo4j.procedure.Mode.READ;

public class PageRankStreamProc extends PageRankProcBase<PageRankStreamConfig> {

    @Procedure(value = "gds.algo.pageRank.stream", mode = READ)
    @Description("CALL gds.algo.pageRank.stream(graphName: STRING, configuration: MAP {" +
                 "    maxIterations: INTEGER" +
                 "    tolerance: FLOAT" +
                 "    dampingFactor: FLOAT" +
                 "    weightProperty: STRING" +
                 "  }" +
                 ") YIELD" +
                 "  nodeId: INTEGER" +
                 "  score: FLOAT")
    public Stream<StreamResult> stream(
        @Name(value = "graphName") Object graphNameOrConfig,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        ComputationResult<PageRank, PageRank, PageRankStreamConfig> computationResult = compute(
            graphNameOrConfig,
            configuration
        );
        return stream(computationResult);
    }

    @Procedure(value = "gds.algo.pageRank.stream.estimate", mode = READ)
    @Description("CALL gds.algo.pageRank.stream.estimate(graphName: STRING, configuration: MAP {" +
                 "    maxIterations: INTEGER" +
                 "    tolerance: FLOAT" +
                 "    dampingFactor: FLOAT" +
                 "    weightProperty: STRING" +
                 "  }" +
                 ") YIELD" +
                 "  nodes: INTEGER, "+
                 "  relationships: INTEGER," +
                 "  bytesMin: INTEGER," +
                 "  bytesMax: INTEGER," +
                 "  requiredMemory: STRING," +
                 "  mapView: MAP," +
                 "  treeView: STRING")
    public Stream<MemoryEstimateResult> estimate(
        @Name(value = "graphName") Object graphNameOrConfig,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        return computeMemoryEstimate(graphNameOrConfig, configuration);
    }

    private Stream<StreamResult> stream(ComputationResult<PageRank, PageRank, PageRankStreamConfig> computationResult) {
        Graph graph = computationResult.graph();
        PageRank pageRank = computationResult.result();
        return LongStream.range(0, graph.nodeCount())
            .mapToObj(nodeId -> {
                long neoNodeId = graph.toOriginalNodeId(nodeId);
                double score = pageRank.result().score(nodeId);
                return new StreamResult(neoNodeId, score);
            });
    }

    @Override
    protected PageRankStreamConfig newConfig(
        String username,
        Optional<String> graphName,
        Optional<GraphCreateConfig> maybeImplicitCreate,
        CypherMapWrapper config
    ) {
        return PageRankStreamConfig.of(username, graphName, maybeImplicitCreate, config);
    }

    public static final class StreamResult {
        public final long nodeId;
        public final double score;

        StreamResult(long nodeId, double score) {
            this.nodeId = nodeId;
            this.score = score;
        }
    }
}

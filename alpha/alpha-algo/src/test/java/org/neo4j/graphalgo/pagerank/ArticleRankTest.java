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
package org.neo4j.graphalgo.pagerank;

import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.utils.BatchingProgressLogger;
import org.neo4j.graphalgo.extension.GdlExtension;
import org.neo4j.graphalgo.extension.GdlGraph;
import org.neo4j.graphalgo.extension.IdFunction;
import org.neo4j.graphalgo.extension.Inject;
import org.neo4j.graphalgo.result.CentralityResult;
import org.neo4j.logging.NullLog;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

import static org.junit.jupiter.api.Assertions.assertEquals;

@GdlExtension
final class ArticleRankTest {

    private static final PageRankBaseConfig DEFAULT_CONFIG = ImmutablePageRankStreamConfig
        .builder()
        .maxIterations(40)
        .build();

    @GdlGraph
    private static final String DB_CYPHER =
            "CREATE" +
            "  (a:Label1)" +
            ", (b:Label1)" +
            ", (c:Label1)" +
            ", (d:Label1)" +
            ", (e:Label1)" +
            ", (f:Label1)" +
            ", (g:Label1)" +
            ", (h:Label1)" +
            ", (i:Label1)" +
            ", (j:Label1)" +

            ", (b)-[:TYPE1]->(c)" +
            ", (c)-[:TYPE1]->(b)" +

            ", (d)-[:TYPE1]->(a)" +
            ", (d)-[:TYPE1]->(b)" +

            ", (e)-[:TYPE1]->(b)" +
            ", (e)-[:TYPE1]->(d)" +
            ", (e)-[:TYPE1]->(f)" +

            ", (f)-[:TYPE1]->(b)" +
            ", (f)-[:TYPE1]->(e)";

    @Inject
    private Graph graph;

    @Inject
    private IdFunction idFunction;

    @Test
    void test() {
        Map<Long, Double> expected = new HashMap<>();

            expected.put(idFunction.of("a"), 0.2071625);
            expected.put(idFunction.of("b"), 0.4706795);
            expected.put(idFunction.of("c"), 0.3605195);
            expected.put(idFunction.of("d"), 0.195118);
            expected.put(idFunction.of("e"), 0.2071625);
            expected.put(idFunction.of("f"), 0.195118);
            expected.put(idFunction.of("g"), 0.15);
            expected.put(idFunction.of("h"), 0.15);
            expected.put(idFunction.of("i"), 0.15);
            expected.put(idFunction.of("j"), 0.15);

        CentralityResult rankResult = LabsPageRankAlgorithmType.ARTICLE_RANK
            .create(
                graph,
                DEFAULT_CONFIG,
                LongStream.empty(),
                new BatchingProgressLogger(NullLog.getInstance(), 0, "PageRank", DEFAULT_CONFIG.concurrency())
            ).compute()
            .result();

        IntStream.range(0, expected.size()).forEach(i -> {
            final long nodeId = graph.toOriginalNodeId(i);
            assertEquals(
                expected.get(nodeId),
                rankResult.score(i),
                1e-2,
                "Node#" + nodeId
            );
        });
    }
}

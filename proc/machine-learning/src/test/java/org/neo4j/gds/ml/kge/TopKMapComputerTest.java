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
package org.neo4j.gds.ml.kge;

import com.carrotsearch.hppc.BitSet;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.extension.TestGraph;
import org.neo4j.gds.similarity.SimilarityResult;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.gds.ml.kge.ScoreFunction.DISTMULT;
import static org.neo4j.gds.ml.kge.ScoreFunction.TRANSE;

@GdlExtension
class TopKMapComputerTest {

    @GdlGraph
    private static final String DB_CYPHER =
        "CREATE" +
            "  (a:N {emb: [-3.0, 3.0]})" +
            ", (b:N {emb: [-4.0, 4.0]})" +
            ", (c:N {emb: [-5.0, 5.0]})" +
            ", (d:M {emb: [0.1, 1.0]})" +
            ", (e:M {emb: [0.2, 2.0]})" +
            ", (f:M {emb: [0.3, 3.0]})" +
            ", (a)-[:REL {prop: 1.0}]->(b)" +
            ", (b)-[:REL {prop: 1.0}]->(a)" +
            ", (a)-[:REL {prop: 1.0}]->(c)" +
            ", (c)-[:REL {prop: 1.0}]->(a)" +
            ", (b)-[:REL {prop: 1.0}]->(c)" +
            ", (c)-[:REL {prop: 1.0}]->(b)";

    @Inject
    private TestGraph graph;

    @Test
    void shouldComputeTopKMapTransE() {
        var sourceNodes = create(graph.toMappedNodeId("a"), graph.toMappedNodeId("b"), graph.toMappedNodeId("c"));
        var targetNodes = create(graph.toMappedNodeId("d"), graph.toMappedNodeId("e"), graph.toMappedNodeId("f"));
        var topK = 1;
        var concurrency = 4;

        var computer = new TopKMapComputer(
            graph,
            sourceNodes,
            targetNodes,
            "emb",
            List.of(3.0, -0.5),
            TRANSE,
            topK,
            concurrency,
            ProgressTracker.NULL_TRACKER
        );

        KGEPredictResult result = computer.compute();
        assertTrue(assertTopKApproximatelyEquals(
                result.topKMap().stream().collect(Collectors.toList()),
                List.of(
                    new SimilarityResult(0L, 4L, 0.538),
                    new SimilarityResult(1L, 5L, 1.393),
                    new SimilarityResult(2L, 5L, 2.746)
                ),
                0.001
            )
        );

    }

    @Test
    void shouldComputeTopKMapDistMult() {
        var sourceNodes = create(graph.toMappedNodeId("a"), graph.toMappedNodeId("b"), graph.toMappedNodeId("c"));
        var targetNodes = create(graph.toMappedNodeId("d"), graph.toMappedNodeId("e"), graph.toMappedNodeId("f"));
        var topK = 1;
        var concurrency = 4;

        var computer = new TopKMapComputer(
            graph,
            sourceNodes,
            targetNodes,
            "emb",
            List.of(0.5, -0.5),
            DISTMULT,
            topK,
            concurrency,
            ProgressTracker.NULL_TRACKER
        );

        KGEPredictResult result = computer.compute();
        assertTrue(assertTopKApproximatelyEquals(
                result.topKMap().stream().collect(Collectors.toList()),
                List.of(
                    new SimilarityResult(0L, 3L, -1.65),
                    new SimilarityResult(1L, 3L, -2.2),
                    new SimilarityResult(2L, 3L, -2.75)
                ),
                0.01
            )
        );

    }

    @Test
    void shouldComputeOverCorrectFiltering() {
        var sourceNodes = create(graph.toMappedNodeId("a"), graph.toMappedNodeId("b"), graph.toMappedNodeId("c"));
        var targetNodes = create(graph.toMappedNodeId("a"), graph.toMappedNodeId("b"), graph.toMappedNodeId("c"),
            graph.toMappedNodeId("d")
        );
        var topK = 10;
        var concurrency = 4;

        var computer = new TopKMapComputer(
            graph,
            sourceNodes,
            targetNodes,
            "emb",
            List.of(0.5, -0.5),
            TRANSE,
            topK,
            concurrency,
            ProgressTracker.NULL_TRACKER
        );

        KGEPredictResult result = computer.compute();
        assertTrue(assertTopKApproximatelyEquals(
                result.topKMap().stream().collect(Collectors.toList()),
                List.of(
                    new SimilarityResult(0L, 3L, 3.00),
                    new SimilarityResult(1L, 3L, 4.38),
                    new SimilarityResult(2L, 3L, 5.78)
                ),
                0.01
            )
        );

    }

    private BitSet create(long... ids) {
        long capacity = Arrays.stream(ids).max().orElse(0);

        BitSet bitSet = new BitSet(capacity + 1);

        for (long id : ids) {
            bitSet.set(id);
        }

        return bitSet;
    }

    private boolean assertTopKApproximatelyEquals(
        List<SimilarityResult> actual,
        List<SimilarityResult> expected,
        double epsilon
    ) {
        if (actual.size() != expected.size()) {
            return false;
        }

        for (int i = 0; i < actual.size(); i++) {
            var actualSimilarity = actual.get(i);
            var expectedSimilarity = expected.get(i);
            if (!approximatelyEquals(actualSimilarity, expectedSimilarity, epsilon)) {
                return false;
            }
        }

        return true;
    }

    private boolean approximatelyEquals(SimilarityResult actualSimilarity, SimilarityResult expectedSimilarity, double epsilon) {
        return actualSimilarity.node1 == expectedSimilarity.node1 &&
            actualSimilarity.node2 == expectedSimilarity.node2 &&
            Math.abs(actualSimilarity.similarity - expectedSimilarity.similarity) <= epsilon;
    }
}

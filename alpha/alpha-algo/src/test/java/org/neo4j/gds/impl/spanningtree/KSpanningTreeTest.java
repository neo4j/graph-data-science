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
package org.neo4j.gds.impl.spanningtree;

import org.apache.commons.lang3.mutable.MutableLong;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.neo4j.gds.Orientation;
import org.neo4j.gds.TestProgressTracker;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.compat.Neo4jProxy;
import org.neo4j.gds.compat.TestLog;
import org.neo4j.gds.core.utils.progress.EmptyTaskRegistryFactory;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.IdFunction;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.gdl.GdlFactory;
import org.neo4j.gds.spanningtree.Prim;

import java.util.HashSet;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.gds.assertj.Extractors.removingThreadId;
import static org.neo4j.gds.assertj.Extractors.replaceTimings;

/**
 *           1
 *  (x), (a)---(d)    (x)  (a)   (d)
 *       /3 \2 /3   =>     /     /
 *     (b)---(c)         (b)   (c)
 *         1
 */
@GdlExtension
class KSpanningTreeTest {

    @GdlGraph(orientation = Orientation.UNDIRECTED)
    private static final String DB_CYPHER =
            "CREATE " +
            "  (a:Node)" +
            ", (b:Node)" +
            ", (c:Node)" +
            ", (d:Node)" +
            ", (x:Node)" +

            ", (a)-[:TYPE {w: 3.0}]->(b)" +
            ", (a)-[:TYPE {w: 2.0}]->(c)" +
            ", (a)-[:TYPE {w: 1.0}]->(d)" +
            ", (b)-[:TYPE {w: 1.0}]->(c)" +
            ", (d)-[:TYPE {w: 3.0}]->(c)";

    @Inject
    private Graph graph;

    @Inject
    private IdFunction idFunction;

    private static final int OFFSET = 5;

    @GdlGraph(idOffset = OFFSET, orientation = Orientation.UNDIRECTED, graphNamePrefix = "offset")
    private static final String DB_CYPHER_WITH_OFFSET = DB_CYPHER;

    @Inject
    private Graph offsetGraph;

    private int a, b, c, d, x;

    @BeforeEach
    void setUp() {
        a = (int) idFunction.of("a");
        b = (int) idFunction.of("b");
        c = (int) idFunction.of("c");
        d = (int) idFunction.of("d");
        x = (int) idFunction.of("x");
    }

    @Test
    void testMaximumKSpanningTree() {
        var spanningTree = new KSpanningTree(graph, Prim.MAX_OPERATOR, a, 2, ProgressTracker.NULL_TRACKER)
            .compute();

        assertThat(spanningTree).matches(tree -> tree.head(a) == tree.head(b) ^ tree.head(c) == tree.head(d));
        assertThat(spanningTree.head(a)).isNotEqualTo(spanningTree.head(c));
        assertThat(spanningTree.head(a)).isNotEqualTo(spanningTree.head(x));
        assertThat(spanningTree.head(c)).isNotEqualTo(spanningTree.head(x));
    }

    @Test
    void testMinimumKSpanningTree() {
        var spanningTree = new KSpanningTree(graph, Prim.MIN_OPERATOR, a, 2, ProgressTracker.NULL_TRACKER)
            .compute();

        assertThat(spanningTree).matches(tree -> tree.head(a) == tree.head(d) ^ tree.head(b) == tree.head(c));
        assertThat(spanningTree.head(a)).isNotEqualTo(spanningTree.head(b));
        assertThat(spanningTree.head(a)).isNotEqualTo(spanningTree.head(x));
        assertThat(spanningTree.head(b)).isNotEqualTo(spanningTree.head(x));
    }

    @Test
    void testNeoIdsWithOffset() {
        var spanningTree = new KSpanningTree(graph, Prim.MIN_OPERATOR, 0, 2, ProgressTracker.NULL_TRACKER).compute();
        var otherSpanningTree = new KSpanningTree(offsetGraph, Prim.MIN_OPERATOR, OFFSET, 2, ProgressTracker.NULL_TRACKER).compute();

        assertThat(spanningTree.parentArray().toArray())
            .containsExactly(otherSpanningTree.parentArray().toArray());
    }

    @Test
    void shouldProduceSingleConnectedTree() {
        var factory = GdlFactory.of("CREATE" +
                                    "  (a:Node)" +
                                    ", (b:Node)" +
                                    ", (c:Node)" +
                                    ", (d:Node)" +
                                    ", (e:Node)" +
                                    ", (a)-[:TYPE {cost: 1.0}]->(b)" +
                                    ", (b)-[:TYPE {cost: 20.0}]->(c)" +
                                    ", (c)-[:TYPE {cost: 30.0}]->(d)" +
                                    ", (d)-[:TYPE {cost: 1.0}]->(e)"
        );
        var graph = factory.build().getUnion();
        var startNode = factory.nodeId("a");

        var k = 3;
        var spanningTree = new KSpanningTree(
            graph,
            Prim.MIN_OPERATOR,
            startNode,
            k,
            ProgressTracker.NULL_TRACKER
        ).compute();

        // if there are more than k nodes then there is more than one root
        // meaning there is more than one tree (or the tree is broken)
        var nodesInTree = new HashSet<Long>();
        spanningTree.forEach((s, t, __) -> {
            nodesInTree.add(s);
            nodesInTree.add(t);
            return true;
        });

        assertThat(nodesInTree.size()).isEqualTo(k);
    }

    @ParameterizedTest
    @CsvSource({"2,1.0", "3,2.0"})
    void shouldProduceSingleTreeWithKMinusOneEdges(int k, double expected) {
        var factory = GdlFactory.of("CREATE" +
                                    "  (a:Node)" +
                                    ", (b:Node)" +
                                    ", (c:Node)" +
                                    ", (d:Node)" +
                                    ", (e:Node)" +
                                    ", (f:Node)" +
                                    ", (a)-[:TYPE {cost: 1.0}]->(b)" +
                                    ", (b)-[:TYPE {cost: 20.0}]->(c)" +
                                    ", (c)-[:TYPE {cost: 30.0}]->(d)" +
                                    ", (d)-[:TYPE {cost: 1.0}]->(e)" +
                                    ", (e)-[:TYPE {cost: 1.0}]->(f)"
        );
        var graph = factory.build().getUnion();
        var startNode = factory.nodeId("a");


        var spanningTree = new KSpanningTree(
            graph,
            Prim.MIN_OPERATOR,
            startNode,
            k,
            ProgressTracker.NULL_TRACKER
        ).compute();

        var counter = new MutableLong(0);
        spanningTree.forEach((__, ___, ____) -> {
            counter.add(1);
            return true;
        });

        assertThat(counter.getValue()).isEqualTo(k - 1);

        assertThat(spanningTree.totalWeight()).isEqualTo(expected);
    }

    @Test
    void worstCaseForCuttingHeaviest() {
        var factory = GdlFactory.of("CREATE" +
                                    "  (a:Node)" +
                                    ", (b:Node)" +
                                    ", (c:Node)" +
                                    ", (d:Node)" +
                                    ", (e:Node)" +
                                    ", (f:Node)" +
                                    ", (g:Node)" +
                                    ", (h:Node)" +
                                    ", (a)-[:TYPE {cost: 9.0}]->(b)" +
                                    ", (b)-[:TYPE {cost: 9.0}]->(c)" +
                                    ", (c)-[:TYPE {cost: 0.0}]->(d)" +
                                    ", (d)-[:TYPE {cost: 10.0}]->(e)" +
                                    ", (e)-[:TYPE {cost: 0.0}]->(f)" +
                                    ", (f)-[:TYPE {cost: 9.0}]->(g)" +
                                    ", (g)-[:TYPE {cost: 9.0}]->(h)"

        );
        var graph = factory.build().getUnion();
        var startNode = factory.nodeId("a");


        var spanningTree = new KSpanningTree(
            graph,
            Prim.MIN_OPERATOR,
            startNode,
            4,
            ProgressTracker.NULL_TRACKER
        ).compute();

        var counter = new MutableLong(0);
        spanningTree.forEach((__, ___, ____) -> {
            counter.add(1);
            return true;
        });

        assertThat(counter.getValue()).isEqualTo(4 - 1);
        assertThat(spanningTree.totalWeight()).isEqualTo(10.0);
        //here a 'cut-heaviest'  approach would begin by cutting edge 10.
        //this would prune the tree into two smaller subtrees of 4 nodes each
        //adn thus return a 18 despite the optimal being 10.
        //this is one of the many situations where cut-heavy-fails

    }

    @Test
    void worstCaseForPruningLeaves() {
        var factory = GdlFactory.of("CREATE" +
                                    "  (a:Node)" +
                                    ", (b:Node)" +
                                    ", (c:Node)" +
                                    ", (d:Node)" +
                                    ", (e:Node)" +
                                    ", (f:Node)" +
                                    ", (g:Node)" +
                                    ", (a)-[:TYPE {cost: 9.0}]->(b)" +
                                    ", (b)-[:TYPE {cost: 0.0}]->(c)" +
                                    ", (c)-[:TYPE {cost: 0.0}]->(d)" +
                                    ", (a)-[:TYPE {cost: 1.0}]->(e)" +
                                    ", (e)-[:TYPE {cost: 1.0}]->(f)" +
                                    ", (f)-[:TYPE {cost: 1.0}]->(g)"

        );
        var graph = factory.build().getUnion();
        var startNode = factory.nodeId("a");


        var spanningTree = new KSpanningTree(
            graph,
            Prim.MIN_OPERATOR,
            startNode,
            4,
            ProgressTracker.NULL_TRACKER
        ).compute();

        var counter = new MutableLong(0);
        spanningTree.forEach((__, ___, ____) -> {
            counter.add(1);
            return true;
        });

        assertThat(counter.getValue()).isEqualTo(4 - 1);
        assertThat(spanningTree.totalWeight()).isEqualTo(3.0);
        //here a bad case for pruning just leaves
        /// edge weight should be eliminated for the final solution but it is not because
        //its leaves are not good.

    }

    @Test
    void shouldWorkForComponentSmallerThanK() {
        var factory = GdlFactory.of("CREATE" +
                                    "  (a:Node)" +
                                    ", (b:Node)" +
                                    ", (c:Node)" +
                                    ", (d:Node)" +
                                    ", (e:Node)" +
                                    ", (f:Node)" +
                                    ", (g:Node)" +
                                    ", (a)-[:TYPE {cost: 1.0}]->(b)" +
                                    ", (b)-[:TYPE {cost: 1.0}]->(c)" +
                                    ", (c)-[:TYPE {cost: 1.0}]->(d)");

        var graph = factory.build().getUnion();
        var startNode = factory.nodeId("a");

        var spanningTree = new KSpanningTree(
            graph,
            Prim.MIN_OPERATOR,
            startNode,
            5,
            ProgressTracker.NULL_TRACKER
        ).compute();

        assertThat(spanningTree.effectiveNodeCount()).isEqualTo(4);

    }

    @Test
    void shouldLogProgress() {
        var config = KSpanningTreeBaseConfigImpl.builder().sourceNode(idFunction.of("a")).k(2).build();
        var factory = new KSpanningTreeAlgorithmFactory<>();
        var log = Neo4jProxy.testLog();
        var progressTracker = new TestProgressTracker(
            factory.progressTask(graph, config),
            log,
            1,
            EmptyTaskRegistryFactory.INSTANCE
        );
        factory.build(graph, config, progressTracker).compute();
        assertThat(log.getMessages(TestLog.INFO))
            .extracting(removingThreadId())
            .extracting(replaceTimings())
            .containsExactly(
                "KSpanningTree :: Start",
                "KSpanningTree :: SpanningTree :: Start",
                "KSpanningTree :: SpanningTree 30%",
                "KSpanningTree :: SpanningTree 50%",
                "KSpanningTree :: SpanningTree 80%",
                "KSpanningTree :: SpanningTree 100%",
                "KSpanningTree :: SpanningTree :: Finished",
                "KSpanningTree :: Remove relationships 1 :: Start",
                "KSpanningTree :: Remove relationships 1 50%",
                "KSpanningTree :: Remove relationships 1 100%",
                "KSpanningTree :: Remove relationships 1 :: Finished",
                "KSpanningTree :: Remove relationships 2 :: Start",
                "KSpanningTree :: Remove relationships 2 20%",
                "KSpanningTree :: Remove relationships 2 40%",
                "KSpanningTree :: Remove relationships 2 60%",
                "KSpanningTree :: Remove relationships 2 100%",
                "KSpanningTree :: Remove relationships 2 :: Finished",
                "KSpanningTree :: Finished"
            );
    }

}

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
package org.neo4j.gds.embeddings.node2vec;

import org.assertj.core.data.Percentage;
import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.AlgoTestBase;
import org.neo4j.graphalgo.TestGraphLoader;
import org.neo4j.graphalgo.api.Graph;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.graphalgo.TestSupport.FactoryType.NATIVE;
import static org.neo4j.graphalgo.TestSupport.fromGdl;

class RandomWalkTest extends AlgoTestBase {

    private static final int NODE_COUNT = 5;
    private static final String DEFAULT_DB_CYPHER =
        "CREATE" +
        "  (a:Node1)" +
        ", (b:Node1)" +
        ", (c:Node2)" +
        ", (d:Isolated)" +
        ", (e:Isolated)" +
        ", (a)-[:REL1]->(b)" +
        ", (b)-[:REL1]->(a)" +
        ", (a)-[:REL1]->(c)" +
        ", (c)-[:REL2]->(a)" +
        ", (b)-[:REL2]->(c)" +
        ", (c)-[:REL2]->(b)";

    @Test
    void testWithDefaultConfig() {
        runQuery(DEFAULT_DB_CYPHER);
        Node2VecStreamConfig config = ImmutableNode2VecStreamConfig.builder().build();
        Graph graph = TestGraphLoader.from(db).graph(NATIVE);
        RandomWalk randomWalk = new RandomWalk(
            graph,
            config.walkLength(),
            config.concurrency(),
            config.walksPerNode(),
            config.walkBufferSize(),
            config.returnFactor(),
            config.inOutFactor()
        );

        int expectedNumberOfWalks = config.walksPerNode() * 3;
        List<long[]> result = randomWalk.compute().collect(Collectors.toList());
        assertEquals(expectedNumberOfWalks, result.size());
        long[] walkForNodeZero = result
            .stream()
            .filter(arr -> graph.toOriginalNodeId(arr[0]) == 0)
            .findFirst()
            .orElse(new long[0]);
        int expectedStepsInWalkForNode0 = config.walkLength() + 1;
        assertEquals(expectedStepsInWalkForNode0, walkForNodeZero.length);
    }

    @Test
    void testSampleFromMultipleRelationshipTypes() {
        runQuery(DEFAULT_DB_CYPHER);
        Node2VecStreamConfig config = ImmutableNode2VecStreamConfig.builder().build();
        Graph graph = TestGraphLoader.from(db).withRelationshipTypes("REL1", "REL2").graph(NATIVE);
        RandomWalk randomWalk = new RandomWalk(
            graph,
            config.walkLength(),
            config.concurrency(),
            config.walksPerNode(),
            config.walkBufferSize(),
            config.returnFactor(),
            config.inOutFactor()
        );

        int expectedNumberOfWalks = config.walksPerNode() * 3;
        List<long[]> result = randomWalk.compute().collect(Collectors.toList());
        assertEquals(expectedNumberOfWalks, result.size());
        long[] walkForNodeZero = result.stream().filter(arr -> graph.toOriginalNodeId(arr[0]) == 0).findFirst().orElse(new long[0]);
        int expectedStepsInWalkForNode0 = config.walkLength();
        long[] walkForNodeZero = result
            .stream()
            .filter(arr -> graph.toOriginalNodeId(arr[0]) == 0)
            .findFirst()
            .orElse(new long[0]);
        int expectedStepsInWalkForNode0 = config.walkLength() + 1;
        assertEquals(expectedStepsInWalkForNode0, walkForNodeZero.length);
    }

    @Test
    void returnFactorShouldMakeWalksIncludeStartNodeMoreOften() {
        runQuery("CREATE (a:Node)" +
                 ", (a)-[:REL]->(b:Node)-[:REL]->(a)" +
                 ", (b)-[:REL]->(c:Node)-[:REL]->(a)" +
                 ", (c)-[:REL]->(d:Node)-[:REL]->(a)" +
                 ", (d)-[:REL]->(e:Node)-[:REL]->(a)" +
                 ", (e)-[:REL]->(f:Node)-[:REL]->(a)" +
                 ", (f)-[:REL]->(g:Node)-[:REL]->(a)" +
                 ", (g)-[:REL]->(h:Node)-[:REL]->(a)");

        Graph graph = TestGraphLoader.from(db).graph(NATIVE);
        RandomWalk randomWalk = new RandomWalk(
            graph,
            10,
            4,
            100,
            1000,
            0.01,
            1
        );

        var nodeCounter = new HashMap<Long, Long>();
        randomWalk
            .compute()
            .filter(arr -> graph.toOriginalNodeId(arr[0]) == 0)
            .forEach(arr -> Arrays.stream(arr).forEach(n -> {
                    long neo4jId = graph.toOriginalNodeId(n);
                    nodeCounter.merge(neo4jId, 1L, Long::sum);
                })
            );

        // (a) and (b) have similar occurrences, since from (a) the only reachable node is (b)
        assertThat(Math.abs(nodeCounter.get(0L) - nodeCounter.get(1L)) <= 100)
            .withFailMessage("occurrences: %s", nodeCounter)
            .isTrue();

        // all other nodes should occur far less often because of the high return probability
        assertThat(nodeCounter.get(0L) > nodeCounter.getOrDefault(2L, 0L) * 40)
            .withFailMessage("occurrences: %s", nodeCounter)
            .isTrue();
    }


    /*
            +-->B+----+
            |   +     |
            |   |     |
            +   |     +
        +-->A+------->C+->E+->F
        |   +   |     +       +
        |   |   |     |       |
        |   |   |     |       |
        |   |   +     |       |
        |   +-->D+----+       |
        |                     |
        +---------------------+
     */
    @Test
    void largeInOutFactorShouldMakeTheWalkKeepTheSameDistance() {
        runQuery("CREATE " +
                 "  (a:Node)" +
                 ", (b:Node)" +
                 ", (c:Node)" +
                 ", (d:Node)" +
                 ", (e:Node)" +
                 ", (f:Node)" +
                 ", (a)-[:REL]->(b)" +
                 ", (a)-[:REL]->(c)" +
                 ", (a)-[:REL]->(d)" +
                 ", (b)-[:REL]->(c)" +
                 ", (b)-[:REL]->(d)" +
                 ", (b)-[:REL]->(e)" +
                 ", (c)-[:REL]->(b)" +
                 ", (c)-[:REL]->(d)" +
                 ", (c)-[:REL]->(e)" +
                 ", (d)-[:REL]->(b)" +
                 ", (d)-[:REL]->(c)" +
                 ", (d)-[:REL]->(e)" +
                 ", (e)-[:REL]->(f)" +
                 ", (f)-[:REL]->(a)");

        Graph graph = TestGraphLoader.from(db).graph(NATIVE);
        RandomWalk randomWalk = new RandomWalk(
            graph,
            10,
            4,
            1000,
            1000,
            0.1,
            10
        );

        var nodeCounter = new HashMap<Long, Long>();
        randomWalk
            .compute()
            .filter(arr -> graph.toOriginalNodeId(arr[0]) == 0)
            .forEach(arr -> Arrays.stream(arr).forEach(n -> {
                    long neo4jId = graph.toOriginalNodeId(n);
                    nodeCounter.merge(neo4jId, 1L, Long::sum);
                })
            );

        // (b), (c), (d) should be much more common than (e) and (f)
        assertThat(nodeCounter.get(1L) > nodeCounter.get(4L) * 10)
            .withFailMessage("occurrences: %s", nodeCounter)
            .isTrue();
        assertThat(nodeCounter.get(1L) > nodeCounter.get(5L) * 10)
            .withFailMessage("occurrences: %s", nodeCounter)
            .isTrue();

        assertThat(nodeCounter.get(2L) > nodeCounter.get(4L) * 10)
            .withFailMessage("occurrences: %s", nodeCounter)
            .isTrue();
        assertThat(nodeCounter.get(2L) > nodeCounter.get(5L) * 10)
            .withFailMessage("occurrences: %s", nodeCounter)
            .isTrue();

        assertThat(nodeCounter.get(3L) > nodeCounter.get(4L) * 10)
            .withFailMessage("occurrences: %s", nodeCounter)
            .isTrue();
        assertThat(nodeCounter.get(3L) > nodeCounter.get(5L) * 10)
            .withFailMessage("occurrences: %s", nodeCounter)
            .isTrue();
    }

    @Test
    void shouldRespectRelationshipWeights() {
        var graph = fromGdl(
            "  (a:Node)" +
            ", (b:Node)" +
            ", (c:Node)" +
            ", (a)-[:REL {weight: 100.0}]->(b)" +
            ", (a)-[:REL {weight: 1.0}]->(c)" +
            ", (b)-[:REL {weight: 1.0}]->(a)" +
            ", (c)-[:REL {weight: 1.0}]->(a)"
        );

        RandomWalk randomWalk = new RandomWalk(
            graph,
            10,
            new RandomWalk.NextNodeStrategy(graph, 1, 1),
            4,
            1000,
            1000
        );

        var nodeCounter = new HashMap<Long, Long>();
        randomWalk
            .compute()
            .filter(arr -> graph.toOriginalNodeId(arr[0]) == 0)
            .forEach(arr -> Arrays.stream(arr).forEach(n -> {
                    long neo4jId = graph.toOriginalNodeId(n);
                    nodeCounter.merge(neo4jId, 1L, Long::sum);
                })
            );

        assertThat(nodeCounter.get(2L) * 100).isCloseTo(nodeCounter.get(1L), Percentage.withPercentage(25.0));
    }
}

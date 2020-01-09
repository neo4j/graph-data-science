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
package org.neo4j.graphalgo;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;

import java.util.Arrays;
import java.util.List;
import java.util.stream.StreamSupport;

import static java.util.stream.Collectors.toList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.graphalgo.QueryRunner.runInTransaction;

/**
 * Graph:
 *
 *            (0)
 *          /  |  \
 *       (4)--(5)--(1)
 *         \  /  \ /
 *         (3)---(2)
 */
class UtilityProcsTest extends BaseProcTest {

    @BeforeEach
    void setupGraph() throws Exception {
        db = TestDatabaseCreator.createTestDatabase();
        String cypher =
                "CREATE (a:Node {name:'a'})\n" +
                "CREATE (b:Node {name:'b'})\n" +
                "CREATE (c:Node {name:'c'})\n" +
                "CREATE (d:Node {name:'d'})\n" +
                "CREATE (e:Node {name:'e'})\n" +
                "CREATE (f:Node {name:'f'})\n";

        runQuery(cypher);
        registerProcedures(UtilityProc.class);
    }

    @AfterEach
    void teardownGraph() {
        db.shutdown();
    }

    @Test
    void shouldReturnPaths() {
        final String cypher = "CALL algo.asPath([0, 1,2])";

        List<Node> expectedNodes = getNodes("a", "b", "c");

        runQueryWithRowConsumer(cypher, row -> {
            Path path = (Path) row.get("path");
            List<Node> actualNodes = StreamSupport.stream(path.nodes().spliterator(), false).collect(toList());
            assertEquals(expectedNodes, actualNodes);
        });
    }

    @Test
    void shouldReturnPathsWithCosts() {
        final String cypher = "CALL algo.asPath([0,1,2], [0.1,0.2], {cumulativeWeights: false})";

        List<Node> expectedNodes = getNodes("a", "b", "c");
        List<Double> expectedCosts = Arrays.asList(0.1, 0.2);

        runQueryWithRowConsumer(cypher, row -> {
            Path path = (Path) row.get("path");

            List<Node> actualNodes = StreamSupport.stream(path.nodes().spliterator(), false).collect(toList());
            List<Double> actualCosts = StreamSupport.stream(path.relationships().spliterator(), false)
                    .map(rel -> (double)rel.getProperty("cost")).collect(toList());

            assertEquals(expectedNodes, actualNodes);
            assertEquals(expectedCosts, actualCosts);
        });
    }

    @Test
    void shouldThrowExceptionIfNotEnoughCostsProvided() {
        String cypher = "CALL algo.asPath([0,1,2], [0.1], {cumulativeWeights: false})";
        assertThrows(RuntimeException.class, () -> runQuery(cypher), "'weights' contains 1 values, but 2 values were expected");
    }

    @Test
    void shouldPreprocessCumulativeWeights() {
        String cypher = "CALL algo.asPath([0,1,2], [0, 40.0, 70.0])";

        List<Node> expectedNodes = getNodes("a", "b", "c");
        List<Double> expectedCosts = Arrays.asList(40.0, 30.0);

        runQueryWithRowConsumer(cypher, row -> {
            Path path = (Path) row.get("path");

            List<Node> actualNodes = StreamSupport.stream(path.nodes().spliterator(), false).collect(toList());
            List<Double> actualCosts = StreamSupport.stream(path.relationships().spliterator(), false)
                    .map(rel -> (double)rel.getProperty("cost")).collect(toList());

            assertEquals(expectedNodes, actualNodes);
            assertEquals(expectedCosts, actualCosts);
        });
    }

    @Test
    void shouldThrowExceptionIfNotEnoughCumulativeWeightsProvided() {
        String cypher = "CALL algo.asPath([0,1,2], [0, 40.0])";
        assertThrows(RuntimeException.class, () -> runQuery(cypher), "'weights' contains 2 values, but 3 values were expected");
    }

    private List<Node> getNodes(String... nodes) {
        return runInTransaction(db, () ->
            Arrays.stream(nodes)
                .map(name -> db.findNode(Label.label("Node"), "name", name))
                .collect(toList())
        );
    }
}

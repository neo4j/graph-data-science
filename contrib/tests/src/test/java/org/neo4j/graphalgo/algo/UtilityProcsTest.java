/*
 * Copyright (c) 2017 "Neo4j, Inc." <http://neo4j.com>
 *
 * This file is part of Neo4j Graph Algorithms <http://github.com/neo4j-contrib/neo4j-graph-algorithms>.
 *
 * Neo4j Graph Algorithms is free software: you can redistribute it and/or modify
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
package org.neo4j.graphalgo.algo;

import org.hamcrest.CoreMatchers;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.neo4j.graphalgo.KShortestPathsProc;
import org.neo4j.graphalgo.UtilityProc;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.test.rule.ImpermanentDatabaseRule;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.StreamSupport;

import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.matchers.JUnitMatchers.containsString;

/**
 * Graph:
 *
 *            (0)
 *          /  |  \
 *       (4)--(5)--(1)
 *         \  /  \ /
 *         (3)---(2)
 *
 * @author mknblch
 */
public class UtilityProcsTest {

    @ClassRule
    public static ImpermanentDatabaseRule DB = new ImpermanentDatabaseRule();

    @BeforeClass
    public static void setupGraph() throws KernelException {

        final String cypher =
                "CREATE (a:Node {name:'a'})\n" +
                        "CREATE (b:Node {name:'b'})\n" +
                        "CREATE (c:Node {name:'c'})\n" +
                        "CREATE (d:Node {name:'d'})\n" +
                        "CREATE (e:Node {name:'e'})\n" +
                        "CREATE (f:Node {name:'f'})\n";

        DB.execute(cypher);
        DB.resolveDependency(Procedures.class).registerProcedure(UtilityProc.class);
    }


    @Test
    public void shouldReturnPaths() throws Exception {
        final String cypher = "CALL algo.asPath([0, 1,2])";

        List<Node> expectedNodes = getNodes("a", "b", "c");


        DB.execute(cypher).accept(row -> {
            Path path = (Path) row.get("path");

            List<Node> actualNodes = StreamSupport.stream(path.nodes().spliterator(), false).collect(toList());

            assertEquals(expectedNodes, actualNodes);

            return true;
        });
    }

    @Test
    public void shouldReturnPathsWithCosts() throws Exception {
        final String cypher = "CALL algo.asPath([0,1,2], [0.1,0.2], {cumulativeWeights: false})";

        List<Node> expectedNodes = getNodes("a", "b", "c");
        List<Double> expectedCosts = Arrays.asList(0.1, 0.2);

        DB.execute(cypher).accept(row -> {
            Path path = (Path) row.get("path");

            List<Node> actualNodes = StreamSupport.stream(path.nodes().spliterator(), false).collect(toList());
            List<Double> actualCosts = StreamSupport.stream(path.relationships().spliterator(), false)
                    .map(rel -> (double)rel.getProperty("cost")).collect(toList());

            assertEquals(expectedNodes, actualNodes);
            assertEquals(expectedCosts, actualCosts);

            return true;
        });
    }

    @Rule public ExpectedException exception = ExpectedException.none();

    @Test
    public void shouldThrowExceptionIfNotEnoughCostsProvided() throws Exception {
        final String cypher = "CALL algo.asPath([0,1,2], [0.1], {cumulativeWeights: false})";

        exception.expect(RuntimeException.class);
        exception.expectMessage(CoreMatchers.containsString("'weights' contains 1 values, but 2 values were expected"));

        DB.execute(cypher).close();
    }

    @Test
    public void shouldPreprocessCumulativeWeights() throws Exception {
        final String cypher = "CALL algo.asPath([0,1,2], [0, 40.0, 70.0])";

        List<Node> expectedNodes = getNodes("a", "b", "c");
        List<Double> expectedCosts = Arrays.asList(40.0, 30.0);

        DB.execute(cypher).accept(row -> {
            Path path = (Path) row.get("path");

            List<Node> actualNodes = StreamSupport.stream(path.nodes().spliterator(), false).collect(toList());
            List<Double> actualCosts = StreamSupport.stream(path.relationships().spliterator(), false)
                    .map(rel -> (double)rel.getProperty("cost")).collect(toList());

            assertEquals(expectedNodes, actualNodes);
            assertEquals(expectedCosts, actualCosts);

            return true;
        });
    }

    @Test
    public void shouldThrowExceptionIfNotEnoughCumulativeWeightsProvided() throws Exception {
        final String cypher = "CALL algo.asPath([0,1,2], [0, 40.0])";

        exception.expect(RuntimeException.class);
        exception.expectMessage(CoreMatchers.containsString("'weights' contains 2 values, but 3 values were expected"));

        DB.execute(cypher).close();
    }

    private List<Node> getNodes(String... nodes) {
        List<Node> nodeIds;
        try (Transaction tx = DB.beginTx()) {
            nodeIds = Arrays.stream(nodes)
                    .map(name -> DB.findNode(Label.label("Node"), "name", name))
                    .collect(toList());
        }
        return nodeIds;

    }

}

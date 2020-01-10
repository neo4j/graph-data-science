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
package org.neo4j.graphalgo.impl;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.AlgoTestBase;
import org.neo4j.graphalgo.PropertyMapping;
import org.neo4j.graphalgo.TestDatabaseCreator;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.loading.HugeGraphFactory;
import org.neo4j.graphalgo.impl.shortestpaths.WeightedPath;
import org.neo4j.graphalgo.impl.shortestpaths.YensKShortestPaths;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;

import java.util.List;
import java.util.function.DoubleConsumer;

import static org.mockito.Mockito.mock;

/**
 * Graph:
 *
 *     (b)   (e)
 *   1/ 2\ 1/ 2\
 * >(a)  (d)  (g)
 *   2\ 1/ 2\ 1/
 *    (c)   (f)
 */
public class YensDebugTest extends AlgoTestBase {

    private Graph graph;

    @BeforeEach
    void setupGraph() {
        db = TestDatabaseCreator.createTestDatabase();
        String cypher =
                "CREATE (a:Node {name:'a'})\n" +
                "CREATE (b:Node {name:'b'})\n" +
                "CREATE (c:Node {name:'c'})\n" +
                "CREATE (d:Node {name:'d'})\n" +
                "CREATE (e:Node {name:'e'})\n" +
                "CREATE (f:Node {name:'f'})\n" +
                "CREATE (g:Node {name:'g'})\n" +
                "CREATE" +
                " (a)-[:TYPE {cost:2.0}]->(b),\n" +
                " (a)-[:TYPE {cost:1.0}]->(c),\n" +
                " (b)-[:TYPE {cost:1.0}]->(d),\n" +
                " (c)-[:TYPE {cost:2.0}]->(d),\n" +
                " (d)-[:TYPE {cost:1.0}]->(e),\n" +
                " (d)-[:TYPE {cost:2.0}]->(f),\n" +
                " (e)-[:TYPE {cost:2.0}]->(g),\n" +
                " (f)-[:TYPE {cost:1.0}]->(g)";

        runQuery(cypher);

        graph = new GraphLoader(db)
                .withAnyRelationshipType()
                .withAnyLabel()
                .withRelationshipProperties(PropertyMapping.of("cost", Double.MAX_VALUE))
                .withDirection(Direction.BOTH)
                .load(HugeGraphFactory.class);
    }

    @Test
    void test() {
        YensKShortestPaths yens = new YensKShortestPaths(
            graph,
            getNode("a").getId(),
            getNode("g").getId(),
            Direction.BOTH,
            5,
            4
        ).compute();
        List<WeightedPath> paths = yens.getPaths();
        DoubleConsumer mock = mock(DoubleConsumer.class);
        for (int i = 0; i < paths.size(); i++) {
            final WeightedPath path = paths.get(i);
            mock.accept(path.getCost());
            System.out.println(path + " = "  + path.getCost());
        }
    }

    private Node getNode(String name) {
        final Node[] node = new Node[1];
        runQuery("MATCH (n:Node) WHERE n.name = '" + name + "' RETURN n", row -> node[0] = row.getNode("n"));
        return node[0];
    }
}

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
package org.neo4j.graphalgo.core.loading;

import com.carrotsearch.hppc.LongArrayList;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraphFactory;
import org.neo4j.graphdb.Direction;
import org.neo4j.test.rule.ImpermanentDatabaseRule;

import java.util.Arrays;

import static org.junit.Assert.assertArrayEquals;

public final class UndirectedLoopsTest {

    @Rule
    public ImpermanentDatabaseRule DB = new ImpermanentDatabaseRule();

    private static final String DB_CYPHER = "" +
            "CREATE (a:Label1 {name:\"a\"})\n" +
            "CREATE (b:Label1 {name:\"b\"})\n" +
            "CREATE (c:Label1 {name:\"c\"})\n" +
            "CREATE (d:Label1 {name:\"d\"})\n" +
            "CREATE (e:Label1 {name:\"e\"})\n" +
            "CREATE (f:Label1 {name:\"f\"})\n" +
            "CREATE (g:Label1 {name:\"g\"})\n" +
            "CREATE\n" +

            "  (g)-[:TYPE4 {cost:4}]->(c),\n" +
            "  (b)-[:TYPE5 {cost:4}]->(g),\n" +
            "  (g)-[:ZZZZ {cost:4}]->(g),\n" +
            "  (g)-[:TYPE6 {cost:4}]->(d),\n" +
            "  (b)-[:TYPE6 {cost:4}]->(g),\n" +
            "  (g)-[:TYPE99 {cost:4}]->(g)";

    @Before
    public void setUp() {
        DB.execute(DB_CYPHER).close();
    }

    @Test
    public void undirectedWithMultipleLoopsShouldSucceed() {
        Graph graph = new GraphLoader(DB)
                .withLabel("Foo|Bar")
                .withRelationshipType("Bar|Foo")
                .withRelationshipWeightsFromProperty("cost", Double.MAX_VALUE)
                .withDirection(Direction.OUTGOING)
                .asUndirected(true)
                .load(HeavyGraphFactory.class);

        LongArrayList nodes = new LongArrayList();
        graph.forEachNode(nodeId -> {
            nodes.add(graph.toOriginalNodeId(nodeId));
            return true;
        });

        long[] nodeIds = nodes.toArray();
        Arrays.sort(nodeIds);

        assertArrayEquals(new long[]{0L, 1L, 2L, 3L, 4L, 5L, 6L}, nodeIds);
    }
}

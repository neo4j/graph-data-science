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
package org.neo4j.graphalgo.core.loading;

import com.carrotsearch.hppc.LongArrayList;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.PropertyMapping;
import org.neo4j.graphalgo.TestDatabaseCreator;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphdb.Direction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;

final class UndirectedLoopsTest {

    private GraphDatabaseAPI db;

    private static final String DB_CYPHER =
            "CREATE" +
            "  (a:Label1 {name: 'a'})" +
            ", (b:Label1 {name: 'b'})" +
            ", (c:Label1 {name: 'c'})" +
            ", (d:Label1 {name: 'd'})" +
            ", (e:Label1 {name: 'e'})" +
            ", (f:Label1 {name: 'f'})" +
            ", (g:Label1 {name: 'g'})" +

            ", (g)-[:TYPE4 {cost:4}]->(c)" +
            ", (b)-[:TYPE5 {cost:4}]->(g)" +
            ", (g)-[:ZZZZ {cost:4}]->(g)" +
            ", (g)-[:TYPE6 {cost:4}]->(d)" +
            ", (b)-[:TYPE6 {cost:4}]->(g)" +
            ", (g)-[:TYPE99 {cost:4}]->(g)";

    @BeforeEach
    void setUp() {
        db = TestDatabaseCreator.createTestDatabase();
        db.execute(DB_CYPHER).close();
    }

    @Test
    void undirectedWithMultipleLoopsShouldSucceed() {
        Graph graph = new GraphLoader(db)
                .withLabel("")
                .withRelationshipType("")
                .withRelationshipProperties(PropertyMapping.of("cost", Double.MAX_VALUE))
                .withDirection(Direction.OUTGOING)
                .undirected()
                .load(HugeGraphFactory.class);

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

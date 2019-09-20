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
package org.neo4j.graphalgo.impl;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.graphalgo.TestDatabaseCreator;
import org.neo4j.graphalgo.TestProgressLogger;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.huge.loader.HugeGraphFactory;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphalgo.core.utils.TerminationFlag;
import org.neo4j.graphalgo.impl.infomap.InfoMap;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * --------- Graph 3x2 ---------
 *
 * (b)        (e)
 * /  \       /  \    (x)
 * (a)--(c)---(d)--(f)
 *
 * --------- Graph 4x2 ---------
 *
 * (a)-(b)---(e)-(f)
 * | X |     | X |    (z)
 * (c)-(d)   (g)-(h)
 */
class InfoMapTest {

    private static final String CYPHER_2x4 =
            "CREATE" +
            "  (a:Node {name: 'a'})" +
            ", (c:Node {name: 'c'})" +
            ", (b:Node {name: 'b'})" +
            ", (d:Node {name: 'd'})" +
            ", (e:Node {name: 'e'})" +
            ", (g:Node {name: 'g'})" +
            ", (f:Node {name: 'f'})" +
            ", (h:Node {name: 'h'})" +
            ", (z:Node {name: 'z'})" +

            ", (a)-[:TYPE]->(b)" +
            ", (a)-[:TYPE]->(c)" +
            ", (a)-[:TYPE]->(d)" +
            ", (b)-[:TYPE]->(c)" +
            ", (c)-[:TYPE]->(d)" +
            ", (b)-[:TYPE]->(d)" +

            ", (f)-[:TYPE]->(e)" +
            ", (e)-[:TYPE]->(h)" +
            ", (e)-[:TYPE]->(g)" +
            ", (f)-[:TYPE]->(g)" +
            ", (f)-[:TYPE]->(h)" +
            ", (g)-[:TYPE]->(h)" +
            ", (b)-[:TYPE]->(e)";

    private static final String CYPHER_2x3 =
            "CREATE" +
            "  (a:Node {name: 'a'})" +
            ", (b:Node {name: 'b'})" +
            ", (c:Node {name: 'c'})" +
            ", (d:Node {name: 'd'})" +
            ", (e:Node {name: 'e'})" +
            ", (f:Node {name: 'f'})" +
            ", (x:Node {name: 'x'})" +

            ", (b)-[:TYPE]->(a)" +
            ", (a)-[:TYPE]->(c)" +
            ", (c)-[:TYPE]->(a)" +

            ", (d)-[:TYPE]->(c)" +

            ", (d)-[:TYPE]->(e)" +
            ", (d)-[:TYPE]->(f)" +
            ", (e)-[:TYPE]->(f)";

    static Stream<String> cypherQueries() {
        return Stream.of(CYPHER_2x4, CYPHER_2x3);
    }

    private GraphDatabaseAPI db;

    @BeforeEach
    void setupDb() {
        db = TestDatabaseCreator.createTestDatabase();
    }

    @AfterEach
    void clearDb() {
        db.shutdown();
    }

    @ParameterizedTest
    @MethodSource("cypherQueries")
    void testClustering(String cypher) {
        db.execute(cypher);
        Graph graph = new GraphLoader(db)
                .withAnyRelationshipType()
                .withAnyLabel()
                .withoutNodeProperties()
                .undirected()
                .load(HugeGraphFactory.class);

        // trigger parallel exec on small set size
        InfoMap.MIN_MODS_PARALLEL_EXEC = 2;

        // do it!
        final InfoMap algo = InfoMap.unweighted(
                graph,
                10,
                InfoMap.THRESHOLD,
                InfoMap.TAU,
                Pools.FJ_POOL,
                4, TestProgressLogger.INSTANCE, TerminationFlag.RUNNING_TRUE
        ).compute();

        // should be 3 communities in each graph
        assertEquals(3, algo.getCommunityCount());
    }
}

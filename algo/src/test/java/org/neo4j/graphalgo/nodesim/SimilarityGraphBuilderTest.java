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
package org.neo4j.graphalgo.nodesim;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.AlgoTestBase;
import org.neo4j.graphalgo.Orientation;
import org.neo4j.graphalgo.StoreLoaderBuilder;
import org.neo4j.graphalgo.TestDatabaseCreator;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.concurrency.Pools;
import org.neo4j.graphalgo.core.huge.HugeGraph;
import org.neo4j.graphalgo.core.huge.UnionGraph;
import org.neo4j.graphalgo.core.loading.NativeFactory;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;

import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.graphalgo.TestGraph.Builder.fromGdl;
import static org.neo4j.graphalgo.TestSupport.assertGraphEquals;

class SimilarityGraphBuilderTest extends AlgoTestBase {

    private static final String DB_CYPHER =
        "CREATE" +
        "  (a:Person {name: 'Alice'})" +
        ", (b:Person {name: 'Bob'})" +
        ", (i1:Item {name: 'p1'})" +
        ", (i2:Item {name: 'p2'})" +
        ", (a)-[:LIKES]->(i1)" +
        ", (a)-[:LIKES]->(i2)" +
        ", (b)-[:LIKES]->(i1)" +
        ", (b)-[:LOVES]->(i2)";

    @BeforeEach
    void setup() {
        db = TestDatabaseCreator.createTestDatabase();
        runQuery(DB_CYPHER);
    }

    @Test
    void testConstructionFromHugeGraph() {
        Graph graph = new StoreLoaderBuilder()
            .api(db)
            .loadAnyLabel()
            .loadAnyRelationshipType()
            .globalOrientation(Orientation.NATURAL)
            .build()
            .graph(NativeFactory.class);

        assertEquals(HugeGraph.class, graph.getClass());

        SimilarityGraphBuilder similarityGraphBuilder = new SimilarityGraphBuilder(
            graph,
            Pools.DEFAULT,
            AllocationTracker.EMPTY
        );

        Graph simGraph = similarityGraphBuilder.build(Stream.of(new SimilarityResult(0, 1, 0.42)));

        assertEquals(graph.nodeCount(), simGraph.nodeCount());
        assertEquals(1, simGraph.relationshipCount());

        assertGraphEquals(fromGdl("(a)-[{w: 0.42000D}]->(b), (i1), (i2)"), simGraph);
    }

    @Test
    void testConstructionFromUnionGraph() {
        Graph graph = new StoreLoaderBuilder()
            .api(db)
            .addNodeLabel("Person")
            .addNodeLabel("Item")
            .addRelationshipType("LIKES")
            .addRelationshipType("LOVES")
            .globalOrientation(Orientation.NATURAL)
            .build()
            .graph(NativeFactory.class);

        assertEquals(UnionGraph.class, graph.getClass());

        SimilarityGraphBuilder similarityGraphBuilder = new SimilarityGraphBuilder(
            graph,
            Pools.DEFAULT,
            AllocationTracker.EMPTY
        );

        Graph simGraph = similarityGraphBuilder.build(Stream.of(new SimilarityResult(0, 1, 0.42)));

        assertEquals(graph.nodeCount(), simGraph.nodeCount());
        assertEquals(1, simGraph.relationshipCount());

        assertGraphEquals(fromGdl("(a)-[{w: 0.42000D}]->(b), (i1), (i2)"), simGraph);
    }
}
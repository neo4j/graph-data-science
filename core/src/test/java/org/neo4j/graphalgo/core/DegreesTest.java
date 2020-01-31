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
package org.neo4j.graphalgo.core;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.AlgoTestBase;
import org.neo4j.graphalgo.Projection;
import org.neo4j.graphalgo.RelationshipProjection;
import org.neo4j.graphalgo.StoreLoaderBuilder;
import org.neo4j.graphalgo.TestDatabaseCreator;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.loading.GraphsByRelationshipType;
import org.neo4j.graphalgo.core.loading.HugeGraphFactory;
import org.neo4j.graphdb.Label;

import java.util.Arrays;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.graphalgo.QueryRunner.runInTransaction;

/**
 * A->B; A->C; B->C;
 *
 *
 *     OutD:   InD: BothD:
 * A:     2      0      2
 * B:     1      1      2
 * C:     0      2      2
 */
class DegreesTest extends AlgoTestBase {

    private static final String UNI_DIRECTIONAL =
            "CREATE" +
            "  (a:Node {name:'a'})" +
            ", (b:Node {name:'b'})" +
            ", (c:Node {name:'c'})" + // shuffled
            ", (a)-[:TYPE]->(b)" +
            ", (a)-[:TYPE]->(c)" +
            ", (b)-[:TYPE]->(c)";

    private static final String BI_DIRECTIONAL =
            "CREATE" +
            "  (a:Node {name:'a'})" +
            ", (b:Node {name:'b'})" +
            ", (c:Node {name:'c'})" + // shuffled
            ", (a)-[:TYPE]->(b)" +
            ", (b)-[:TYPE]->(a)" +
            ", (a)-[:TYPE]->(c)" +
            ", (c)-[:TYPE]->(a)" +
            ", (b)-[:TYPE]->(c)" +
            ", (c)-[:TYPE]->(b)";

    @BeforeEach
    void setupGraphDb() {
        db = TestDatabaseCreator.createTestDatabase();
    }

    @AfterEach
    void clearDb() {
        db.shutdown();
    }

    private Graph graph;

    @Test
    void testUnidirectionalNatural() {
        setup(UNI_DIRECTIONAL, Projection.NATURAL);
        assertEquals(2, graph.degree(nodeId("a")));
        assertEquals(1, graph.degree(nodeId("b")));
        assertEquals(0, graph.degree(nodeId("c")));
    }

    @Test
    void testUnidirectionalReverse() {
        setup(UNI_DIRECTIONAL, Projection.REVERSE);
        assertEquals(0, graph.degree(nodeId("a")));
        assertEquals(1, graph.degree(nodeId("b")));
        assertEquals(2, graph.degree(nodeId("c")));
    }

    @Test
    void testUnidirectionalBoth() {
        setup(UNI_DIRECTIONAL, null);
        assertEquals(2, graph.degree(nodeId("a")));
        assertEquals(2, graph.degree(nodeId("b")));
        assertEquals(2, graph.degree(nodeId("c")));
    }

    @Test
    void testBidirectionalNatural() {
        setup(BI_DIRECTIONAL, Projection.NATURAL);
        assertEquals(2, graph.degree(nodeId("a")));
        assertEquals(2, graph.degree(nodeId("b")));
        assertEquals(2, graph.degree(nodeId("c")));
    }

    @Test
    void testBidirectionalReverse() {
        setup(BI_DIRECTIONAL, Projection.REVERSE);
        assertEquals(2, graph.degree(nodeId("a")));
        assertEquals(2, graph.degree(nodeId("b")));
        assertEquals(2, graph.degree(nodeId("c")));
    }

    @Test
    void testBidirectionalBoth() {
        setup(BI_DIRECTIONAL, null);
        assertEquals(4, graph.degree(nodeId("a")));
        assertEquals(4, graph.degree(nodeId("b")));
        assertEquals(4, graph.degree(nodeId("c")));
    }

    @Test
    void testBidirectionalUndirected() {
        setup(BI_DIRECTIONAL, Projection.UNDIRECTED);
        assertEquals(4, graph.degree(nodeId("a")));
        assertEquals(4, graph.degree(nodeId("b")));
        assertEquals(4, graph.degree(nodeId("c")));
    }

    private void setup(String cypher, Projection projection) {
        runQuery(cypher);
        GraphsByRelationshipType graphs = new StoreLoaderBuilder()
            .api(db)
            .putRelationshipProjectionsWithIdentifier(
                "TYPE_OUT",
                RelationshipProjection.empty().withType("TYPE").withProjection(Projection.NATURAL)
            )
            .putRelationshipProjectionsWithIdentifier(
                "TYPE_IN",
                RelationshipProjection.empty().withType("TYPE").withProjection(Projection.REVERSE)
            )
            .putRelationshipProjectionsWithIdentifier(
                "TYPE_UNDIRECTED",
                RelationshipProjection.empty().withType("TYPE").withProjection(Projection.UNDIRECTED)
            )
            .build()
            .graphs(HugeGraphFactory.class);

        if (projection == Projection.NATURAL) {
            graph = graphs.getGraphProjection("TYPE_OUT");
        } else if (projection == Projection.REVERSE) {
            graph = graphs.getGraphProjection("TYPE_IN");
        } else if (projection == Projection.UNDIRECTED) {
            graph = graphs.getGraphProjection("TYPE_UNDIRECTED");
        } else if (projection == null) { // BOTH case
            graph = graphs.getGraphProjection(Arrays.asList("TYPE_OUT", "TYPE_IN"), Optional.empty());
        }
    }

    private long nodeId(String name) {
        return runInTransaction(db, () -> graph.toMappedNodeId(db.findNodes(Label.label("Node"), "name", name).next().getId()));
    }
}

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

package org.neo4j.graphalgo;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.loading.GraphLoadFactory;
import org.neo4j.graphalgo.impl.jaccard.SimilarityResult;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.internal.kernel.api.exceptions.KernelException;

import java.util.Collection;
import java.util.HashSet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.graphalgo.TestGraph.Builder.fromGdl;
import static org.neo4j.graphalgo.TestSupport.AllGraphNamesTest;
import static org.neo4j.graphalgo.TestSupport.assertGraphEquals;

class NeighborhoodSimilarityProcTest extends ProcTestBase {

    private static final String DB_CYPHER =
            "CREATE" +
            "  (a:Person {id: 0, name: 'Alice'})" +
            ", (b:Person {id: 1, name: 'Bob'})" +
            ", (c:Person {id: 2, name: 'Charlie'})" +
            ", (d:Person {id: 3, name: 'Dave'})" +
            ", (i1:Item {name: 'p1'})" +
            ", (i2:Item {name: 'p2'})" +
            ", (i3:Item {name: 'p3'})" +
            ", (i4:Item {name: 'p4'})" +
            ", (a)-[:LIKES]->(i1)" +
            ", (a)-[:LIKES]->(i2)" +
            ", (a)-[:LIKES]->(i3)" +
            ", (b)-[:LIKES]->(i1)" +
            ", (b)-[:LIKES]->(i2)" +
            ", (c)-[:LIKES]->(i3)";

    private static final Collection<SimilarityResult> EXPECTED_OUTGOING = new HashSet<>();
    private static final Collection<SimilarityResult> EXPECTED_INCOMING = new HashSet<>();

    static {
        EXPECTED_OUTGOING.add(new SimilarityResult(0, 1, 2 / 3.0));
        EXPECTED_OUTGOING.add(new SimilarityResult(0, 2, 1 / 3.0));
        EXPECTED_OUTGOING.add(new SimilarityResult(1, 2, 0.0));

        EXPECTED_INCOMING.add(new SimilarityResult(4, 5, 3.0 / 3.0));
        EXPECTED_INCOMING.add(new SimilarityResult(4, 6, 1 / 3.0));
        EXPECTED_INCOMING.add(new SimilarityResult(5, 6, 1 / 3.0));
    }

    @BeforeEach
    void setup() throws KernelException {
        db = TestDatabaseCreator.createTestDatabase();
        db.execute(DB_CYPHER);
        registerProcedures(db, NeighborhoodSimilarityProc.class);
    }

    @AfterEach
    void teardown() {
        db.shutdown();
    }

    @AllGraphNamesTest
    void shouldStreamResults(String graphImpl) {
        String query = "CALL algo.neighborhoodSimilarity.stream(" +
                       "    '', 'LIKES', {" +
                       "        graph: $graph, direction: 'OUTGOING'" +
                       "    }" +
                       ") YIELD node1, node2, similarity";

        Collection<SimilarityResult> result = new HashSet<>();
        runQuery(query, db, MapUtil.map("graph", graphImpl),
                row -> {
                    long node1 = row.getNumber("node1").longValue();
                    long node2 = row.getNumber("node2").longValue();
                    double similarity = row.getNumber("similarity").doubleValue();
                    result.add(new SimilarityResult(node1, node2, similarity));
                });

        assertEquals(EXPECTED_OUTGOING, result);
    }

    @AllGraphNamesTest
    void shouldWriteResults(String graphImpl) throws KernelException {
        String query = "CALL algo.neighborhoodSimilarity(" +
                       "    '', 'LIKES', {" +
                       "        graph: $graph, direction: 'OUTGOING'," +
                       "        write: true, writeRelationshipType: 'SIMILAR_TO', writeProperty: 'score'" +
                       "    }" +
                       ") YIELD nodesCompared, relationships, write, writeRelationshipType, writeProperty";

        runQuery(query, db, MapUtil.map("graph", graphImpl),
            row -> {
                assertEquals(3, row.getNumber("nodesCompared").longValue());
                assertEquals(3, row.getNumber("relationships").longValue());
                assertTrue(row.getBoolean("write"));
                assertEquals("SIMILAR_TO", row.getString("writeRelationshipType"));
                assertEquals("score", row.getString("writeProperty"));
            });

        registerProcedures(GraphLoadProc.class);
        String loadQuery = "CALL algo.graph.load('simGraph', 'Person', 'SIMILAR_TO', { nodeProperties: 'id', relationshipProperties: 'score' })";
        runQuery(loadQuery, db, row -> {});
        Graph simGraph = GraphLoadFactory.getUnion("simGraph");
        assertGraphEquals(
            fromGdl(String.format(
                "  (a {id: 0})" +
                ", (b {id: 1})" +
                ", (c {id: 2})" +
                ", (d {id: 3})" +
                ", (a)-[{w: %f}]->(b)" +
                ", (a)-[{w: %f}]->(c)" +
                ", (b)-[{w: %f}]->(c)", 2 / 3.0, 1 / 3.0, 0.0)
            ), simGraph);
    }

    @Test
    void shouldComputeMemrec() {
        String query = "CALL algo.neighborhoodSimilarity.memrec(" +
                       "    '', 'LIKES', {" +
                       "        direction: 'OUTGOING'," +
                       "        write: true, writeRelationshipType: 'SIMILAR_TO', writeProperty: 'score'" +
                       "    }" +
                       ") YIELD treeView";

        System.out.println(db.execute(query).resultAsString());
    }
}

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
package org.neo4j.gds.paths.sourcetarget;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.BaseProcTest;
import org.neo4j.gds.GdsCypher;
import org.neo4j.gds.TestSupport;
import org.neo4j.gds.api.DatabaseId;
import org.neo4j.gds.catalog.GraphProjectProc;
import org.neo4j.gds.core.loading.GraphStoreCatalog;
import org.neo4j.gds.extension.Neo4jGraph;

import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.isA;
import static org.neo4j.gds.TestSupport.assertGraphEquals;
import static org.neo4j.gds.paths.PathTestUtil.WRITE_RELATIONSHIP_TYPE;

class ShortestPathDijkstraMutateProcTest extends BaseProcTest {

    @Neo4jGraph
    private static final String DB_CYPHER =
        "CREATE" +
        "  (a:Label)" +
        ", (b:Label)" +
        ", (c:Label)" +
        ", (d:Label)" +
        ", (e:Label)" +
        ", (f:Label)" +
        ", (a)-[:T{w: 4.0D}]->(b)" +
        ", (a)-[:T{w: 2.0D}]->(c)" +
        ", (b)-[:T{w: 5.0D}]->(c)" +
        ", (b)-[:T{w: 10.0D}]->(d)" +
        ", (c)-[:T{w: 3.0D}]->(e)" +
        ", (d)-[:T{w: 11.0D}]->(f)" +
        ", (e)-[:T{w: 4.0D}]->(d)";

    public String expectedMutatedGraph() {
        return DB_CYPHER + ", (a)-[:PATH {w: 3.0D}]->(f)";
    }

    @BeforeEach
    void setup() throws Exception {
        registerProcedures(
            ShortestPathDijkstraMutateProc.class,
            GraphProjectProc.class
        );

        runQuery(GdsCypher.call("graph")
            .graphProject()
            .withNodeLabel("Label")
            .withRelationshipType("T")
            .withRelationshipProperty("w")
            .yields());
    }

    @Test
    void shouldMutate() {

        var query = GdsCypher.call("graph")
            .algo("gds.shortestPath.dijkstra")
            .mutateMode()
            .addParameter("sourceNode", idFunction.of("a"))
            .addParameter("targetNode", idFunction.of("f"))
            .addParameter("mutateRelationshipType", WRITE_RELATIONSHIP_TYPE)
            .yields();

        assertCypherResult(query, List.of(Map.of(
            "relationshipsWritten", 1L,
            "preProcessingMillis", greaterThan(-1L),
            "computeMillis", greaterThan(-1L),
            "postProcessingMillis", greaterThan(-1L),
            "mutateMillis", greaterThan(-1L),
            "configuration", isA(Map.class)
        )));

        var actualGraph = GraphStoreCatalog.get(getUsername(), DatabaseId.of(db), "graph")
            .graphStore()
            .getUnion();
        var expectedGraph = TestSupport.fromGdl(expectedMutatedGraph());

        assertGraphEquals(expectedGraph, actualGraph);
    }

    @Test
    void testWeightedMutate() {

        var query = GdsCypher.call("graph")
            .algo("gds.shortestPath.dijkstra")
            .mutateMode()
            .addParameter("sourceNode", idFunction.of("a"))
            .addParameter("targetNode", idFunction.of("f"))
            .addParameter("relationshipWeightProperty", "w")
            .addParameter("mutateRelationshipType", WRITE_RELATIONSHIP_TYPE)
            .yields();

        assertCypherResult(query, List.of(Map.of(
            "relationshipsWritten", 1L,
            "preProcessingMillis", greaterThan(-1L),
            "computeMillis", greaterThan(-1L),
            "postProcessingMillis", greaterThan(-1L),
            "mutateMillis", greaterThan(-1L),
            "configuration", isA(Map.class)
        )));

        var actualGraph = GraphStoreCatalog.get(getUsername(), DatabaseId.of(db), "graph")
            .graphStore()
            .getUnion();
        var expected = TestSupport.fromGdl(DB_CYPHER + ", (a)-[:PATH {w: 20.0D}]->(f)");

        assertGraphEquals(expected, actualGraph);
    }
    
}



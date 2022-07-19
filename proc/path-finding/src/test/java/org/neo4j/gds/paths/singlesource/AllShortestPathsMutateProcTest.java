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
package org.neo4j.gds.paths.singlesource;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.GdsCypher;
import org.neo4j.gds.TestSupport;
import org.neo4j.gds.core.Username;
import org.neo4j.gds.core.loading.GraphStoreCatalog;

import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.isA;
import static org.neo4j.gds.TestSupport.assertGraphEquals;

public abstract class AllShortestPathsMutateProcTest extends AllShortestPathsProcTest {
    private static final String EXISTING_GRAPH =
        "CREATE" +
        "  (a:Label)" +
        ", (b:Label)" +
        ", (c:Label)" +
        ", (d:Label)" +
        ", (e:Label)" +
        ", (f:Label)" +
        "  (a)-[:MUTATE {w: 4.0D}]->(b)" +
        ", (a)-[:MUTATE {w: 2.0D}]->(c)" +
        ", (b)-[:MUTATE {w: 5.0D}]->(c)" +
        ", (b)-[:MUTATE {w: 10.0D}]->(d)" +
        ", (c)-[:MUTATE {w: 3.0D}]->(e)" +
        ", (d)-[:MUTATE {w: 11.0D}]->(f)" +
        ", (e)-[:MUTATE {w: 4.0D}]->(d)";

    @Test
    void testMutate() {
        var query = GdsCypher.call("graph")
            .algo(getProcedureName())
            .mutateMode()
            .addParameter("sourceNode", idFunction.of("a"))
            .addParameter("mutateRelationshipType", "MUTATE")
            .yields();

        assertCypherResult(query, List.of(Map.of(
            "relationshipsWritten", 6L,
            "preProcessingMillis", greaterThan(-1L),
            "computeMillis", greaterThan(-1L),
            "postProcessingMillis", greaterThan(-1L),
            "mutateMillis", greaterThan(-1L),
            "configuration", isA(Map.class)
        )));

        var actual = GraphStoreCatalog
            .get(Username.EMPTY_USERNAME.username(), db.databaseName(), "graph")
            .graphStore()
            .getUnion();
        var expected = TestSupport.fromGdl(
            EXISTING_GRAPH +
            // new relationship as a result from mutate
            ", (a)-[{w: 0.0D}]->(a)" +
            ", (a)-[{w: 1.0D}]->(c)" +
            ", (a)-[{w: 1.0D}]->(b)" +
            ", (a)-[{w: 2.0D}]->(e)" +
            ", (a)-[{w: 2.0D}]->(d)" +
            ", (a)-[{w: 3.0D}]->(f)"
        );

        assertGraphEquals(expected, actual);
    }

    @Test
    void testWeightedMutate() {
        var query = GdsCypher.call("graph")
            .algo(getProcedureName())
            .mutateMode()
            .addParameter("sourceNode", idFunction.of("a"))
            .addParameter("relationshipWeightProperty", "cost")
            .addParameter("mutateRelationshipType", "MUTATE")
            .yields();

        assertCypherResult(query, List.of(Map.of(
            "relationshipsWritten", 6L,
            "preProcessingMillis", greaterThan(-1L),
            "computeMillis", greaterThan(-1L),
            "postProcessingMillis", greaterThan(-1L),
            "mutateMillis", greaterThan(-1L),
            "configuration", isA(Map.class)
        )));

        var actual = GraphStoreCatalog
            .get(Username.EMPTY_USERNAME.username(), db.databaseName(), "graph")
            .graphStore()
            .getUnion();
        var expected = TestSupport.fromGdl(
            EXISTING_GRAPH +
            // new relationship as a result from mutate
            ", (a)-[{w: 0.0D}]->(a)" +
            ", (a)-[{w: 2.0D}]->(c)" +
            ", (a)-[{w: 4.0D}]->(b)" +
            ", (a)-[{w: 5.0D}]->(e)" +
            ", (a)-[{w: 9.0D}]->(d)" +
            ", (a)-[{w: 20.0D}]->(f)"
        );

        assertGraphEquals(expected, actual);
    }

    @Test
    void testMemoryEstimation() {
        var query = GdsCypher.call("graph")
            .algo(getProcedureName())
            .estimationMode(GdsCypher.ExecutionModes.MUTATE)
            .addParameter("sourceNode", idFunction.of("a"))
            .addParameter("mutateRelationshipType", "MUTATE")
            .yields("bytesMin", "bytesMax", "nodeCount", "relationshipCount");

        assertCypherResult(query, List.of(Map.of(
            "bytesMin", greaterThan(0L),
            "bytesMax", greaterThan(0L),
            "nodeCount", 6L,
            "relationshipCount", 7L
        )));
    }
}

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
package org.neo4j.gds.paths.steiner;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.BaseProcTest;
import org.neo4j.gds.GdsCypher;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.catalog.GraphProjectProc;
import org.neo4j.gds.core.Username;
import org.neo4j.gds.core.loading.GraphStoreCatalog;
import org.neo4j.gds.extension.Neo4jGraph;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.LongAdder;

import static org.assertj.core.api.Assertions.assertThat;

class SteinerTreeMutateProcTest extends BaseProcTest {

    @Neo4jGraph
    static final String DB_CYPHER =
        "CREATE " +
        " (a:Node) " +
        " ,(b:Node) " +
        " ,(c:Node) " +
        " ,(a)-[:TYPE {cost: 5.4}]->(b) ";

    @BeforeEach
    void setup() throws Exception {
        registerProcedures(SteinerTreeMutateProc.class, GraphProjectProc.class);
        var createQuery = GdsCypher.call(DEFAULT_GRAPH_NAME)
            .graphProject()
            .withAnyLabel()
            .withRelationshipType("TYPE")
            .withRelationshipProperty("cost")
            .yields();
        runQuery(createQuery);
    }

    @Test
    void testYields() {

        var sourceNode = idFunction.of("a");
        var terminalNode = idFunction.of("b");

        String query = GdsCypher.call(DEFAULT_GRAPH_NAME)
            .algo("gds.alpha.steinerTree")
            .mutateMode()
            .addParameter("sourceNode", sourceNode)
            .addParameter("targetNodes", List.of(terminalNode))
            .addParameter("relationshipWeightProperty", "cost")
            .addParameter("mutateRelationshipType", "STEINER")
            .addParameter("mutateProperty", "cost")
            .yields();

        runQuery(query, result -> {
            assertThat(result.columns()).containsExactlyInAnyOrder(
                "preProcessingMillis",
                "computeMillis",
                "effectiveNodeCount",
                "effectiveTargetNodesCount",
                "totalWeight",
                "configuration",
                "mutateMillis",
                "relationshipsWritten"
            );
            assertThat(result.hasNext())
                .as("Result should have a single row")
                .isTrue();
            var resultRow = result.next();

            assertThat(resultRow)
                .containsEntry("effectiveNodeCount", 2L)
                .containsEntry("effectiveTargetNodesCount", 1L)
                .containsEntry("totalWeight", 5.4)
                .containsEntry("relationshipsWritten", 1L);

            assertThat(result.hasNext())
                .as("There should be no more result rows")
                .isFalse();

            return false;

        });
    }

    @Test
    void testMutatedGraph() {

        var sourceNode = idFunction.of("a");
        var terminalNode = idFunction.of("b");

        String query = GdsCypher.call(DEFAULT_GRAPH_NAME)
            .algo("gds.alpha.steinerTree")
            .mutateMode()
            .addParameter("sourceNode", sourceNode)
            .addParameter("targetNodes", List.of(terminalNode))
            .addParameter("relationshipWeightProperty", "cost")
            .addParameter("mutateRelationshipType", "STEINER")
            .addParameter("mutateProperty", "cost")
            .yields();

        runQuery(query);

        var mutatedGraph = GraphStoreCatalog
            .get(Username.EMPTY_USERNAME.username(), db.databaseName(), DEFAULT_GRAPH_NAME)
            .graphStore()
            .getGraph(RelationshipType.of("STEINER"), Optional.of("cost"));

        assertThat(mutatedGraph.relationshipCount()).isEqualTo(1L);

        var relationshipCounter = new LongAdder();
        mutatedGraph.forEachRelationship(idFunction.of("a"), -1, (s, t, w) -> {
            assertThat(t).isEqualTo(idFunction.of("b"));
            assertThat(w).isEqualTo(5.4);
            relationshipCounter.increment();
            return true;
        });

        assertThat(relationshipCounter.longValue()).isEqualTo(1L);
    }

}

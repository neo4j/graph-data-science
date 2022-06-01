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
package org.neo4j.gds.similarity.filterednodesim;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.BaseProcTest;
import org.neo4j.gds.GdsCypher;
import org.neo4j.gds.catalog.GraphProjectProc;
import org.neo4j.gds.core.loading.GraphStoreCatalog;

import java.util.List;
import java.util.Map;

class FilteredNodeSimilarityStreamProcTest extends BaseProcTest {

    private static final String DB_CYPHER =
        "CREATE" +
        "  (a:Person)" +
        ", (b:Person)" +
        ", (c:Person)" +
        ", (d:Person)" +
        ", (i1:Item)" +
        ", (i2:Item)" +
        ", (i3:Item)" +
        ", (i4:Item)" +
        ", (a)-[:LIKES]->(i1)" +
        ", (a)-[:LIKES]->(i2)" +
        ", (a)-[:LIKES]->(i3)" +
        ", (b)-[:LIKES]->(i1)" +
        ", (b)-[:LIKES]->(i2)" +
        ", (c)-[:LIKES]->(i3)" +
        ", (d)-[:LIKES]->(i1)" +
        ", (d)-[:LIKES]->(i2)" +
        ", (d)-[:LIKES]->(i3)";

    @BeforeEach
    void setup() throws Exception {
        registerProcedures(
            FilteredNodeSimilarityStreamProc.class,
            GraphProjectProc.class
        );
    }

    @AfterEach
    void teardown() {
        GraphStoreCatalog.removeAllLoadedGraphs();
    }

    @Test
    void shouldWorkWithoutFiltering() {
        runQuery(DB_CYPHER);

        var createQuery = GdsCypher.call("graph")
            .graphProject()
            .withNodeLabels("Person", "Item")
            .withAnyRelationshipType()
            .yields();
        runQuery(createQuery);

        var algoQuery = GdsCypher.call("graph")
            .algo("gds.alpha.nodeSimilarity.filtered")
            .streamMode()
            .yields();
        assertCypherResult(algoQuery, List.of(
            Map.of("node1", 0L, "node2", 3L, "similarity", 1.0),
            Map.of("node1", 0L, "node2", 1L, "similarity", 0.6666666666666666),
            Map.of("node1", 0L, "node2", 2L, "similarity", 0.3333333333333333),
            Map.of("node1", 1L, "node2", 3L, "similarity", 0.6666666666666666),
            Map.of("node1", 1L, "node2", 0L, "similarity", 0.6666666666666666),
            Map.of("node1", 2L, "node2", 3L, "similarity", 0.3333333333333333),
            Map.of("node1", 2L, "node2", 0L, "similarity", 0.3333333333333333),
            Map.of("node1", 3L, "node2", 0L, "similarity", 1.0),
            Map.of("node1", 3L, "node2", 1L, "similarity", 0.6666666666666666),
            Map.of("node1", 3L, "node2", 2L, "similarity", 0.3333333333333333)
        ));
    }

    @Test
    void shouldWorkWithFiltering() {
        runQuery(DB_CYPHER);

        var createQuery = GdsCypher.call("graph")
            .graphProject()
            .withNodeLabels("Person", "Item")
            .withAnyRelationshipType()
            .yields();
        runQuery(createQuery);

        var sourceFilteredAlgoQuery = GdsCypher.call("graph")
            .algo("gds.alpha.nodeSimilarity.filtered")
            .streamMode()
            .addParameter("sourceNodeFilter", List.of(0, 3))
            .yields();
        assertCypherResult(sourceFilteredAlgoQuery, List.of(
            Map.of("node1", 0L, "node2", 3L, "similarity", 1.0),
            Map.of("node1", 0L, "node2", 1L, "similarity", 0.6666666666666666),
            Map.of("node1", 0L, "node2", 2L, "similarity", 0.3333333333333333),
            Map.of("node1", 3L, "node2", 0L, "similarity", 1.0),
            Map.of("node1", 3L, "node2", 1L, "similarity", 0.6666666666666666),
            Map.of("node1", 3L, "node2", 2L, "similarity", 0.3333333333333333)
        ));

        var targetFilteredAlgoQuery = GdsCypher.call("graph")
            .algo("gds.alpha.nodeSimilarity.filtered")
            .streamMode()
            .addParameter("targetNodeFilter", List.of(0, 3))
            .yields();
        assertCypherResult(targetFilteredAlgoQuery, List.of(
            Map.of("node1", 0L, "node2", 3L, "similarity", 1.0),
            Map.of("node1", 1L, "node2", 3L, "similarity", 0.6666666666666666),
            Map.of("node1", 1L, "node2", 0L, "similarity", 0.6666666666666666),
            Map.of("node1", 2L, "node2", 3L, "similarity", 0.3333333333333333),
            Map.of("node1", 2L, "node2", 0L, "similarity", 0.3333333333333333),
            Map.of("node1", 3L, "node2", 0L, "similarity", 1.0)
        ));
    }
}

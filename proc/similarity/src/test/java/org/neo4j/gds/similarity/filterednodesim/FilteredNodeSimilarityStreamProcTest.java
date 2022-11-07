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
import org.neo4j.gds.extension.IdFunction;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.extension.Neo4jGraph;

import java.util.List;
import java.util.Map;

class FilteredNodeSimilarityStreamProcTest extends BaseProcTest {

    @Neo4jGraph(offsetIds = true)
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

    @Inject
    IdFunction idFunction;

    @BeforeEach
    void setup() throws Exception {
        registerProcedures(
            FilteredNodeSimilarityStreamProc.class,
            GraphProjectProc.class
        );

        var createQuery = GdsCypher.call("graph")
            .graphProject()
            .withNodeLabels("Person", "Item")
            .withAnyRelationshipType()
            .yields();
        runQuery(createQuery);
    }

    @AfterEach
    void teardown() {
        GraphStoreCatalog.removeAllLoadedGraphs();
    }

    @Test
    void shouldWorkWithoutFiltering() {
        var algoQuery = GdsCypher.call("graph")
            .algo("gds.alpha.nodeSimilarity.filtered")
            .streamMode()
            .yields();

        var a = idFunction.of("a");
        var b = idFunction.of("b");
        var c = idFunction.of("c");
        var d = idFunction.of("d");

        assertCypherResult(algoQuery, List.of(
            Map.of("node1", a, "node2", d, "similarity", 1.0),
            Map.of("node1", a, "node2", b, "similarity", 0.6666666666666666),
            Map.of("node1", a, "node2", c, "similarity", 0.3333333333333333),
            Map.of("node1", b, "node2", d, "similarity", 0.6666666666666666),
            Map.of("node1", b, "node2", a, "similarity", 0.6666666666666666),
            Map.of("node1", c, "node2", d, "similarity", 0.3333333333333333),
            Map.of("node1", c, "node2", a, "similarity", 0.3333333333333333),
            Map.of("node1", d, "node2", a, "similarity", 1.0),
            Map.of("node1", d, "node2", b, "similarity", 0.6666666666666666),
            Map.of("node1", d, "node2", c, "similarity", 0.3333333333333333)
        ));
    }

    @Test
    void shouldWorkWithFiltering() {
        var a = idFunction.of("a");
        var b = idFunction.of("b");
        var c = idFunction.of("c");
        var d = idFunction.of("d");

        var sourceFilteredAlgoQuery = GdsCypher.call("graph")
            .algo("gds.alpha.nodeSimilarity.filtered")
            .streamMode()
            .addParameter("sourceNodeFilter", List.of(a, d))
            .yields();
        assertCypherResult(sourceFilteredAlgoQuery, List.of(
            Map.of("node1", a, "node2", d, "similarity", 1.0),
            Map.of("node1", a, "node2", b, "similarity", 0.6666666666666666),
            Map.of("node1", a, "node2", c, "similarity", 0.3333333333333333),
            Map.of("node1", d, "node2", a, "similarity", 1.0),
            Map.of("node1", d, "node2", b, "similarity", 0.6666666666666666),
            Map.of("node1", d, "node2", c, "similarity", 0.3333333333333333)
        ));

        var targetFilteredAlgoQuery = GdsCypher.call("graph")
            .algo("gds.alpha.nodeSimilarity.filtered")
            .streamMode()
            .addParameter("targetNodeFilter", List.of(a, d))
            .yields();
        assertCypherResult(targetFilteredAlgoQuery, List.of(
            Map.of("node1", a, "node2", d, "similarity", 1.0),
            Map.of("node1", b, "node2", d, "similarity", 0.6666666666666666),
            Map.of("node1", b, "node2", a, "similarity", 0.6666666666666666),
            Map.of("node1", c, "node2", d, "similarity", 0.3333333333333333),
            Map.of("node1", c, "node2", a, "similarity", 0.3333333333333333),
            Map.of("node1", d, "node2", a, "similarity", 1.0)
        ));
    }
}

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
package org.neo4j.gds.paths.maxflow;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.BaseProcTest;
import org.neo4j.gds.GdsCypher;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.catalog.GraphProjectProc;
import org.neo4j.gds.core.Username;
import org.neo4j.gds.core.loading.GraphStoreCatalog;
import org.neo4j.gds.extension.IdFunction;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.extension.Neo4jGraph;

import java.util.Optional;
import java.util.concurrent.atomic.LongAdder;

import static org.assertj.core.api.Assertions.assertThat;

class MaxFlowMutateProcTest extends BaseProcTest {

    @Neo4jGraph
    static final String DB_CYPHER = """
        CREATE
            (a:Node {id: 0}),
            (b:Node {id: 1}),
            (c:Node {id: 2}),
            (d:Node {id: 3}),
            (e:Node {id: 4}),
            (a)-[:R {w: 4.0}]->(d),
            (b)-[:R {w: 3.0}]->(a),
            (c)-[:R {w: 2.0}]->(a),
            (c)-[:R {w: 0.0}]->(b),
            (d)-[:R {w: 5.0}]->(e)
        """;

    @Inject
    private IdFunction idFunction;

    @BeforeEach
    void setup() throws Exception {
        registerProcedures(MaxFlowMutateProc.class, GraphProjectProc.class);
        var createQuery = GdsCypher.call(DEFAULT_GRAPH_NAME)
            .graphProject()
            .withAnyLabel()
            .withRelationshipProperty("w")
            .yields();
        runQuery(createQuery);
    }

    @Test
    void testMutate() {
        String query = GdsCypher.call(DEFAULT_GRAPH_NAME)
            .algo("gds.maxFlow")
            .mutateMode()
            .addParameter("sourceNodes", idFunction.of("a"))
            .addParameter("capacityProperty", "w")
            .addParameter("targetNodes", idFunction.of("e"))
            .addParameter("mutateRelationshipType", "MAX_FLOW")
            .addParameter("mutateProperty", "flow")
            .yields("totalFlow", "relationshipsWritten");

        var rowCount = runQueryWithRowConsumer(query,
        resultRow -> {

            assertThat(resultRow.get("totalFlow")).isInstanceOf(Double.class);
            assertThat(resultRow.get("relationshipsWritten")).isInstanceOf(Long.class);

            assertThat((double) resultRow.get("totalFlow")).isEqualTo(4D);
            assertThat((long) resultRow.get("relationshipsWritten")).isEqualTo(2L);
        });
        assertThat(rowCount).isEqualTo(1L);

        var mutatedGraph = GraphStoreCatalog
            .get(Username.EMPTY_USERNAME.username(), db.databaseName(), DEFAULT_GRAPH_NAME)
            .graphStore()
            .getGraph(RelationshipType.of("MAX_FLOW"), Optional.of("flow"));

        assertThat(mutatedGraph.relationshipCount()).isEqualTo(2L);

        var relationshipCounter = new LongAdder();
        mutatedGraph.forEachRelationship(mutatedGraph.toMappedNodeId(idFunction.of("a")), 0, (s, t, w) -> {
            assertThat(t).isEqualTo(mutatedGraph.toMappedNodeId(idFunction.of("d")));
            assertThat(w).isEqualTo(4D);
            relationshipCounter.increment();
            return true;
        });
        mutatedGraph.forEachRelationship(mutatedGraph.toMappedNodeId(idFunction.of("d")), 0, (s, t, w) -> {
            assertThat(t).isEqualTo(mutatedGraph.toMappedNodeId(idFunction.of("e")));
            assertThat(w).isEqualTo(4D);
            relationshipCounter.increment();
            return true;
        });
        assertThat(relationshipCounter.longValue()).isEqualTo(2L);

    }
}

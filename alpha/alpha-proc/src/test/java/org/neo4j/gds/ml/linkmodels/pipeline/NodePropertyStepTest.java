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
package org.neo4j.gds.ml.linkmodels.pipeline;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.BaseProcTest;
import org.neo4j.gds.GdsCypher;
import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.ProcedureRunner;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.catalog.GraphCreateProc;
import org.neo4j.gds.catalog.GraphStreamNodePropertiesProc;
import org.neo4j.gds.extension.Neo4jGraph;
import org.neo4j.gds.louvain.LouvainStreamProc;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;

class NodePropertyStepTest extends BaseProcTest {

    @Neo4jGraph
    private static final String GRAPH =
        "CREATE" +
        "  (a:Node)" +
        ", (b:Node)" +
        ", (c:Node)" +
        ", (a)-[:R]->(b)" +
        ", (b)-[:R]->(c)" +
        ", (c)-[:R]->(a)";

    @BeforeEach
    void setup() throws Exception {
        registerProcedures(
            GraphCreateProc.class,
            GraphStreamNodePropertiesProc.class
        );
        String createQuery = GdsCypher.call()
            .loadEverything()
            .graphCreate("g")
            .yields();

        runQuery(createQuery);
    }

    @Test
    void testInvokeProc() {
        var step = NodePropertyStep.of("pageRank", Map.of("mutateProperty", "foo"));
        ProcedureRunner.applyOnProcedure(db, LouvainStreamProc.class, proc -> {
            step.execute(proc, "g", List.of(NodeLabel.ALL_NODES), List.of(RelationshipType.ALL_RELATIONSHIPS));
        });
        String streamQuery = "CALL gds.graph.streamNodeProperties('g', ['foo'])";
        runQueryWithResultConsumer(streamQuery, result -> {
            for (int i = 0; i < 3; i++) {
                assertThat(result.next().get("propertyValue")).isEqualTo(0.9612404689154856);
            }
            assertFalse(result.hasNext());
        });
    }
}

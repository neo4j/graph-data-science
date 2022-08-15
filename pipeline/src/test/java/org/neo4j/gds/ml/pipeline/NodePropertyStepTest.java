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
package org.neo4j.gds.ml.pipeline;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.BaseProcTest;
import org.neo4j.gds.ElementProjection;
import org.neo4j.gds.GdsCypher;
import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.TestProcedureRunner;
import org.neo4j.gds.catalog.GraphProjectProc;
import org.neo4j.gds.catalog.GraphStreamNodePropertiesProc;
import org.neo4j.gds.core.GraphDimensions;
import org.neo4j.gds.core.loading.GraphStoreCatalog;
import org.neo4j.gds.core.model.OpenModelCatalog;
import org.neo4j.gds.executor.GdsCallableFinder;
import org.neo4j.gds.extension.Neo4jGraph;
import org.neo4j.gds.test.TestProc;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class NodePropertyStepTest extends BaseProcTest {
    private static final String GRAPH_NAME = "g";
    private static final String PROPERTY_NAME = "foo";

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
            GraphProjectProc.class,
            GraphStreamNodePropertiesProc.class
        );
        String createQuery = GdsCypher.call(GRAPH_NAME)
            .graphProject()
            .loadEverything()
            .yields();

        runQuery(createQuery);
    }

    @AfterEach
    void teardown() {
        GraphStoreCatalog.removeAllLoadedGraphs();
    }

    @Test
    void testInvokeProc() {
        var gdsCallableDefinition = GdsCallableFinder
            .findByName("gds.testProc.mutate", List.of())
            .get();

        var step = new NodePropertyStep(gdsCallableDefinition, Map.of("mutateProperty", PROPERTY_NAME));
        TestProcedureRunner.applyOnProcedure(
            db,
            TestProc.class,
            proc -> step.execute(
                proc.executionContext(),
                GRAPH_NAME,
                List.of(NodeLabel.ALL_NODES),
                List.of(RelationshipType.ALL_RELATIONSHIPS)
            )
        );

        var graphStore = GraphStoreCatalog.get("", db.databaseId(), GRAPH_NAME).graphStore();

        graphStore
            .nodeLabels()
            .forEach(label -> assertThat(graphStore.nodePropertyKeys(label)).containsExactly(PROPERTY_NAME));
    }

    @Test
    void testEstimate() {
        var gdsCallableDefinition = GdsCallableFinder
            .findByName("gds.testProc.mutate", List.of())
            .orElseThrow();

        var step = new NodePropertyStep(
            gdsCallableDefinition,
            Map.of("mutateProperty", PROPERTY_NAME, "throwOnEstimate", true)
        );

        // verify exception is caught
        assertThat(step
            .estimate(new OpenModelCatalog(), "myUser", List.of(ElementProjection.PROJECT_ALL), List.of(ElementProjection.PROJECT_ALL))
            .estimate(GraphDimensions.of(1), 4)
            .memoryUsage().max).isZero();
    }
}

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
package org.neo4j.gds.pipeline;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.BaseTest;
import org.neo4j.gds.GdsCypher;
import org.neo4j.gds.NodeProjections;
import org.neo4j.gds.RelationshipProjections;
import org.neo4j.gds.catalog.GraphCreateProc;
import org.neo4j.gds.compat.GraphDatabaseApiProxy;
import org.neo4j.gds.config.GraphCreateFromStoreConfig;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.test.TestAlgorithm;
import org.neo4j.gds.test.TestMutateConfig;
import org.neo4j.gds.test.TestMutateSpec;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MemoryEstimationExecutorTest extends BaseTest {


    private MemoryEstimationExecutor<TestAlgorithm, TestAlgorithm, TestMutateConfig> memoryEstimationExecutor;

    @BeforeEach
    void setup() throws Exception {
        GraphDatabaseApiProxy.registerProcedures(db, GraphCreateProc.class);

        memoryEstimationExecutor = new MemoryEstimationExecutor<>(
            new TestMutateSpec(),
            new ProcedurePipelineSpec<>()
        );
    }

    @Test
    void testMemoryEstimate() {
        var graphName = "memoryEstimateGraph";
        runQuery(GdsCypher.call(graphName)
                .graphCreate()
                .loadEverything()
                .yields());

        var estimationResult = memoryEstimationExecutor.computeEstimate(
            graphName,
            Map.of("mutateProperty", "foo")
        );

        estimationResult.forEach(row -> {
            assertThat(row.nodeCount).isEqualTo(0);
            assertThat(row.bytesMin).isGreaterThan(0);
            assertThat(row.bytesMax).isGreaterThanOrEqualTo(row.bytesMin);
            assertThat(row.mapView).isNotNull();
            assertThat(row.treeView).isNotEmpty();
        });
    }

    @Test
    void testMemoryEstimateOnExplicitDimensions() {
        Map<String, Object> graphCreateConfig = CypherMapWrapper.empty()
            .withEntry(GraphCreateFromStoreConfig.NODE_PROJECTION_KEY, NodeProjections.all())
            .withEntry(GraphCreateFromStoreConfig.RELATIONSHIP_PROJECTION_KEY, RelationshipProjections.all())
            .withNumber("nodeCount", 100_000_000L)
            .withNumber("relationshipCount", 20_000_000_000L)
            .withoutEntry("nodeProperties")
            .toMap();

        var estimationResult = memoryEstimationExecutor.computeEstimate(
            graphCreateConfig,
            Map.of("mutateProperty", "foo")
        );

        estimationResult.forEach(row -> {
            assertEquals(100_000_000L, row.nodeCount);
            assertEquals(20_000_000_000L, row.relationshipCount);
            var components = (List<Map<String, Object>>) row.mapView.get("components");
            assertEquals(2, components.size());

            var graphComponent = components.get(0);
            assertEquals("graph", graphComponent.get("name"));
            assertEquals("[21 GiB ... 58 GiB]", graphComponent.get("memoryUsage"));

            assertTrue(row.bytesMin > row.nodeCount + row.relationshipCount);
            assertTrue(row.bytesMax >= row.bytesMin);
            assertNotNull(row.mapView);
            assertFalse(row.treeView.isEmpty());
        });
    }
}

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
package org.neo4j.gds.executor;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.BaseTest;
import org.neo4j.gds.GdsCypher;
import org.neo4j.gds.NodeProjections;
import org.neo4j.gds.RelationshipProjections;
import org.neo4j.gds.api.AlgorithmMetaDataSetter;
import org.neo4j.gds.api.CloseableResourceRegistry;
import org.neo4j.gds.api.DatabaseId;
import org.neo4j.gds.api.NodeLookup;
import org.neo4j.gds.api.TerminationMonitor;
import org.neo4j.gds.catalog.GraphProjectProc;
import org.neo4j.gds.compat.GraphDatabaseApiProxy;
import org.neo4j.gds.compat.Neo4jProxy;
import org.neo4j.gds.config.GraphProjectFromStoreConfig;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.core.loading.GraphStoreCatalog;
import org.neo4j.gds.core.model.ModelCatalog;
import org.neo4j.gds.core.utils.progress.EmptyTaskRegistryFactory;
import org.neo4j.gds.core.utils.warnings.EmptyUserLogRegistryFactory;
import org.neo4j.gds.gdl.GdlGraphs;
import org.neo4j.gds.test.TestAlgorithm;
import org.neo4j.gds.test.TestAlgorithmResult;
import org.neo4j.gds.test.TestMutateConfig;
import org.neo4j.gds.transaction.TransactionContextImpl;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.kernel.api.procs.ProcedureCallContext;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MemoryEstimationExecutorTest extends BaseTest {

    private Transaction procedureTransaction;
    private MemoryEstimationExecutor<TestAlgorithm, TestAlgorithmResult, TestMutateConfig> memoryEstimationExecutor;

    @BeforeEach
    void setup() throws Exception {
        procedureTransaction = db.beginTx();

        GraphDatabaseApiProxy.registerProcedures(db, GraphProjectProc.class);

        var executionContext = ImmutableExecutionContext
            .builder()
            .databaseId(DatabaseId.of(db))
            .dependencyResolver(GraphDatabaseApiProxy.dependencyResolver(db))
            .callContext(new ProcedureCallContext(42, new String[0], false, "neo4j", false))
            .log(Neo4jProxy.testLog())
            .taskRegistryFactory(EmptyTaskRegistryFactory.INSTANCE)
            .userLogRegistryFactory(EmptyUserLogRegistryFactory.INSTANCE)
            .username("")
            .terminationMonitor(TerminationMonitor.EMPTY)
            .closeableResourceRegistry(CloseableResourceRegistry.EMPTY)
            .algorithmMetaDataSetter(AlgorithmMetaDataSetter.EMPTY)
            .nodeLookup(NodeLookup.EMPTY)
            .modelCatalog(ModelCatalog.EMPTY)
            .isGdsAdmin(false)
            .build();

        memoryEstimationExecutor = new MemoryEstimationExecutor<>(
            new TestMutateSpec(),
            new ProcedureExecutorSpec<>(),
            executionContext,
            TransactionContextImpl.of(db, procedureTransaction)
        );
    }

    @AfterEach
    void tearDown() {
        procedureTransaction.close();
    }

    @Test
    void testMemoryEstimate() {
        var graphName = "memoryEstimateGraph";
        GraphStoreCatalog.set(GraphProjectFromStoreConfig.emptyWithName("", graphName), GdlGraphs.EMPTY_GRAPH_STORE);
        runQuery(GdsCypher.call(graphName)
            .graphProject()
            .loadEverything()
            .yields());

        var estimationResult = memoryEstimationExecutor.computeEstimate(
            graphName,
            Map.of("mutateProperty", "foo")
        );

        GraphStoreCatalog.removeAllLoadedGraphs();
        estimationResult.forEach(row -> {
            assertThat(row.nodeCount).isEqualTo(0);
            assertThat(row.bytesMin).isGreaterThan(0);
            assertThat(row.bytesMax).isGreaterThanOrEqualTo(row.bytesMin);
            assertThat(row.mapView).isNotNull();
            assertThat(row.treeView).isNotEmpty();
        });
    }

    @Test
    void failONMemoryEstimationWithInvalidRelationshipFilterOnExplicitGraphStore() {
        var graphName = "memoryEstimateGraph";
        GraphStoreCatalog.set(GraphProjectFromStoreConfig.emptyWithName("", graphName), GdlGraphs.EMPTY_GRAPH_STORE);
        runQuery(GdsCypher.call(graphName)
            .graphProject()
            .loadEverything()
            .yields());

        assertThatThrownBy(() -> memoryEstimationExecutor.computeEstimate(graphName,
            Map.of("mutateProperty", "foo", "relationshipTypes", List.of("INVALID"))
        )).hasMessage(
            "Could not find the specified `relationshipTypes` of ['INVALID']. Available relationship types are ['__ALL__'].");

        GraphStoreCatalog.removeAllLoadedGraphs();
    }

    @Test
    void testMemoryEstimateOnExplicitDimensions() {
        Map<String, Object> graphProjectConfig = CypherMapWrapper.empty()
            .withEntry(GraphProjectFromStoreConfig.NODE_PROJECTION_KEY, NodeProjections.all())
            .withEntry(GraphProjectFromStoreConfig.RELATIONSHIP_PROJECTION_KEY, RelationshipProjections.ALL)
            .withNumber("nodeCount", 100_000_000L)
            .withNumber("relationshipCount", 20_000_000_000L)
            .withoutEntry("nodeProperties")
            .toMap();

        var estimationResult = memoryEstimationExecutor.computeEstimate(
            graphProjectConfig,
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

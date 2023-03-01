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
package org.neo4j.gds.core.write;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.neo4j.gds.BaseTest;
import org.neo4j.gds.StoreLoaderBuilder;
import org.neo4j.gds.TestSupport;
import org.neo4j.gds.api.DefaultValue;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.compat.Neo4jProxy;
import org.neo4j.gds.compat.TestLog;
import org.neo4j.gds.core.Aggregation;
import org.neo4j.gds.core.concurrency.Pools;
import org.neo4j.gds.core.huge.DirectIdMap;
import org.neo4j.gds.core.utils.TerminationFlag;
import org.neo4j.gds.core.utils.progress.EmptyTaskRegistryFactory;
import org.neo4j.gds.core.utils.progress.tasks.TaskProgressTracker;
import org.neo4j.gds.nodeproperties.DoubleTestPropertyValues;
import org.neo4j.gds.nodeproperties.LongTestPropertyValues;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.neo4j.gds.TestSupport.assertGraphEquals;
import static org.neo4j.gds.TestSupport.assertTransactionTermination;
import static org.neo4j.gds.TestSupport.fromGdl;
import static org.neo4j.gds.assertj.Extractors.removingThreadId;

class NativeNodePropertyExporterTest extends BaseTest {

    @BeforeEach
    void setup() {
        runQuery(
            "CREATE " +
            "  (n1:Node {prop1: 1.0, prop2: 42.0})" +
            ", (n2:Node {prop1: 2.0, prop2: 42.0})" +
            ", (n3:Node {prop1: 3.0, prop2: 42.0})" +
            ", (n1)-[:REL]->(n2)" +
            ", (n1)-[:REL]->(n3)" +
            ", (n2)-[:REL]->(n3)" +
            ", (n2)-[:REL]->(n3)"
        );
    }

    @Test
    void exportSingleNodeProperty() {
        Graph graph = new StoreLoaderBuilder().databaseService(db)
            .addNodeProperty("newProp1", "prop1", DefaultValue.of(42.0), Aggregation.NONE)
            .build()
            .graph();

        var exporter = NativeNodePropertyExporter
            .builder(TestSupport.fullAccessTransaction(db), graph, TerminationFlag.RUNNING_TRUE)
            .build();

        int[] intData = {23, 42, 84};
        exporter.write("newProp1",  new LongTestPropertyValues(nodeId -> intData[(int) nodeId]));

        Graph updatedGraph = new StoreLoaderBuilder().databaseService(db)
            .addNodeProperty("prop1", "prop1", DefaultValue.of(42.0), Aggregation.NONE)
            .addNodeProperty("newProp1", "newProp1", DefaultValue.of(42), Aggregation.NONE)
            .build()
            .graph();

        assertGraphEquals(
            fromGdl(
                "(a { prop1: 1.0, newProp1: 23 })" +
                "(b { prop1: 2.0, newProp1: 42 })" +
                "(c { prop1: 3.0, newProp1: 84 })" +
                "(a)-->(b)" +
                "(a)-->(c)" +
                "(b)-->(c)" +
                "(b)-->(c)"),
            updatedGraph
        );
    }

    @Test
    void exportMultipleNodeProperties() {
        Graph graph = new StoreLoaderBuilder().databaseService(db)
            .addNodeProperty("newProp1", "prop1", DefaultValue.of(42.0), Aggregation.NONE)
            .addNodeProperty("newProp2", "prop2", DefaultValue.of(42.0), Aggregation.NONE)
            .build()
            .graph();

        var exporter = NativeNodePropertyExporter
            .builder(TestSupport.fullAccessTransaction(db), graph, TerminationFlag.RUNNING_TRUE)
            .build();

        int[] intData = {23, 42, 84};
        double[] doubleData = {123D, 142D, 184D};

        List<NodeProperty> nodeProperties = Arrays.asList(
            ImmutableNodeProperty.of("newProp1", new LongTestPropertyValues(nodeId -> intData[(int) nodeId])),
            ImmutableNodeProperty.of("newProp2", new DoubleTestPropertyValues(nodeId -> doubleData[(int) nodeId]))
        );

        exporter.write(nodeProperties);

        Graph updatedGraph = new StoreLoaderBuilder().databaseService(db)
            .addNodeProperty("prop1", "prop1", DefaultValue.of(42.0), Aggregation.NONE)
            .addNodeProperty("newProp1", "newProp1", DefaultValue.of(42), Aggregation.NONE)
            .addNodeProperty("newProp2", "newProp2", DefaultValue.of(42.0), Aggregation.NONE)
            .build()
            .graph();

        assertGraphEquals(
            fromGdl(
                "(a { prop1: 1.0, newProp1: 23, newProp2: 123.0d })" +
                "(b { prop1: 2.0, newProp1: 42, newProp2: 142.0d })" +
                "(c { prop1: 3.0, newProp1: 84, newProp2: 184.0d })" +
                "(a)-->(b)" +
                "(a)-->(c)" +
                "(b)-->(c)" +
                "(b)-->(c)"),
            updatedGraph
        );
    }

    @Test
    void stopsExportingWhenTransactionHasBeenTerminated() {
        transactionTerminationTest(null);
    }

    @Test
    void stopsParallelExportingWhenTransactionHasBeenTerminated() {
        transactionTerminationTest(Pools.DEFAULT);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void progressLogging(boolean parallel) {
        // given a graph of 20 nodes
        runQuery("UNWIND range(1, 20) AS i CREATE (:A)");
        Graph graph = new StoreLoaderBuilder().databaseService(db).addNodeLabel("A").build().graph();

        // with a node exporter
        var log = Neo4jProxy.testLog();
        var writeConcurrency = 4;
        var progressTracker = new TaskProgressTracker(
            NodePropertyExporter.baseTask("AlgoNameGoesHere", graph.nodeCount()),
            log,
            writeConcurrency,
            EmptyTaskRegistryFactory.INSTANCE
        );
        var exporterBuilder = NativeNodePropertyExporter
            .builder(TestSupport.fullAccessTransaction(db), graph, TerminationFlag.RUNNING_TRUE)
            .withProgressTracker(progressTracker);
        if (parallel) {
            exporterBuilder = exporterBuilder.parallel(Pools.DEFAULT, writeConcurrency);
        }
        var exporter = exporterBuilder.build();

        // when writing properties
        exporter.write("newProp1", new LongTestPropertyValues(nodeId -> 1L));

        // then assert messages
        assertThat(log.getMessages(TestLog.INFO))
            .extracting(removingThreadId())
            .containsExactly(
                "AlgoNameGoesHere :: WriteNodeProperties :: Start",
                "AlgoNameGoesHere :: WriteNodeProperties 5%",
                "AlgoNameGoesHere :: WriteNodeProperties 10%",
                "AlgoNameGoesHere :: WriteNodeProperties 15%",
                "AlgoNameGoesHere :: WriteNodeProperties 20%",
                "AlgoNameGoesHere :: WriteNodeProperties 25%",
                "AlgoNameGoesHere :: WriteNodeProperties 30%",
                "AlgoNameGoesHere :: WriteNodeProperties 35%",
                "AlgoNameGoesHere :: WriteNodeProperties 40%",
                "AlgoNameGoesHere :: WriteNodeProperties 45%",
                "AlgoNameGoesHere :: WriteNodeProperties 50%",
                "AlgoNameGoesHere :: WriteNodeProperties 55%",
                "AlgoNameGoesHere :: WriteNodeProperties 60%",
                "AlgoNameGoesHere :: WriteNodeProperties 65%",
                "AlgoNameGoesHere :: WriteNodeProperties 70%",
                "AlgoNameGoesHere :: WriteNodeProperties 75%",
                "AlgoNameGoesHere :: WriteNodeProperties 80%",
                "AlgoNameGoesHere :: WriteNodeProperties 85%",
                "AlgoNameGoesHere :: WriteNodeProperties 90%",
                "AlgoNameGoesHere :: WriteNodeProperties 95%",
                "AlgoNameGoesHere :: WriteNodeProperties 100%",
                "AlgoNameGoesHere :: WriteNodeProperties :: Finished"
            );
    }

    private void transactionTerminationTest(ExecutorService executorService) {
        TerminationFlag terminationFlag = () -> false;
        var exporter = NativeNodePropertyExporter
            .builder(TestSupport.fullAccessTransaction(db), new DirectIdMap(3), terminationFlag)
            .parallel(executorService, 4)
            .build();

        assertTransactionTermination(() -> exporter.write("foo", new DoubleTestPropertyValues(ignore -> 42.0)));

        runQueryWithRowConsumer(db, "MATCH (n) WHERE n.foo IS NOT NULL RETURN COUNT(*) AS count", row -> {
            Number count = row.getNumber("count");
            assertNotNull(count);
            assertEquals(0, count.intValue());
        });
    }
}

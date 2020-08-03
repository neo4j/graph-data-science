/*
 * Copyright (c) 2017-2020 "Neo4j,"
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
package org.neo4j.graphalgo.core.write;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.BaseTest;
import org.neo4j.graphalgo.StoreLoaderBuilder;
import org.neo4j.graphalgo.api.DefaultValue;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.nodeproperties.DoubleNodeProperties;
import org.neo4j.graphalgo.api.nodeproperties.LongNodeProperties;
import org.neo4j.graphalgo.core.Aggregation;
import org.neo4j.graphalgo.core.concurrency.Pools;
import org.neo4j.graphalgo.core.huge.DirectIdMapping;
import org.neo4j.graphalgo.core.utils.TerminationFlag;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.neo4j.graphalgo.TestSupport.assertGraphEquals;
import static org.neo4j.graphalgo.TestSupport.assertTransactionTermination;
import static org.neo4j.graphalgo.TestSupport.fromGdl;

class NodePropertyExporterTest extends BaseTest {

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
        Graph graph = new StoreLoaderBuilder().api(db)
            .addNodeProperty("newProp1", "prop1", DefaultValue.of(42.0), Aggregation.NONE)
            .build()
            .graph();

        NodePropertyExporter exporter = NodePropertyExporter.builder(db, graph, TerminationFlag.RUNNING_TRUE).build();

        int[] intData = {23, 42, 84};
        exporter.write("newProp1",  (LongNodeProperties) (long nodeId) -> intData[(int) nodeId]);

        Graph updatedGraph = new StoreLoaderBuilder().api(db)
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
        Graph graph = new StoreLoaderBuilder().api(db)
            .addNodeProperty("newProp1", "prop1", DefaultValue.of(42.0), Aggregation.NONE)
            .addNodeProperty("newProp2", "prop2", DefaultValue.of(42.0), Aggregation.NONE)
            .build()
            .graph();

        NodePropertyExporter exporter = NodePropertyExporter.builder(db, graph, TerminationFlag.RUNNING_TRUE).build();

        int[] intData = {23, 42, 84};
        double[] doubleData = {123D, 142D, 184D};

        List<NodePropertyExporter.NodeProperty<?>> nodeProperties = Arrays.asList(
            ImmutableNodeProperty.of("newProp1", (LongNodeProperties) (long nodeId) -> intData[(int) nodeId]),
            ImmutableNodeProperty.of("newProp2", (DoubleNodeProperties) (long nodeId) -> doubleData[(int) nodeId])
        );

        exporter.write(nodeProperties);

        Graph updatedGraph = new StoreLoaderBuilder().api(db)
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

    private void transactionTerminationTest(ExecutorService executorService) {
        TerminationFlag terminationFlag = () -> false;
        NodePropertyExporter exporter = NodePropertyExporter.builder(db, new DirectIdMapping(3), terminationFlag)
            .parallel(executorService, 4)
            .build();

        assertTransactionTermination(() -> exporter.write("foo", (DoubleNodeProperties) ignore -> 42.0));

        runQueryWithRowConsumer(db, "MATCH (n) WHERE n.foo IS NOT NULL RETURN COUNT(*) AS count", row -> {
            Number count = row.getNumber("count");
            assertNotNull(count);
            assertEquals(0, count.intValue());
        });
    }
}

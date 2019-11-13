/*
 * Copyright (c) 2017-2019 "Neo4j,"
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

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.core.huge.DirectIdMapping;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphalgo.core.utils.TerminationFlag;
import org.neo4j.graphdb.TransactionTerminatedException;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.TestGraphDatabaseFactory;

import java.io.File;
import java.util.UUID;
import java.util.concurrent.ExecutorService;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class NodeExporterTest {

    private static GraphDatabaseAPI DB;

    @BeforeAll
    static void setup() {
        DB = (GraphDatabaseAPI) new TestGraphDatabaseFactory()
                .newImpermanentDatabaseBuilder(new File(UUID.randomUUID().toString()))
                .newGraphDatabase();
        DB.execute("CREATE " +
                   "(n1:Node1 {prop1: 1})," +
                   "(n2:Node2 {prop2: 2})," +
                   "(n3:Node3 {prop3: 3})" +
                   "CREATE " +
                   "(n1)-[:REL1 {prop1: 1}]->(n2)," +
                   "(n1)-[:REL2 {prop2: 2}]->(n3)," +
                   "(n2)-[:REL1 {prop3: 3, weight: 42}]->(n3)," +
                   "(n2)-[:REL3 {prop4: 4, weight: 1337}]->(n3);");
    }

    @AfterAll
    static void tearDown() {
        if (DB != null) DB.shutdown();
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
        NodeExporter exporter = NodeExporter.of(DB, new DirectIdMapping(3), terminationFlag)
                .parallel(executorService, 4)
                .build();
        TransactionTerminatedException exception = assertThrows(
                TransactionTerminatedException.class,
                () -> exporter.write("foo", 42.0, new DoublePropertyTranslator()));
        assertEquals(Status.Transaction.Terminated, exception.status());

        DB.execute("MATCH (n) WHERE n.foo IS NOT NULL RETURN COUNT(*) AS count").accept(row -> {
            Number count = row.getNumber("count");
            assertNotNull(count);
            assertEquals(0, count.intValue());
            return false;
        });
    }

    static class DoublePropertyTranslator implements PropertyTranslator.OfDouble<Double> {
        @Override
        public double toDouble(final Double data, final long nodeId) {
            return data;
        }
    }
}

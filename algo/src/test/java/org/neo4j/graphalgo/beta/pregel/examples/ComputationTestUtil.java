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
package org.neo4j.graphalgo.beta.pregel.examples;

import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.utils.paged.HugeDoubleArray;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.graphalgo.compat.GraphDatabaseApiProxy.findNode;
import static org.neo4j.graphalgo.compat.GraphDatabaseApiProxy.runInTransaction;

final class ComputationTestUtil {

    private ComputationTestUtil() {}

    static void assertLongValues(
            final GraphDatabaseService db,
            Label nodeLabel,
            String idProperty,
            final Graph graph,
            HugeDoubleArray computedValues,
            final long... values) {
        Map<Long, Long> expectedValues = new HashMap<>();
        runInTransaction(db, tx -> {
            for (int i = 0; i < values.length; i++) {
                expectedValues.put(findNode(db, tx, nodeLabel, idProperty, i).getId(), values[i]);
            }
        });
        expectedValues.forEach((idProp, expectedValue) -> {
            long neoId = graph.toOriginalNodeId(idProp);
            long computedValue = (long) computedValues.get(neoId);
            assertEquals(
                    (long) expectedValue,
                    computedValue,
                    String.format("Node.id = %d should have value %d", idProp, expectedValue));
        });
    }

    static void assertDoubleValues(
            final GraphDatabaseService db,
            Label nodeLabel,
            String idProperty,
            final Graph graph,
            HugeDoubleArray computedValues,
            double delta,
            final double... values) {
        Map<Long, Double> expectedValues = new HashMap<>();
        runInTransaction(db, tx -> {
            for (int i = 0; i < values.length; i++) {
                expectedValues.put(findNode(db, tx, nodeLabel, idProperty, i).getId(), values[i]);
            }
        });

        expectedValues.forEach((idProp, expectedValue) -> {
            long neoId = graph.toOriginalNodeId(idProp);
            double computedValue = computedValues.get(neoId);
            assertEquals(
                    expectedValue,
                    computedValue,
                    delta,
                    String.format("Node.id = %d should have value %f", idProp, expectedValue));
        });
    }
}

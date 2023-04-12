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
import org.neo4j.gds.BaseTest;
import org.neo4j.gds.Orientation;
import org.neo4j.gds.RelationshipProjection;
import org.neo4j.gds.StoreLoaderBuilder;
import org.neo4j.gds.TestSupport;
import org.neo4j.gds.api.DefaultValue;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.core.Aggregation;
import org.neo4j.gds.core.concurrency.Pools;
import org.neo4j.gds.core.utils.TerminationFlag;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;

import java.util.List;
import java.util.Map;

class NativeRelationshipPropertiesExporterTest  extends BaseTest {

    private static final String DB_CYPHER =
        "CREATE " +
        "  (p:Person), " +
        "  (p1:Person), " +
        "  (p2:Person), " +
        "  (p3:Person), " +
        "  (p)-[:PAYS { amount: 1.0}]->(p1), " + //
        "  (p)-[:PAYS { amount: 2.0}]->(p1), " + //
        "  (p)-[:PAYS { amount: 3.0}]->(p1), " + // These become all one relationship in the in-memory graph
        "  (p)-[:PAYS { amount: 4.0}]->(p1), " + //
        "  (p)-[:PAYS { amount: 5.0}]->(p1), " + //
        "  (p)-[:PAYS { amount: 6.0}]->(p1), " + //

        "  (p2)-[:PAYS { amount: 3.0}]->(p1), " +
        "  (p2)-[:PAYS { amount: 4.0}]->(p1), " +
        "  (p3)-[:PAYS { amount: 5.0}]->(p2), " +
        "  (p3)-[:PAYS { amount: 6.0}]->(p2)";

    @BeforeEach
    void setup() {
        runQuery(DB_CYPHER);
    }

    @Test
    void shouldWriteRelationshipsWithMultipleProperties() {
        GraphStore graphStore = new StoreLoaderBuilder().databaseService(db)
            .putRelationshipProjectionsWithIdentifier(
                "PAID",
                RelationshipProjection.of("PAYS", Orientation.NATURAL)
            )
            .addRelationshipProperty("totalAmount", "amount", DefaultValue.of(0), Aggregation.SUM)
            .addRelationshipProperty("numberOfPayments", "amount", DefaultValue.of(0), Aggregation.COUNT)
            .build()
            .graphStore();

        var exporter = new NativeRelationshipPropertiesExporter(
            TestSupport.fullAccessTransaction(db),
            graphStore,
            ProgressTracker.NULL_TRACKER,
            Pools.DEFAULT_SINGLE_THREAD_POOL,
            TerminationFlag.RUNNING_TRUE
        );

        exporter.write("PAID", List.of("totalAmount", "numberOfPayments"));

        assertCypherResult(
            "MATCH ()-[r:PAID]->() RETURN r.totalAmount AS totalAmount, r.numberOfPayments AS numberOfPayments",
            List.of(
                Map.of("totalAmount", 21d, "numberOfPayments", 6d),
                Map.of("totalAmount", 7d, "numberOfPayments", 2d),
                Map.of("totalAmount", 11d, "numberOfPayments", 2d)
            )
        );
    }

}

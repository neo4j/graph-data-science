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
package org.neo4j.gds.paths.singlesource.bellmanford;

import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.neo4j.gds.BaseProcTest;
import org.neo4j.gds.catalog.GraphProjectProc;
import org.neo4j.gds.extension.IdFunction;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.extension.Neo4jGraph;

import java.util.Map;
import java.util.concurrent.atomic.LongAdder;

import static org.assertj.core.api.InstanceOfAssertFactories.DOUBLE;
import static org.assertj.core.api.InstanceOfAssertFactories.LONG;

class BellmanFordWriteProcTest extends BaseProcTest {

    @Inject
    public IdFunction idFunction;

    @BeforeEach
    void setUp() throws Exception {
        registerProcedures(
            GraphProjectProc.class,
            BellmanFordWriteProc.class
        );

        var projectQuery="CALL gds.graph.project('graph', '*' , { R : { properties :'weight' }})";
        runQuery(projectQuery);
    }

    @Nested
    @ExtendWith(SoftAssertionsExtension.class)
    class WithNegativeCycle extends BaseProcTest {
        @Neo4jGraph(offsetIds = true)
        private static final String NEGATIVE_CYCLE_DB_CYPHER =
            "CREATE " +
            "  (a0:Node {id: 0})," +
            "  (a1:Node {id: 1})," +
            "  (a2:Node {id: 2})," +
            "  (a3:Node {id: 3})," +
            "  (a4:Node {id: 4})," +
            "  (a0)-[:R {weight: 1.0}]->(a1)," +
            "  (a0)-[:R {weight: 10.0}]->(a2), " +
            "  (a2)-[:R {weight: -8.0}]->(a3), " +
            "  (a3)-[:R {weight: -4.0}]->(a4), " +
            "  (a4)-[:R {weight: 1.0}]->(a2) ";


        @Test
        void shouldNotWriteBackToNeo4j(SoftAssertions assertions) {
            var rowCounter = new LongAdder();
            runQueryWithRowConsumer(
                " MATCH (n) WHERE  n.id = 0 " +
                " CALL gds.bellmanFord.write('graph', {sourceNode: n, relationshipWeightProperty: 'weight', writeRelationshipType: 'BF', writeNegativeCycles: false}) " +
                " YIELD relationshipsWritten, containsNegativeCycle " +
                " RETURN relationshipsWritten, containsNegativeCycle",
                row -> {
                    assertions.assertThat(row.getBoolean("containsNegativeCycle"))
                        .as("This graph should contain a negative cycle.")
                        .isTrue();

                    assertions.assertThat(row.getNumber("relationshipsWritten"))
                        .asInstanceOf(LONG)
                        .as("No relationships should have been written to Neo4j because `writeNegativeCycles` is false.")
                        .isEqualTo(0L);


                    rowCounter.increment();
                });

            assertions.assertThat(rowCounter.longValue())
                .as("There should always be one result row.")
                .isEqualTo(1L);
        }

        @Test
        void shouldWriteBackToNeo4j(SoftAssertions assertions) {
            var rowCount = runQueryWithRowConsumer(
                " MATCH (n) WHERE  n.id = 0 " +
                " CALL gds.bellmanFord.write('graph', {sourceNode: n, relationshipWeightProperty: 'weight', writeRelationshipType: 'BF', writeNegativeCycles: true}) " +
                " YIELD relationshipsWritten, containsNegativeCycle " +
                " RETURN relationshipsWritten, containsNegativeCycle",
                row -> {
                    assertions.assertThat(row.getBoolean("containsNegativeCycle"))
                        .as("This graph should contain a negative cycle.")
                        .isTrue();

                    assertions.assertThat(row.getNumber("relationshipsWritten"))
                        .asInstanceOf(LONG)
                        .as("Incorrect count of the written relationships")
                        .isEqualTo(1L);
                });

            assertions.assertThat(rowCount)
                .as("There should always be one result row.")
                .isEqualTo(1L);

            var matchResultCount = runQueryWithRowConsumer(
                "MATCH (n)-[r:BF]->(m) RETURN id(n) AS sourceNodeId, id(m) AS targetNodeId, r.totalCost AS totalCost",
                row -> {
                    assertions.assertThat(row.getNumber("sourceNodeId"))
                        .asInstanceOf(LONG)
                        .isEqualTo(idFunction.of("a3"));
                    assertions.assertThat(row.getNumber("targetNodeId"))
                        .asInstanceOf(LONG)
                        .isEqualTo(idFunction.of("a3"));
                    assertions.assertThat(row.getNumber("totalCost"))
                        .asInstanceOf(DOUBLE)
                        .as("Incorrect `totalCost`.")
                        .isEqualTo(-11.0);
                }
            );

            assertions.assertThat(matchResultCount)
                .as("Incorrect result count")
                .isEqualTo(1L);
        }
    }

    @Nested
    @ExtendWith(SoftAssertionsExtension.class)
    class WithoutNegativeCycle extends BaseProcTest {

        @Neo4jGraph(offsetIds = true)
        private static final String DB_CYPHER =
            "CREATE " +
            "  (a0:Node {id:0})," +
            "  (a1:Node {id:1})," +
            "  (a2:Node {id:2})," +
            "  (a3:Node {id:3})," +
            "  (a4:Node {id:4})," +
            "  (a0)-[:R {weight: 1.0}]->(a1)," +
            "  (a0)-[:R {weight: -1.0}]->(a2)," +
            "  (a0)-[:R {weight: 10.0}]->(a3), " +
            "  (a3)-[:R {weight: -8.0}]->(a4), " +
            "  (a1)-[:R {weight: 3.0}]->(a4) ";

        @Test
        void shouldWriteBackToNeo4j(SoftAssertions assertions) {
            var rowCounter = new LongAdder();
            runQueryWithRowConsumer(
                " MATCH (n) WHERE  n.id = 0 " +
                " CALL gds.bellmanFord.write('graph', {sourceNode: n, relationshipWeightProperty: 'weight', writeRelationshipType: 'BF'}) " +
                " YIELD relationshipsWritten, containsNegativeCycle " +
                " RETURN relationshipsWritten, containsNegativeCycle",
                row -> {
                    assertions.assertThat(row.getBoolean("containsNegativeCycle"))
                        .as("This graph should not contain a negative cycle.")
                        .isFalse();

                    assertions.assertThat(row.getNumber("relationshipsWritten"))
                        .asInstanceOf(LONG)
                        .as("There should be five relationships written")
                        .isEqualTo(5L);

                    rowCounter.increment();
                });



            assertions.assertThat(rowCounter.longValue())
                .as("There should always be one result row.")
                .isEqualTo(1L);

            var EXPECTED_COST = Map.of(
                idFunction.of("a0"), 0d,
                idFunction.of("a1"), 1d,
                idFunction.of("a2"), -1d,
                idFunction.of("a3"), 10d,
                idFunction.of("a4"), 2d
            );

            var matchResultCount = runQueryWithRowConsumer(
                "MATCH (n)-[r:BF]->(m) RETURN id(n) AS sourceNodeId, id(m) AS targetNodeId, r.totalCost AS totalCost",
                row -> {
                    assertions.assertThat(row.getNumber("sourceNodeId"))
                        .asInstanceOf(LONG)
                        .as("Source node should always be `a0`")
                        .isEqualTo(idFunction.of("a0"));
                    var targetNodeId = row.getNumber("targetNodeId").longValue();
                    var writtenCost = row.getNumber("totalCost").doubleValue();
                    assertions.assertThat(EXPECTED_COST.get(targetNodeId))
                        .as("The cost between `%s` and `%s` should be `%s`", idFunction.of("a0"), targetNodeId, writtenCost)
                        .isEqualTo(writtenCost);
                }
            );

            assertions.assertThat(matchResultCount)
                .as("Five properties should have been streamed")
                .isEqualTo(5L);
        }
    }

}

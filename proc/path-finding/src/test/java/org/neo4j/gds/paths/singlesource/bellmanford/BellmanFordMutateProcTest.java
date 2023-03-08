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
import org.neo4j.gds.catalog.GraphStreamRelationshipPropertiesProc;
import org.neo4j.gds.extension.IdFunction;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.extension.Neo4jGraph;

import java.util.Map;
import java.util.concurrent.atomic.LongAdder;

import static org.assertj.core.api.InstanceOfAssertFactories.DOUBLE;
import static org.assertj.core.api.InstanceOfAssertFactories.LONG;

class BellmanFordMutateProcTest extends BaseProcTest{


    @Inject
    public IdFunction idFunction;

    @BeforeEach
    void setUp() throws Exception {
        registerProcedures(
            GraphProjectProc.class,
            BellmanFordMutateProc.class,
            GraphStreamRelationshipPropertiesProc.class
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
        void shouldNotMutateTheInMemoryGraph(SoftAssertions assertions) {
            var rowCounter = new LongAdder();
            runQueryWithRowConsumer(
                " MATCH (n) WHERE  n.id = 0 " +
                " CALL gds.bellmanFord.mutate('graph', {sourceNode: n, relationshipWeightProperty: 'weight', mutateRelationshipType: 'BF', mutateNegativeCycles: false}) " + // `mutateNegativeCycles` is `false` by default but we want to be explicit here
                " YIELD relationshipsWritten, containsNegativeCycle " +
                " RETURN relationshipsWritten, containsNegativeCycle",
                row -> {
                    assertions.assertThat(row.getBoolean("containsNegativeCycle"))
                        .as("This graph should contain a negative cycle.")
                        .isTrue();

                    assertions.assertThat(row.getNumber("relationshipsWritten"))
                        .asInstanceOf(LONG)
                            .as("The in-memory graph should not have been mutated because configuration parameter `mutateNegativeCycles` is `false`.")
                                .isEqualTo(0L);


                    rowCounter.increment();
                });

            assertions.assertThat(rowCounter.longValue())
                .as("There should always be one result row.")
                .isEqualTo(1L);
        }

        @Test
        void shouldMutateTheInMemoryGraph(SoftAssertions assertions) {
            var rowCounter = new LongAdder();
            runQueryWithRowConsumer(
                " MATCH (n) WHERE  n.id = 0 " +
                " CALL gds.bellmanFord.mutate('graph', {sourceNode: n, relationshipWeightProperty: 'weight', mutateRelationshipType: 'BF', mutateNegativeCycles: true}) " +
                " YIELD relationshipsWritten, containsNegativeCycle " +
                " RETURN relationshipsWritten, containsNegativeCycle",
                row -> {
                    assertions.assertThat(row.getBoolean("containsNegativeCycle"))
                        .as("This graph should contain a negative cycle.")
                        .isTrue();

                    assertions.assertThat(row.getNumber("relationshipsWritten"))
                        .asInstanceOf(LONG)
                            .as("The in-memory graph should have been mutated because configuration parameter `mutateNegativeCycles` is `true`.")
                                .isEqualTo(1L);

                    rowCounter.increment();
                });
            assertions.assertThat(rowCounter.longValue())
                .as("There should always be one result row.")
                .isEqualTo(1L);

            var streamPropertyCount = runQueryWithRowConsumer(
                "CALL gds.graph.relationshipProperty.stream(" +
                "    'graph'," +
                "    'totalCost'," +
                "    ['BF']," +
                "    {}" +
                ")",
                row -> {
                    assertions.assertThat(row.getNumber("sourceNodeId"))
                        .asInstanceOf(LONG)
                        .as("Source node should be `a3`")
                        .isEqualTo(idFunction.of("a3"));
                    assertions.assertThat(row.getNumber("targetNodeId"))
                        .asInstanceOf(LONG)
                        .as("Target node should be `a3`")
                        .isEqualTo(idFunction.of("a3"));

                    assertions.assertThat(row.getNumber("propertyValue"))
                        .asInstanceOf(DOUBLE)
                        .as("Incorrect negative cycle cost")
                        .isEqualTo(-11.0);
                }
            );

            assertions.assertThat(streamPropertyCount)
                .as("One property should have been streamed")
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
        void shouldMutateTheInMemoryGraph(SoftAssertions assertions) {
            var rowCount = runQueryWithRowConsumer(
                " MATCH (n) WHERE  n.id = 0 " +
                " CALL gds.bellmanFord.mutate('graph', {sourceNode: n, relationshipWeightProperty: 'weight', mutateRelationshipType: 'BF'}) " +
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
                });

            assertions.assertThat(rowCount)
                .as("There should always be one result row.")
                .isEqualTo(1L);

            var EXPECTED_COST = Map.of(
                idFunction.of("a0"), 0d,
                idFunction.of("a1"), 1d,
                idFunction.of("a2"), -1d,
                idFunction.of("a3"), 10d,
                idFunction.of("a4"), 2d
            );

            var streamPropertyCount = runQueryWithRowConsumer(
                "CALL gds.graph.relationshipProperty.stream(" +
                "    'graph'," +
                "    'totalCost'," +
                "    ['BF']," +
                "    {}" +
                ")",
                row -> {
                    assertions.assertThat(row.getNumber("sourceNodeId"))
                        .asInstanceOf(LONG)
                        .as("Source node should always be `a0`")
                        .isEqualTo(idFunction.of("a0"));
                    var targetNodeId = row.getNumber("targetNodeId").longValue();
                    var mutatedCost = row.getNumber("propertyValue").doubleValue();
                    assertions.assertThat(EXPECTED_COST.get(targetNodeId))
                        .as("The cost between `%s` and `%s` should be `%s`", idFunction.of("a0"), targetNodeId, mutatedCost)
                        .isEqualTo(mutatedCost);
                }
            );

            assertions.assertThat(streamPropertyCount)
                .as("Five properties should have been streamed")
                .isEqualTo(5L);
        }
    }

}

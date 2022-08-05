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
package org.neo4j.gds.catalog;

import org.apache.commons.lang3.function.TriFunction;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.gds.BaseProcTest;
import org.neo4j.gds.GdsCypher;
import org.neo4j.gds.api.DatabaseId;
import org.neo4j.gds.beta.generator.GraphGenerateProc;
import org.neo4j.gds.catalog.GraphStreamRelationshipsProc.TopologyResult;
import org.neo4j.gds.core.loading.GraphStoreCatalog;
import org.neo4j.gds.extension.IdFunction;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.extension.Neo4jGraph;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class GraphStreamRelationshipsProcTest extends BaseProcTest {

    @Neo4jGraph(idOffset = 10)
    static String DB_CYPHER = "CREATE" +
                              "  (a:N), (b:N), (c:N)" +
                              ", (a)-[:REL1]->(b)" +
                              ", (b)-[:REL1]->(c)" +
                              ", (c)-[:REL2]->(b)" +
                              ", (b)-[:REL2]->(a)";

    @Inject
    IdFunction idFunction;

    @BeforeEach
    void setup() throws Exception {
        registerProcedures(GraphProjectProc.class, GraphStreamRelationshipsProc.class, GraphGenerateProc.class);

        runQuery(GdsCypher.call("graph")
            .graphProject()
            .withNodeLabel("N")
            .withRelationshipType("REL1")
            .withRelationshipType("REL2")
            .yields()
        );
    }

    @Test
    void shouldStreamAllRelationships() {
        var actualRelationships = new ArrayList<TopologyResult>();

        runQueryWithRowConsumer("CALL gds.beta.graph.relationships.stream('graph')", row -> actualRelationships.add(
            relationship(
                row.getNumber("sourceNodeId").longValue(),
                row.getNumber("targetNodeId").longValue(),
                row.getString("relationshipType")
            )
        ));

        assertThat(actualRelationships).containsExactlyInAnyOrderElementsOf(
            List.of(
                relationship(idFunction.of("a"), idFunction.of("b"), "REL1"),
                relationship(idFunction.of("b"), idFunction.of("c"), "REL1"),
                relationship(idFunction.of("c"), idFunction.of("b"), "REL2"),
                relationship(idFunction.of("b"), idFunction.of("a"), "REL2")
            )
        );
    }

    static Stream<Arguments> relTypesAndResultContainerFunction() {
        return Stream.of(
            Arguments.of(
                "REL1",
                (TriFunction<Long, Long, String, TopologyResult>) GraphStreamRelationshipsProcTest::relationship
            ),
            Arguments.of(
                "REL2",
                (TriFunction<Long, Long, String, TopologyResult>) GraphStreamRelationshipsProcTest::reverseRelationship
            )
        );
    }

    @ParameterizedTest
    @MethodSource("relTypesAndResultContainerFunction")
    void shouldStreamFilteredRelationships(
        String relType,
        TriFunction<Long, Long, String, TopologyResult> resultContainerFunction
    ) {
        var actualRelationships = new ArrayList<TopologyResult>();
        runQueryWithRowConsumer(
            "CALL gds.beta.graph.relationships.stream('graph', ['" + relType + "'])",
            row -> actualRelationships.add(
                resultContainerFunction.apply(
                    row.getNumber("sourceNodeId").longValue(),
                    row.getNumber("targetNodeId").longValue(),
                    row.getString("relationshipType")
                ))
        );

        assertThat(actualRelationships).containsExactlyInAnyOrderElementsOf(
            List.of(
                relationship(idFunction.of("a"), idFunction.of("b"), relType),
                relationship(idFunction.of("b"), idFunction.of("c"), relType)
            )
        );
    }

    @Test
    void shouldStreamInParallel() {
        runQuery("CALL gds.beta.graph.generate('generatedGraph', 10000, 5)");

        var actualRelationships = new ArrayList<TopologyResult>();
        runQueryWithRowConsumer("CALL gds.beta.graph.relationships.stream('generatedGraph', ['*'], { concurrency: 4 })", row -> actualRelationships.add(
            relationship(
                row.getNumber("sourceNodeId").longValue(),
                row.getNumber("targetNodeId").longValue(),
                row.getString("relationshipType")
            )
        ));

        var expectedRelationships = new ArrayList<TopologyResult>();
        var generatedGraph = GraphStoreCatalog.get("", DatabaseId.of(db), "generatedGraph").graphStore().getUnion();
        generatedGraph.forEachNode(nodeId -> {
            generatedGraph.forEachRelationship(nodeId, (source, target) -> expectedRelationships.add(
                relationship(source, target, "REL")
            ));
            return true;
        });

        assertThat(actualRelationships).containsExactlyInAnyOrderElementsOf(expectedRelationships);
    }

    @Test
    void shouldFailOnNonExistentRelationshipType() {
        assertThatThrownBy(() -> runQuery("CALL gds.beta.graph.relationships.stream('graph', ['NON_EXISTENT'])"))
            .hasRootCauseInstanceOf(IllegalStateException.class)
            .hasMessageContaining("could not find")
            .hasMessageContaining("'NON_EXISTENT'");
    }

    private static TopologyResult relationship(long sourceId, long targetId, String relationshipType) {
        return new TopologyResult(
            sourceId,
            targetId,
            relationshipType
        );
    }

    private static TopologyResult reverseRelationship(long sourceId, long targetId, String relationshipType) {
        return relationship(targetId, sourceId, relationshipType);
    }
}

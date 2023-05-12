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
package org.neo4j.gds.beta.indexInverse;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.BaseProcTest;
import org.neo4j.gds.GdsCypher;
import org.neo4j.gds.RelationshipProjection;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.catalog.GraphProjectProc;
import org.neo4j.gds.core.loading.GraphStoreCatalog;
import org.neo4j.gds.extension.Neo4jGraph;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.hamcrest.Matchers.instanceOf;

class IndexInverseProcTest extends BaseProcTest {

    @Neo4jGraph
    public static final String DB = "CREATE " +
                                    " (a:A) ,(b:B) ,(c:C) " +
                                    ",(a)-[:REL {prop1: 1.0}]->(b)" +
                                    ",(a)-[:REL {prop1: 2.0}]->(c)" +
                                    ",(a)-[:REL2 {prop1: 2.5}]->(c)" +
                                    ",(b)-[:REL {prop1: 3.0}]->(c)";

    @BeforeEach
    void setup() throws Exception {
        registerProcedures(IndexInverseProc.class, GraphProjectProc.class);

        runQuery(GdsCypher.call("graph")
            .graphProject()
            .withAnyLabel()
            .withRelationshipType("REL")
            .withRelationshipType("REL2")
            .withRelationshipType("INDEXED_REL", RelationshipProjection.builder().type("REL").indexInverse(true).build())
            .withRelationshipProperty("prop1")
            .yields()
        );
    }

    @Test
    void indexInverse() {
        String query = "CALL gds.beta.graph.relationships.indexInverse('graph', {relationshipTypes: ['REL', 'REL2']})";

        assertCypherResult(query, List.of(Map.of(
            "inputRelationships", 4L,
            "mutateMillis", instanceOf(Long.class),
            "preProcessingMillis",instanceOf(Long.class),
            "computeMillis",instanceOf(Long.class),
            "postProcessingMillis",instanceOf(Long.class),
            "configuration", instanceOf(Map.class))
        ));

        var gs = GraphStoreCatalog.get(getUsername(), db.databaseName(), "graph");
        assertThat(gs.graphStore().inverseIndexedRelationshipTypes()).contains(RelationshipType.of("REL"), RelationshipType.of("REL2"));
    }

    @Test
    void shouldFailIfRelationshipTypeIsAlreadyIndexed() {
        var query = "CALL gds.beta.graph.relationships.indexInverse('graph', {relationshipTypes: 'INDEXED_REL'})";

        assertThatThrownBy(() -> runQuery(query))
            .hasRootCauseInstanceOf(UnsupportedOperationException.class)
            .hasRootCauseMessage("Inverse index already exists for ['INDEXED_REL'].");
    }

    @Test
    void shouldSupportStarFilterOverMultipleNonStarProjections() {
        runQuery(GdsCypher.call("other_graph")
            .graphProject()
            .withAnyLabel()
            .withRelationshipType("REL")
            .withRelationshipType("REL2")
            .withRelationshipProperty("prop1")
            .yields()
        );

        assertCypherResult(
            "CALL gds.beta.graph.relationships.indexInverse('other_graph', {relationshipTypes: '*'}) YIELD inputRelationships",
            List.of(Map.of("inputRelationships", 4L))
        );

        var gs = GraphStoreCatalog.get(getUsername(), db.databaseName(), "other_graph");
        assertThat(gs.graphStore().inverseIndexedRelationshipTypes()).contains(RelationshipType.of("REL"), RelationshipType.of("REL2"));
    }

    @Test
    void shouldSupportStarFilterOverStarProjection() {
        runQuery(GdsCypher.call("other_graph")
            .graphProject()
            .withAnyLabel()
            .withAnyRelationshipType()
            .withRelationshipProperty("prop1")
            .yields()
        );
        assertCypherResult(
            "CALL gds.beta.graph.relationships.indexInverse('other_graph', {relationshipTypes: '*'}) YIELD inputRelationships",
            List.of(Map.of("inputRelationships", 4L))
        );
        var gs = GraphStoreCatalog.get(getUsername(), db.databaseName(), "other_graph");
        assertThat(gs.graphStore().inverseIndexedRelationshipTypes()).contains(RelationshipType.ALL_RELATIONSHIPS);
    }

    @Test
    void shouldFailIfStarIsAmbiguous() {
        runQuery(GdsCypher.call("other_graph")
            .graphProject()
            .withAnyLabel()
            .withAnyRelationshipType()
            .withRelationshipType("REL2")
            .withRelationshipProperty("prop1")
            .yields()
        );

        assertThatThrownBy(() -> runQuery("CALL gds.beta.graph.relationships.indexInverse('other_graph', {relationshipTypes: '*'})"))
            .hasMessageContaining("The 'relationshipTypes' parameter is ambiguous");
    }

    @Test
    void memoryEstimation() {
        String query = "CALL gds.beta.graph.relationships.indexInverse.estimate('graph', {relationshipTypes: 'REL'})";

        assertCypherResult(query, List.of(Map.of(
            "mapView", instanceOf(Map.class),
            "treeView", instanceOf(String.class),
            "bytesMax", 524640L,
            "heapPercentageMin", 0.1D,
            "nodeCount", 3L,
            "requiredMemory", "512 KiB",
            "bytesMin", 524640L,
            "heapPercentageMax", 0.1D,
            "relationshipCount", 3L
        )));
    }
}

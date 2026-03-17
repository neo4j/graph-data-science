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
package org.neo4j.gds.projection;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.BaseTest;
import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.TestSupport;
import org.neo4j.gds.compat.GraphDatabaseApiProxy;
import org.neo4j.gds.core.GraphDimensions;
import org.neo4j.gds.extension.Neo4jGraph;
import org.neo4j.gds.extension.Neo4jGraphExtension;
import org.neo4j.internal.id.IdGeneratorFactory;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

@Neo4jGraphExtension
class GraphDimensionsReaderTest extends BaseTest {

    @Neo4jGraph
    private static final String DB_QUERY =
        "CREATE " +
        "  (a:Person {name: 'Alice', age: 30})" +
        ", (b:Person {name: 'Bob', age: 25})" +
        ", (c:City {name: 'New York', population: 8000000})" +
        ", (d:City {name: 'London', population: 9000000})" +
        ", (a)-[:KNOWS {since: 2020}]->(b)" +
        ", (a)-[:LIVES_IN]->(c)" +
        ", (b)-[:LIVES_IN]->(d)";

    @Test
    void shouldReadGraphDimensionsWithAllNodesAndRelationships() {
        var transactionContext = TestSupport.fullAccessTransaction(db);
        var idGeneratorFactory = GraphDatabaseApiProxy.resolveDependency(db, IdGeneratorFactory.class);

        var nodeLabelMappings = Map.of(NodeLabel.ALL_NODES, "*");
        var relationshipTypeMappings = Map.of(RelationshipType.ALL_RELATIONSHIPS, "*");
        var nodeProperties = Collections.<String>emptyList();
        var relationshipProperties = Collections.<String>emptyList();

        var reader = new GraphDimensionsReader(
            transactionContext,
            idGeneratorFactory,
            nodeLabelMappings,
            relationshipTypeMappings,
            nodeProperties,
            relationshipProperties
        );

        GraphDimensions dimensions = reader.call();

        assertThat(dimensions.nodeCount()).isEqualTo(4L);
        assertThat(dimensions.relCountUpperBound()).isEqualTo(3L);
        assertThat(dimensions.nodeLabelTokens()).isNotNull();
        assertThat(dimensions.relationshipTypeTokens()).isNotNull();
    }

    @Test
    void shouldReadGraphDimensionsWithSpecificNodeLabels() {
        var transactionContext = TestSupport.fullAccessTransaction(db);
        var idGeneratorFactory = GraphDatabaseApiProxy.resolveDependency(db, IdGeneratorFactory.class);

        var nodeLabelMappings = Map.of(
            NodeLabel.of("Person"), "Person",
            NodeLabel.of("City2"), "City"
        );
        var relationshipTypeMappings = Map.of(RelationshipType.ALL_RELATIONSHIPS, "*");
        var nodeProperties = Collections.<String>emptyList();
        var relationshipProperties = Collections.<String>emptyList();

        var reader = new GraphDimensionsReader(
            transactionContext,
            idGeneratorFactory,
            nodeLabelMappings,
            relationshipTypeMappings,
            nodeProperties,
            relationshipProperties
        );

        GraphDimensions dimensions = reader.call();

        assertThat(dimensions.nodeCount()).isEqualTo(4L);
        assertThat(dimensions.tokenNodeLabelMapping()).isNotNull();
        assertThat(dimensions.availableNodeLabels())
            .containsExactlyInAnyOrder(NodeLabel.of("Person"), NodeLabel.of("City2"));
    }

    @Test
    void shouldReadGraphDimensionsWithSpecificRelationshipTypes() {
        var transactionContext = TestSupport.fullAccessTransaction(db);
        var idGeneratorFactory = GraphDatabaseApiProxy.resolveDependency(db, IdGeneratorFactory.class);

        var nodeLabelMappings = Map.of(NodeLabel.ALL_NODES, "*");
        var relationshipTypeMappings = Map.of(
            RelationshipType.of("KNOWS"), "KNOWS",
            RelationshipType.of("LIVES_IN"), "LIVES_IN"
        );
        var nodeProperties = Collections.<String>emptyList();
        var relationshipProperties = Collections.<String>emptyList();

        var reader = new GraphDimensionsReader(
            transactionContext,
            idGeneratorFactory,
            nodeLabelMappings,
            relationshipTypeMappings,
            nodeProperties,
            relationshipProperties
        );

        GraphDimensions dimensions = reader.call();

        assertThat(dimensions.relCountUpperBound()).isEqualTo(3L);
        assertThat(dimensions.relationshipCounts()).containsKeys(
            RelationshipType.of("KNOWS"),
            RelationshipType.of("LIVES_IN")
        );
        assertThat(dimensions.relationshipCounts().get(RelationshipType.of("KNOWS"))).isEqualTo(1L);
        assertThat(dimensions.relationshipCounts().get(RelationshipType.of("LIVES_IN"))).isEqualTo(2L);
    }

    @Test
    void shouldReadNodePropertyTokens() {
        var transactionContext = TestSupport.fullAccessTransaction(db);
        var idGeneratorFactory = GraphDatabaseApiProxy.resolveDependency(db, IdGeneratorFactory.class);

        var nodeLabelMappings = Map.of(NodeLabel.ALL_NODES, "*");
        var relationshipTypeMappings = Map.of(RelationshipType.ALL_RELATIONSHIPS, "*");
        var nodeProperties = List.of("name", "age", "population");
        var relationshipProperties = Collections.<String>emptyList();

        var reader = new GraphDimensionsReader(
            transactionContext,
            idGeneratorFactory,
            nodeLabelMappings,
            relationshipTypeMappings,
            nodeProperties,
            relationshipProperties
        );

        GraphDimensions dimensions = reader.call();

        assertThat(dimensions.nodePropertyTokens()).containsKeys("name", "age", "population");
        assertThat(dimensions.nodePropertyTokens().values()).allMatch(token -> token >= 0);
    }

    @Test
    void shouldReadRelationshipPropertyTokens() {
        var transactionContext = TestSupport.fullAccessTransaction(db);
        var idGeneratorFactory = GraphDatabaseApiProxy.resolveDependency(db, IdGeneratorFactory.class);

        var nodeLabelMappings = Map.of(NodeLabel.ALL_NODES, "*");
        var relationshipTypeMappings = Map.of(RelationshipType.ALL_RELATIONSHIPS, "*");
        var nodeProperties = Collections.<String>emptyList();
        var relationshipProperties = List.of("since");

        var reader = new GraphDimensionsReader(
            transactionContext,
            idGeneratorFactory,
            nodeLabelMappings,
            relationshipTypeMappings,
            nodeProperties,
            relationshipProperties
        );

        GraphDimensions dimensions = reader.call();

        assertThat(dimensions.relationshipPropertyTokens()).containsKey("since");
        assertThat(dimensions.relationshipPropertyTokens().get("since")).isGreaterThanOrEqualTo(0);
    }

    @Test
    void shouldHandleEmptyDatabase() {
        clearDb();

        var transactionContext = TestSupport.fullAccessTransaction(db);
        var idGeneratorFactory = GraphDatabaseApiProxy.resolveDependency(db, IdGeneratorFactory.class);

        var nodeLabelMappings = Map.of(NodeLabel.ALL_NODES, "*");
        var relationshipTypeMappings = Map.of(RelationshipType.ALL_RELATIONSHIPS, "*");
        var nodeProperties = Collections.<String>emptyList();
        var relationshipProperties = Collections.<String>emptyList();

        var reader = new GraphDimensionsReader(
            transactionContext,
            idGeneratorFactory,
            nodeLabelMappings,
            relationshipTypeMappings,
            nodeProperties,
            relationshipProperties
        );

        GraphDimensions dimensions = reader.call();

        assertThat(dimensions.nodeCount()).isEqualTo(0L);
        assertThat(dimensions.relCountUpperBound()).isEqualTo(0L);
    }

    @Test
    void shouldReadHighestPossibleNodeCount() {
        var transactionContext = TestSupport.fullAccessTransaction(db);
        var idGeneratorFactory = GraphDatabaseApiProxy.resolveDependency(db, IdGeneratorFactory.class);

        var nodeLabelMappings = Map.of(NodeLabel.ALL_NODES, "*");
        var relationshipTypeMappings = Map.of(RelationshipType.ALL_RELATIONSHIPS, "*");
        var nodeProperties = Collections.<String>emptyList();
        var relationshipProperties = Collections.<String>emptyList();

        var reader = new GraphDimensionsReader(
            transactionContext,
            idGeneratorFactory,
            nodeLabelMappings,
            relationshipTypeMappings,
            nodeProperties,
            relationshipProperties
        );

        GraphDimensions dimensions = reader.call();

        assertThat(dimensions.highestPossibleNodeCount()).isGreaterThanOrEqualTo(dimensions.nodeCount());
    }
}

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
package org.neo4j.gds.core.io.file;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.api.DatabaseId;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.api.RelationshipPropertyStore;
import org.neo4j.gds.api.schema.MutableGraphSchema;
import org.neo4j.gds.api.schema.MutableNodeSchema;
import org.neo4j.gds.core.io.GraphStoreRelationshipVisitor;
import org.neo4j.gds.core.loading.GraphStoreBuilder;
import org.neo4j.gds.core.loading.ImmutableNodes;
import org.neo4j.gds.core.loading.ImmutableStaticCapabilities;
import org.neo4j.gds.core.loading.RelationshipImportResult;
import org.neo4j.gds.core.loading.construction.GraphFactory;
import org.neo4j.gds.core.loading.construction.RelationshipsBuilder;
import org.neo4j.gds.core.loading.construction.RelationshipsBuilderBuilder;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.IdFunction;
import org.neo4j.gds.extension.Inject;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.gds.TestSupport.assertGraphEquals;
import static org.neo4j.gds.TestSupport.fromGdl;

@GdlExtension
class GraphStoreRelationshipVisitorTest {

    @GdlGraph
    static String DB_CYPHER = "CREATE (a:A)-[:R {p: 1.23}]->(b:A)-[:R1 {r: 1337}]->(c:B)-[:R1 {r: 42}]->(a)-[:R2]->(b)";

    @Inject
    GraphStore graphStore;

    @Inject
    Graph graph;

    @GdlGraph(graphNamePrefix = "multipleProps")
    static String MULTI_PROPS_CYPHER = "(a)-[:R {p: 42.0D, r: 13.37D}]->(b)";

    @Inject
    Graph multiplePropsGraph;

    @Inject
    IdFunction multiplePropsIdFunction;

    @Test
    void shouldAddRelationshipsToRelationshipBuilder() {
        var relationshipSchema = graphStore.schema().relationshipSchema();

        Map<String, RelationshipsBuilder> relationshipBuildersByType = new HashMap<>();
        Supplier<RelationshipsBuilderBuilder> relationshipBuilderSupplier = () -> GraphFactory
            .initRelationshipsBuilder()
            .concurrency(1)
            .nodes(graph);
        var relationshipVisitor = new GraphStoreRelationshipVisitor(relationshipSchema, relationshipBuilderSupplier, relationshipBuildersByType, List.of());

        var relationshipTypeR = RelationshipType.of("R");
        var relationshipTypeR1 = RelationshipType.of("R1");
        var relationshipTypeR2 = RelationshipType.of("R2");
        graph.forEachNode(nodeId -> {
            visitRelationshipType(relationshipVisitor, nodeId, relationshipTypeR, Optional.of("p"));
            visitRelationshipType(relationshipVisitor, nodeId, relationshipTypeR1, Optional.of("r"));
            visitRelationshipType(relationshipVisitor, nodeId, relationshipTypeR2, Optional.empty());
            return true;
        });

        var actualGraph = createGraph(graph, relationshipBuildersByType, 4L);
        assertGraphEquals(graph, actualGraph);
    }

    @Test
    void shouldBuildRelationshipsWithMultipleProperties() {
        GraphStoreRelationshipVisitor.Builder relationshipVisitorBuilder = new GraphStoreRelationshipVisitor.Builder();
        Map<String, RelationshipsBuilder> relationshipBuildersByType = new ConcurrentHashMap<>();
        var relationshipVisitor = relationshipVisitorBuilder
            .withNodes(multiplePropsGraph)
            .withRelationshipSchema(multiplePropsGraph.schema().relationshipSchema())
            .withRelationshipBuildersToTypeResultMap(relationshipBuildersByType)
            .withInverseIndexedRelationshipTypes(List.of())
            .withAllocationTracker()
            .withConcurrency(1)
            .build();

        relationshipVisitor.type("R");
        relationshipVisitor.startId(multiplePropsIdFunction.of("a"));
        relationshipVisitor.endId(multiplePropsIdFunction.of("b"));
        relationshipVisitor.property("p", 42.0D);
        relationshipVisitor.property("r", 13.37D);
        relationshipVisitor.endOfEntity();

        var actualGraph = createGraph(multiplePropsGraph, relationshipBuildersByType, 1L);
        assertGraphEquals(multiplePropsGraph, actualGraph);
    }

    @Test
    void shouldBuildRelationshipsWithInverseIndex() {
        var expectedGraph = fromGdl("(a)-[R]->(b)");

        GraphStoreRelationshipVisitor.Builder relationshipVisitorBuilder = new GraphStoreRelationshipVisitor.Builder();
        Map<String, RelationshipsBuilder> relationshipBuildersByType = new ConcurrentHashMap<>();
        var relationshipVisitor = relationshipVisitorBuilder
            .withNodes(expectedGraph)
            .withRelationshipSchema(expectedGraph.schema().relationshipSchema())
            .withRelationshipBuildersToTypeResultMap(relationshipBuildersByType)
            .withInverseIndexedRelationshipTypes(List.of(RelationshipType.of("R")))
            .withAllocationTracker()
            .withConcurrency(1)
            .build();

        relationshipVisitor.type("R");
        relationshipVisitor.startId(expectedGraph.toOriginalNodeId("a"));
        relationshipVisitor.endId(expectedGraph.toOriginalNodeId("b"));
        relationshipVisitor.endOfEntity();

        assertThat(relationshipBuildersByType.get("R").build().inverseTopology()).isPresent();
    }

    private Graph createGraph(
        Graph expectedGraph,
        Map<String, RelationshipsBuilder> relationshipBuildersByType,
        long expectedImportedRelationshipsCount
    ) {
        var actualRelationships = FileToGraphStoreImporter.relationshipTopologyAndProperties(relationshipBuildersByType);
        assertThat(actualRelationships.importedRelationships()).isEqualTo(expectedImportedRelationshipsCount);

        var nodes = ImmutableNodes.builder()
            .idMap(expectedGraph)
            .schema(MutableNodeSchema.from(expectedGraph.schema().nodeSchema()))
            .build();

        Map<RelationshipType, RelationshipPropertyStore> propertyStores = actualRelationships.properties();
        return new GraphStoreBuilder()
            .schema(MutableGraphSchema.from(expectedGraph.schema()))
            .capabilities(ImmutableStaticCapabilities.of(true))
            .nodes(nodes)
            .relationshipImportResult(RelationshipImportResult.of(
                actualRelationships.topologies(),
                propertyStores,
                expectedGraph.schema().relationshipSchema().directions()
            ))
            .databaseId(DatabaseId.random())
            .concurrency(1)
            .build()
            .getUnion();
    }

    private void visitRelationshipType(GraphStoreRelationshipVisitor relationshipVisitor, long nodeId, RelationshipType relationshipType, Optional<String> relationshipPropertyKey) {
        graph
            .relationshipTypeFilteredGraph(Set.of(relationshipType))
            .forEachRelationship(nodeId, 0.0, (source, target, propertyValue) -> {
                relationshipVisitor.startId(source);
                relationshipVisitor.endId(target);
                relationshipPropertyKey.ifPresent(propertyKey -> relationshipVisitor.property(propertyKey, propertyValue));
                relationshipVisitor.type(relationshipType.name());
                relationshipVisitor.endOfEntity();
                return true;
            });
    }
}

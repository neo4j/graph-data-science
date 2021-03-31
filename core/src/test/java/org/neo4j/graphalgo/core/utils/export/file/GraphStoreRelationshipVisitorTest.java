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
package org.neo4j.graphalgo.core.utils.export.file;


import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.RelationshipType;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.GraphStore;
import org.neo4j.graphalgo.api.RelationshipPropertyStore;
import org.neo4j.graphalgo.core.loading.construction.GraphFactory;
import org.neo4j.graphalgo.core.loading.construction.RelationshipsBuilder;
import org.neo4j.graphalgo.core.utils.mem.AllocationTracker;
import org.neo4j.graphalgo.extension.GdlExtension;
import org.neo4j.graphalgo.extension.GdlGraph;
import org.neo4j.graphalgo.extension.Inject;
import org.neo4j.kernel.database.TestDatabaseIdRepository;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.graphalgo.TestSupport.assertGraphEquals;

@GdlExtension
class GraphStoreRelationshipVisitorTest {

    @GdlGraph
    static String DB_CYPHER = "CREATE (a:A)-[:R {p: 1.23}]->(b:A)-[:R1 {r: 1337}]->(c:B)-[:R1 {r: 42}]->(a)-[:R2]->(b)";

    @Inject
    GraphStore graphStore;

    @Inject
    Graph graph;

    @Test
    void shouldAddRelationshipsToRelationshipBuilder() {
        var relationshipSchema = graphStore.schema().relationshipSchema();
        var relationshipsBuilderBuilder = GraphFactory.initRelationshipsBuilder()
            .concurrency(1)
            .nodes(graph)
            .tracker(AllocationTracker.empty());

        Map<String, RelationshipsBuilder> relationshipBuildersByType = new HashMap<>();
        var relationshipVisitor = new GraphStoreRelationshipVisitor(relationshipSchema, relationshipsBuilderBuilder, relationshipBuildersByType);

        var relationshipTypeR = RelationshipType.of("R");
        var relationshipTypeR1 = RelationshipType.of("R1");
        var relationshipTypeR2 = RelationshipType.of("R2");
        graph.forEachNode(nodeId -> {
            visitRelationshipType(relationshipVisitor, nodeId, relationshipTypeR, Optional.of("p"));
            visitRelationshipType(relationshipVisitor, nodeId, relationshipTypeR1, Optional.of("r"));
            visitRelationshipType(relationshipVisitor, nodeId, relationshipTypeR2, Optional.empty());
            return true;
        });

        var actualRelationships = CsvToGraphStoreExporter.relationshipTopologyAndProperties(
            relationshipBuildersByType,
            relationshipSchema
        );
        assertThat(actualRelationships.importedRelationships()).isEqualTo(4L);

        assertThat(actualRelationships.topologies().get(relationshipTypeR).elementCount()).isEqualTo(1L);
        assertThat(actualRelationships.topologies().get(relationshipTypeR1).elementCount()).isEqualTo(2L);

        Map<? extends RelationshipType, ? extends RelationshipPropertyStore> propertyStores = actualRelationships.properties();
        var actualGraph = new GraphStoreBuilder()
            .relationshipPropertyStores(propertyStores)
            .relationships(actualRelationships.topologies())
            .nodes(graph)
            .databaseId(TestDatabaseIdRepository.randomNamedDatabaseId())
            .concurrency(1)
            .tracker(AllocationTracker.empty())
            .build()
            .getUnion();

        assertGraphEquals(graph, actualGraph);
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

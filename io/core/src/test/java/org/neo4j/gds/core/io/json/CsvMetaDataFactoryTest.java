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
package org.neo4j.gds.core.io.json;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.api.DatabaseId;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.core.io.file.GraphInfo;
import org.neo4j.gds.core.io.file.GraphInfoBuilder;
import org.neo4j.gds.core.loading.CSRGraphStore;
import org.neo4j.gds.core.loading.Capabilities;
import org.neo4j.gds.core.loading.ImmutableStaticCapabilities;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.gdl.GdlFactory;

import java.util.function.Function;
import java.util.stream.Collectors;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

@GdlExtension
class CsvMetaDataFactoryTest {

    @GdlGraph
    public static final String GDL = """
            (a:Label1 {prop1: 42, prop2: 4.2}),
            (b:Label2 {prop3: [1L, 3L, 3L, 7L], prop4: [1.0f, 3.0f, 3.0f, 7.0f]}),
            (c:Label3),
            (d:Label3),
            (a)-[:REL_TYPE1 {relProp1: 13.37}]->(b),
            (b)-[:REL_TYPE1 {relProp1: 1.337}]->(c),
            (a)-[:REL_TYPE2]->(c)
            (a)-[:REL_TYPE2]->(d)
        """;

    @Inject
    private GraphStore graphStore;

    @Test
    void toGraphInfo() {
        var graphStoreMetadata = GraphStoreMetadataFactory.fromGraphStore(graphStore);

        var result = CsvMetaDataFactory.toGraphInfo(graphStoreMetadata);

        var expected = getGraphInfo(graphStore);
        assertThat(result).isEqualTo(expected);
    }

    @Test
    void toGraphInfoWithRemote() {
        var graphStore = GdlFactory.builder()
            .graphName("my_graph")
            .databaseId(DatabaseId.of("another_custom_name"))
            .databaseLocation(org.neo4j.gds.api.DatabaseInfo.DatabaseLocation.REMOTE)
            .remoteDatabaseId(DatabaseId.of("my_remote_db"))
            .graphCapabilities(ImmutableStaticCapabilities.of(Capabilities.WriteMode.REMOTE))
            .gdlGraph("()-->()")
            .build()
            .build();
        var graphStoreMetadata = GraphStoreMetadataFactory.fromGraphStore(graphStore);

        var result = CsvMetaDataFactory.toGraphInfo(graphStoreMetadata);

        var expected = getGraphInfo(graphStore);
        assertThat(result).isEqualTo(expected);
    }

    @Test
    void toCapabilities() {
        var graphStoreMetadata = GraphStoreMetadataFactory.fromGraphStore(graphStore);

        var result = CsvMetaDataFactory.toCapabilities(graphStoreMetadata);

        var expected = ImmutableStaticCapabilities
            .builder()
            .writeMode(graphStore.capabilities().writeMode())
            .build();
        assertThat(result).isEqualTo(expected);
    }

    @Test
    void toCapabilitiesWithRemote() {
        var graphStore = GdlFactory.builder()
            .graphName("my_graph")
            .databaseId(DatabaseId.of("another_custom_name"))
            .databaseLocation(org.neo4j.gds.api.DatabaseInfo.DatabaseLocation.REMOTE)
            .remoteDatabaseId(DatabaseId.of("my_remote_db"))
            .graphCapabilities(ImmutableStaticCapabilities.of(Capabilities.WriteMode.REMOTE))
            .gdlGraph("()-->()")
            .build()
            .build();
        var graphStoreMetadata = GraphStoreMetadataFactory.fromGraphStore(graphStore);

        var result = CsvMetaDataFactory.toCapabilities(graphStoreMetadata);

        var expected = ImmutableStaticCapabilities
            .builder()
            .writeMode(graphStore.capabilities().writeMode())
            .build();
        assertThat(result).isEqualTo(expected);
    }

    private static GraphInfo getGraphInfo(GraphStore graphStore) {
        return GraphInfoBuilder
            .builder()
            .databaseInfo(graphStore.databaseInfo())
            .idMapBuilderType(graphStore.nodes().typeId())
            .nodeCount(graphStore.nodeCount())
            .maxOriginalId(graphStore.nodes().highestOriginalId())
            .relationshipTypeCounts(graphStore
                .relationshipTypes()
                .stream()
                .collect(Collectors.toMap(
                        Function.identity(),
                        graphStore::relationshipCount
                    ))
                )
            .inverseIndexedRelationshipTypes(graphStore.inverseIndexedRelationshipTypes())
            .build();
    }
}
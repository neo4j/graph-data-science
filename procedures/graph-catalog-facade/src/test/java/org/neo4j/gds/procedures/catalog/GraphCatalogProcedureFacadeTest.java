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
package org.neo4j.gds.procedures.catalog;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.api.CompositeRelationshipIterator;
import org.neo4j.gds.api.DatabaseId;
import org.neo4j.gds.api.DatabaseInfo;
import org.neo4j.gds.api.DatabaseInfo.DatabaseLocation;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.api.IdMap;
import org.neo4j.gds.api.ProcedureReturnColumns;
import org.neo4j.gds.api.RelationshipProperty;
import org.neo4j.gds.api.RelationshipPropertyStore;
import org.neo4j.gds.api.ResultStore;
import org.neo4j.gds.api.Topology;
import org.neo4j.gds.api.User;
import org.neo4j.gds.api.nodeproperties.ValueType;
import org.neo4j.gds.api.properties.graph.GraphProperty;
import org.neo4j.gds.api.properties.graph.GraphPropertyValues;
import org.neo4j.gds.api.properties.nodes.NodeProperty;
import org.neo4j.gds.api.properties.nodes.NodePropertyValues;
import org.neo4j.gds.api.schema.GraphSchema;
import org.neo4j.gds.applications.ApplicationsFacadeBuilder;
import org.neo4j.gds.applications.algorithms.machinery.RequestScopedDependencies;
import org.neo4j.gds.applications.algorithms.machinery.WriteContext;
import org.neo4j.gds.applications.graphstorecatalog.GraphCatalogApplications;
import org.neo4j.gds.config.GraphProjectConfig;
import org.neo4j.gds.core.loading.Capabilities;
import org.neo4j.gds.core.loading.DeletionResult;
import org.neo4j.gds.core.loading.GraphStoreCatalogEntry;
import org.neo4j.gds.core.loading.SingleTypeRelationships;

import java.time.ZonedDateTime;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class GraphCatalogProcedureFacadeTest {
    @Test
    void shouldDetermineIfGraphExists() {
        var businessFacade = mock(GraphCatalogApplications.class);
        var catalogFacade = new LocalGraphCatalogProcedureFacade(
            RequestScopedDependencies.builder()
                .with(DatabaseId.of("current database"))
                .with(new User("current user", false))
                .build(),
            null,
            null,
            null,
            null,
            new ApplicationsFacadeBuilder().with(businessFacade).build().graphCatalog(),
            WriteContext.builder().build(),
            null,
            null
        );

        catalogFacade.graphExists("some graph");

        verify(businessFacade).graphExists(
            new User("current user", false),
            DatabaseId.of("current database"),
            "some graph"
        );
    }

    @Test
    void shouldListGraphsWithoutDegreeDistribution() {
        var procedureReturnColumns = mock(ProcedureReturnColumns.class);
        var businessFacade = mock(GraphCatalogApplications.class);

        var procedureContext =  WriteContext.builder().build();
        var catalogFacade = new LocalGraphCatalogProcedureFacade(
            RequestScopedDependencies.builder()
                .with(new User("Bob", false))
                .build(),
            null,
            null,
            null,
            null,
            new ApplicationsFacadeBuilder().with(businessFacade).build().graphCatalog(),
            procedureContext,
            procedureReturnColumns,
            null
        );

        // the return columns mock returns false by default (all simple types get defaults btw) - should I code that explicitly?
        when(businessFacade.listGraphs(new User("Bob", false), "foo", false, null))
            .thenReturn(
                List.of(
                    Pair.of(new GraphStoreCatalogEntry(
                        new StubGraphStore(),
                        mock(GraphProjectConfig.class),
                        ResultStore.EMPTY
                    ), null)
                )
            );
        var graphs = catalogFacade.listGraphs("foo");

        // when we specify that we do not want a degree distribution, we mean we get null instead of a map
        assertThat(graphs.findFirst().orElseThrow().degreeDistribution).isNull();
    }

    @Test
    void shouldListGraphsWithDegreeDistribution() {
        var procedureReturnColumns = mock(ProcedureReturnColumns.class);
        var businessFacade = mock(GraphCatalogApplications.class);
        var procedureContext =  WriteContext.builder().build();

        var catalogFacade = new LocalGraphCatalogProcedureFacade(
            RequestScopedDependencies.builder()
                .with(new User("Bob", false))
                .build(),
            null,
            null,
            null,
            null,
            new ApplicationsFacadeBuilder().with(businessFacade).build().graphCatalog(),
            procedureContext,
            procedureReturnColumns,
            null
        );

        when(procedureReturnColumns.contains("degreeDistribution")).thenReturn(true);
        when(businessFacade.listGraphs(new User("Bob", false), "foo", true, null))
            .thenReturn(
                List.of(
                    Pair.of(
                        new GraphStoreCatalogEntry(
                            new StubGraphStore(),
                            mock(GraphProjectConfig.class),
                            ResultStore.EMPTY
                        ),
                        Map.of("deg", 117, "ree", 23, "dist", 512)
                    )
                )
            );
        var graphs = catalogFacade.listGraphs("foo");

        // when we specify that we do want a degree distribution, we get a map of stuff
        assertThat(graphs.findFirst().orElseThrow().degreeDistribution).containsExactlyInAnyOrderEntriesOf(
            Map.of(
                "deg",
                117,
                "ree",
                23,
                "dist",
                512
            )
        );
    }

    @Test
    void shouldListGraphsWithoutMemoryUsage() {
        var procedureReturnColumns = mock(ProcedureReturnColumns.class);
        var businessFacade = mock(GraphCatalogApplications.class);
        var procedureContext =  WriteContext.builder().build();

        var catalogFacade = new LocalGraphCatalogProcedureFacade(
            RequestScopedDependencies.builder()
                .with(new User("Bob", false))
                .build(),
            null,
            null,
            null,
            null,
            new ApplicationsFacadeBuilder().with(businessFacade).build().graphCatalog(),
            procedureContext,
            procedureReturnColumns,
            null
        );

        // the return columns mock returns false by default (all simple types get defaults btw) - should I code that explicitly?
        when(businessFacade.listGraphs(new User("Bob", false), "foo", false, null))
            .thenReturn(
                List.of(
                    Pair.of(new GraphStoreCatalogEntry(
                        new StubGraphStore(),
                        mock(GraphProjectConfig.class),
                        ResultStore.EMPTY
                    ), null)
                )
            );
        var graphs = catalogFacade.listGraphs("foo");

        // when we specify that we do not want memory usage, we mean we get null instead of a map
        var result = graphs.findFirst().orElseThrow();
        assertThat(result.memoryUsage).isEmpty();
        assertThat(result.sizeInBytes).isEqualTo(-1L);
    }

    @ParameterizedTest
    @ValueSource(strings = {"memoryUsage", "sizeInBytes"})
    void shouldListGraphsWithMemoryUsage(String returnColumn) {
        var procedureReturnColumns = mock(ProcedureReturnColumns.class);
        var businessFacade = mock(GraphCatalogApplications.class);
        var procedureContext =  WriteContext.builder().build();

        var catalogFacade = new LocalGraphCatalogProcedureFacade(
            RequestScopedDependencies.builder()
                .with(new User("Bob", false))
                .build(),
            null,
            null,
            null,
            null,
            new ApplicationsFacadeBuilder().with(businessFacade).build().graphCatalog(),
            procedureContext,
            procedureReturnColumns,
            null
        );

        when(procedureReturnColumns.contains(returnColumn)).thenReturn(true);
        when(businessFacade.listGraphs(new User("Bob", false), "foo", false, null))
            .thenReturn(
                List.of(
                    Pair.of(new GraphStoreCatalogEntry(
                        new StubGraphStore(),
                        mock(GraphProjectConfig.class),
                        ResultStore.EMPTY
                    ), null)
                )
            );
        var graphs = catalogFacade.listGraphs("foo");

        // when we specify that we do not want memory usage, we mean we get null instead of a map
        var result = graphs.findFirst().orElseThrow();
        assertThat(result.memoryUsage).isEqualTo("16 Bytes");
        assertThat(result.sizeInBytes).isEqualTo(16L);
    }

    private static class StubGraphStore implements GraphStore {
        @Override
        public DatabaseInfo databaseInfo() {
            return DatabaseInfo.of(DatabaseId.of("foo"), DatabaseLocation.LOCAL);
        }

        @Override
        public GraphSchema schema() {
            return GraphSchema.empty();
        }

        @Override
        public ZonedDateTime creationTime() {
            return ZonedDateTime.now();
        }

        @Override
        public ZonedDateTime modificationTime() {
            return ZonedDateTime.now();
        }

        @Override
        public Capabilities capabilities() {
            throw new UnsupportedOperationException("TODO");
        }

        @Override
        public Set<String> graphPropertyKeys() {
            throw new UnsupportedOperationException("TODO");
        }

        @Override
        public boolean hasGraphProperty(String propertyKey) {
            throw new UnsupportedOperationException("TODO");
        }

        @Override
        public GraphProperty graphProperty(String propertyKey) {
            throw new UnsupportedOperationException("TODO");
        }

        @Override
        public GraphPropertyValues graphPropertyValues(String propertyKey) {
            throw new UnsupportedOperationException("TODO");
        }

        @Override
        public void addGraphProperty(String propertyKey, GraphPropertyValues propertyValues) {
            throw new UnsupportedOperationException("TODO");
        }

        @Override
        public void removeGraphProperty(String propertyKey) {
            throw new UnsupportedOperationException("TODO");
        }

        @Override
        public long nodeCount() {
            return 42L;
        }

        @Override
        public IdMap nodes() {
            throw new UnsupportedOperationException("TODO");
        }

        @Override
        public Set<NodeLabel> nodeLabels() {
            throw new UnsupportedOperationException("TODO");
        }

        @Override
        public void addNodeLabel(NodeLabel nodeLabel) {
            throw new UnsupportedOperationException("TODO");
        }

        @Override
        public Set<String> nodePropertyKeys(NodeLabel label) {
            throw new UnsupportedOperationException("TODO");
        }

        @Override
        public Set<String> nodePropertyKeys() {
            throw new UnsupportedOperationException("TODO");
        }

        @Override
        public boolean hasNodeProperty(String propertyKey) {
            throw new UnsupportedOperationException("TODO");
        }

        @Override
        public boolean hasNodeProperty(NodeLabel label, String propertyKey) {
            throw new UnsupportedOperationException("TODO");
        }

        @Override
        public boolean hasNodeProperty(Collection<NodeLabel> labels, String propertyKey) {
            throw new UnsupportedOperationException("TODO");
        }

        @Override
        public NodeProperty nodeProperty(String propertyKey) {
            throw new UnsupportedOperationException("TODO");
        }

        @Override
        public void addNodeProperty(
            Set<NodeLabel> nodeLabels,
            String propertyKey,
            NodePropertyValues propertyValues
        ) {
            throw new UnsupportedOperationException("TODO");
        }

        @Override
        public void removeNodeProperty(String propertyKey) {
            throw new UnsupportedOperationException("TODO");
        }

        @Override
        public long relationshipCount() {
            return 87L;
        }

        @Override
        public long relationshipCount(RelationshipType relationshipType) {
            throw new UnsupportedOperationException("TODO");
        }

        @Override
        public Set<RelationshipType> relationshipTypes() {
            throw new UnsupportedOperationException("TODO");
        }

        @Override
        public boolean hasRelationshipType(RelationshipType relationshipType) {
            throw new UnsupportedOperationException("TODO");
        }

        @Override
        public Set<RelationshipType> inverseIndexedRelationshipTypes() {
            throw new UnsupportedOperationException("TODO");
        }

        @Override
        public boolean hasRelationshipProperty(RelationshipType relType, String propertyKey) {
            throw new UnsupportedOperationException("TODO");
        }

        @Override
        public ValueType relationshipPropertyType(String propertyKey) {
            throw new UnsupportedOperationException("TODO");
        }

        @Override
        public Set<String> relationshipPropertyKeys() {
            throw new UnsupportedOperationException("TODO");
        }

        @Override
        public Set<String> relationshipPropertyKeys(RelationshipType relationshipType) {
            throw new UnsupportedOperationException("TODO");
        }

        @Override
        public RelationshipProperty relationshipPropertyValues(RelationshipType relationshipType, String propertyKey) {
            throw new UnsupportedOperationException("TODO");
        }

        @Override
        public void addRelationshipType(SingleTypeRelationships relationships) {
            throw new UnsupportedOperationException("TODO");
        }

        @Override
        public void addInverseIndex(
            RelationshipType relationshipType,
            Topology topology,
            Optional<RelationshipPropertyStore> properties
        ) {
            throw new UnsupportedOperationException("TODO");
        }

        @Override
        public DeletionResult deleteRelationships(RelationshipType relationshipType) {
            throw new UnsupportedOperationException("TODO");
        }

        @Override
        public Graph getGraph(Collection<NodeLabel> nodeLabels) {
            throw new UnsupportedOperationException("TODO");
        }

        @Override
        public Graph getGraph(
            Collection<NodeLabel> nodeLabels,
            Collection<RelationshipType> relationshipTypes,
            Optional<String> maybeRelationshipProperty
        ) {
            throw new UnsupportedOperationException("TODO");
        }

        @Override
        public Graph getUnion() {
            throw new UnsupportedOperationException("TODO");
        }

        @Override
        public CompositeRelationshipIterator getCompositeRelationshipIterator(
            RelationshipType relationshipType,
            Collection<String> propertyKeys
        ) {
            throw new UnsupportedOperationException("TODO");
        }
    }
}

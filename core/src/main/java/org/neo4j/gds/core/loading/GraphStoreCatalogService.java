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
package org.neo4j.gds.core.loading;

import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.api.DatabaseId;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.GraphName;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.api.User;
import org.neo4j.gds.config.AlgoBaseConfig;
import org.neo4j.gds.config.BaseConfig;
import org.neo4j.gds.config.GraphProjectConfig;
import org.neo4j.gds.core.loading.GraphStoreCatalog.GraphStoreCatalogEntryWithUsername;

import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

/**
 * One day the graph catalog won't be a static thing, it'll instead be a dependency you inject here. One day.
 * <p>
 * For now this service helps us engineer some other things.
 * Calls are mostly 1-1, but we can do some handy and _simple_ adapting, to make calling code easier to test,
 * without having to write separate tests for this class.
 */
public class GraphStoreCatalogService {
    public boolean graphExists(User user, DatabaseId databaseId, GraphName graphName) {
        return GraphStoreCatalog.exists(user.getUsername(), databaseId, graphName.getValue());
    }

    public GraphStoreCatalogEntry removeGraph(
        CatalogRequest request,
        GraphName graphName,
        boolean shouldFailIfMissing
    ) {
        var result = new AtomicReference<GraphStoreCatalogEntry>();
        GraphStoreCatalog.remove(
            request,
            graphName.getValue(),
            result::set,
            shouldFailIfMissing
        );
        return result.get();
    }

    public GraphStoreCatalogEntry get(CatalogRequest catalogRequest, GraphName graphName) {
        return GraphStoreCatalog.get(catalogRequest, graphName.getValue());
    }

    /**
     * Load graphstore and graph, with copious validation.
     */
    public GraphResources getGraphResources(
        GraphName graphName,
        AlgoBaseConfig configuration,
        Optional<Iterable<PostLoadValidationHook>> postGraphStoreLoadValidationHooks,
        Optional<Iterable<PostLoadETLHook>> postGraphStoreLoadETLHooks,
        Optional<String> relationshipProperty,
        User user,
        DatabaseId databaseId
    ) {
        var graphStoreCatalogEntry = getGraphStoreCatalogEntry(graphName, configuration, user, databaseId);

        var graphStore = graphStoreCatalogEntry.graphStore();

        postGraphStoreLoadValidationHooks.ifPresent(hooks -> validateGraphStore(graphStore, hooks));

        var nodeLabels = getNodeLabels(configuration, graphStore);
        var relationshipTypes = getRelationshipTypes(configuration, graphStore);

        // Validate the graph store before going any further
        configuration.graphStoreValidation(graphStore, nodeLabels, relationshipTypes);

        postGraphStoreLoadETLHooks.ifPresent( postLoadETLHooks -> extractAndTransform(graphStore,postLoadETLHooks));

        var graph = graphStore.getGraph(nodeLabels, relationshipTypes, relationshipProperty);

        postGraphStoreLoadValidationHooks.ifPresent(hooks -> validateGraph(graph, hooks));

        return new GraphResources(graphStore, graph, graphStoreCatalogEntry.resultStore());
    }

    private static Collection<RelationshipType> getRelationshipTypes(AlgoBaseConfig config, GraphStore graphStore) {
        if (config.projectAllRelationshipTypes()) return graphStore.relationshipTypes();

        return config.relationshipTypesFilter();
    }

    private static Collection<NodeLabel> getNodeLabels(AlgoBaseConfig config, GraphStore graphStore) {
        var nodeLabels = config.nodeLabelsFilter();

        if (nodeLabels.isEmpty()) return graphStore.nodeLabels();

        return nodeLabels;
    }

    /**
     * Some use cases need special validation. We do this right after loading.
     *
     * @throws java.lang.IllegalArgumentException if the graph store did not conform to desired invariants
     */
    private void validateGraphStore(GraphStore graphStore, Iterable<PostLoadValidationHook> validationHooks) {
        for (PostLoadValidationHook hook : validationHooks) {
            hook.onGraphStoreLoaded(graphStore);
        }
    }

    private void extractAndTransform(GraphStore graphStore, Iterable<PostLoadETLHook> etlHooks) {
        for (PostLoadETLHook hook : etlHooks) {
            hook.onGraphStoreLoaded(graphStore);
        }
    }

    /**
     * Some use cases need special validation. We do this right after loading.
     *
     * @throws java.lang.IllegalArgumentException if the graph did not conform to desired invariants
     */
    private void validateGraph(Graph graph, Iterable<PostLoadValidationHook> validationHooks) {
        for (PostLoadValidationHook hook : validationHooks) {
            hook.onGraphLoaded(graph);
        }
    }

    public GraphStoreCatalogEntry getGraphStoreCatalogEntry(
        GraphName graphName,
        BaseConfig config,
        User user,
        DatabaseId databaseId
    ) {
        var catalogRequest = CatalogRequest.of(user, databaseId, config.usernameOverride());

        return get(catalogRequest, graphName);
    }

    /**
     * Predicate around @graphExists
     *
     * @throws java.lang.IllegalArgumentException if graph already exists in graph catalog
     */
    public void ensureGraphDoesNotExist(User user, DatabaseId databaseId, GraphName graphName) {
        if (graphExists(user, databaseId, graphName)) {
            String message = formatWithLocale(
                "A graph with name '%s' already exists.",
                graphName
            );
            throw new IllegalArgumentException(message);
        }
    }

    /**
     * Predicate around @graphExists
     *
     * @throws java.lang.IllegalArgumentException if graph does not exist in graph catalog
     */
    public void ensureGraphExists(User user, DatabaseId databaseId, GraphName graphName) {
        if (!graphExists(user, databaseId, graphName)) {
            String message = formatWithLocale(
                "The graph '%s' does not exist.",
                graphName
            );
            throw new IllegalArgumentException(message);
        }
    }

    public Optional<Map<String, Object>> getDegreeDistribution(
        User user,
        DatabaseId databaseId,
        GraphName graphName
    ) {
        return GraphStoreCatalog.getDegreeDistribution(user.getUsername(), databaseId, graphName.getValue());
    }

    public void setDegreeDistribution(
        User user,
        DatabaseId databaseId,
        GraphName graphName,
        Map<String, Object> degreeDistribution
    ) {
        GraphStoreCatalog.setDegreeDistribution(
            user.getUsername(),
            databaseId,
            graphName.getValue(),
            degreeDistribution
        );
    }

    public Stream<GraphStoreCatalogEntry> getAllGraphStores() {
        return GraphStoreCatalog
            .getAllGraphStores()
            .map(GraphStoreCatalogEntryWithUsername::catalogEntry);
    }

    public long graphStoreCount() {
        return GraphStoreCatalog.graphStoreCount();
    }

    public Collection<GraphStoreCatalogEntry> getGraphStores(User user) {
        return GraphStoreCatalog.getGraphStores(user.getUsername());
    }

    public void set(GraphProjectConfig configuration, GraphStore graphStore) {
        GraphStoreCatalog.set(configuration, graphStore);
    }
}

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

import org.neo4j.gds.GraphParameters;
import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.api.DatabaseId;
import org.neo4j.gds.api.GraphName;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.api.User;
import org.neo4j.gds.api.graph.store.catalog.GraphStoreAddedEventListener;
import org.neo4j.gds.api.graph.store.catalog.GraphStoreRemovedEventListener;
import org.neo4j.gds.config.GraphProjectConfig;
import org.neo4j.gds.core.loading.GraphStoreCatalog.GraphStoreCatalogEntryWithUsername;
import org.neo4j.gds.core.loading.validation.GraphLoadETL;
import org.neo4j.gds.core.loading.validation.GraphLoadETLBuilder;
import org.neo4j.gds.core.loading.validation.GraphStoreValidation;
import org.neo4j.gds.core.loading.validation.GraphValidation;
import org.neo4j.gds.logging.Log;

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
        return GraphStoreCatalog.exists(user.getUsername(), databaseId, graphName.value());
    }

    public GraphStoreCatalogEntry removeGraph(
        CatalogRequest request,
        GraphName graphName,
        boolean shouldFailIfMissing
    ) {
        var result = new AtomicReference<GraphStoreCatalogEntry>();
        GraphStoreCatalog.remove(
            request,
            graphName.value(),
            result::set,
            shouldFailIfMissing
        );
        return result.get();
    }

    public GraphStoreCatalogEntry getGraphStoreCatalogEntry(CatalogRequest catalogRequest, GraphName graphName) {
        return GraphStoreCatalog.get(catalogRequest, graphName.value());
    }

    public GraphResources getGraphResources(
        GraphName graphName,
        GraphParameters graphParameters,
        Optional<String> relationshipProperty,
        GraphStoreValidation graphStoreValidation,
        Optional<Iterable<PostLoadValidationHook>> postGraphStoreLoadValidationHooks,
        User user,
        DatabaseId databaseId
    ) {
        var graphLoadETLBuilder = new GraphLoadETLBuilder();

        graphLoadETLBuilder.with(graphStoreValidation::validate);
        postGraphStoreLoadValidationHooks.ifPresent(
            postLoadValidationHooks -> postLoadValidationHooks.forEach(
                postLoadValidationHook -> {
                    graphLoadETLBuilder.with(postLoadValidationHook::onGraphStoreLoaded);
                    graphLoadETLBuilder.with(postLoadValidationHook::onGraphLoaded);
                })
        );
        var graphLoadETL = graphLoadETLBuilder.build();

        return loadGraphResources(
            databaseId,
            graphName,
            user,
            graphParameters,
            relationshipProperty,
            graphLoadETL
        );
    }

    public GraphStoreCatalogEntry getGraphStoreCatalogEntry(
        GraphName graphName,
        User user,
        Optional<String> usernameOverride,
        DatabaseId databaseId
    ){
        return getGraphStoreCatalogEntry(
            new CatalogRequest(
                databaseId.databaseName(),
                user.getUsername(),
                usernameOverride,
                user.isAdmin()
            ), graphName);
    }

    public GraphResources fetchGraphResources(
        DatabaseId databaseId,
        GraphName graphName,
        User user,
        GraphParameters graphParameters,
        Optional<String> relationshipProperty,
        GraphStoreValidation graphStoreValidation,
        boolean ignored,
        Optional<GraphValidation> graphValidation
    ) {
        // this will fall away once we have de-duplicated
        var graphLoadETLBuilder = new GraphLoadETLBuilder();
        graphLoadETLBuilder.with(graphStoreValidation::validate);
        graphValidation.ifPresent(hook -> graphLoadETLBuilder.with(hook::validate));
        var graphLoadETL = graphLoadETLBuilder.build();

        return loadGraphResources(
            databaseId,
            graphName,
            user,
            graphParameters,
            relationshipProperty,
            graphLoadETL
        );
    }

    /**
     * Load graph store and graph, under validation and ETL.
     * There are three hooks for your validation and ETL needs:
     *
     * <ol>
     *     <li>When the graph store has been loaded</li>
     *     <li>When mode labels and relationship types have been filtered</li>
     *     <li>When the graph has been loaded</li>
     * </ol>
     *
     * If you want to read books, this method is GoF Template Method and the ETL you pass in is GoF Strategy.
     */
    public GraphResources loadGraphResources(
        DatabaseId databaseId,
        GraphName graphName,
        User user,
        GraphParameters graphParameters,
        Optional<String> relationshipProperty,
        GraphLoadETL graphLoadETL
    ) {
        var graphStoreCatalogEntry = getGraphStoreCatalogEntry(
            graphName,
            user,
            graphParameters.usernameOverride(),
            databaseId
        );

        var graphStore = graphStoreCatalogEntry.graphStore();

        graphLoadETL.onGraphStoreLoaded(graphStore);

        var nodeLabels = GraphStoreCatalogService.resolveNodeLabels(graphStore, graphParameters.nodeLabelsFilter());
        var relationshipTypes = GraphStoreCatalogService.resolveRelationshipTypes(
            graphStore,
            graphParameters.loadAllRelationshipTypes(),
            graphParameters.relationshipTypesFilter()
        );

        graphLoadETL.onGraphStoreFiltered(graphStore, nodeLabels, relationshipTypes, relationshipProperty);

        var graph = graphStore.getGraph(nodeLabels, relationshipTypes, relationshipProperty);

        graphLoadETL.onGraphLoaded(graph);

        return new GraphResources(graphStore, graph, graphStoreCatalogEntry.resultStore());
    }

    public Optional<Map<String, Object>> getDegreeDistribution(
        User user,
        DatabaseId databaseId,
        GraphName graphName
    ) {
        return GraphStoreCatalog.getDegreeDistribution(user.getUsername(), databaseId, graphName.value());
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
            graphName.value(),
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

    public void removeAllLoadedGraphs(DatabaseId databaseId) {
        GraphStoreCatalog.removeAllLoadedGraphs(databaseId);
    }

    public void removeAllLoadedGraphs() {
        GraphStoreCatalog.removeAllLoadedGraphs();
    }

    public void registerGraphStoreAddedListener(GraphStoreAddedEventListener graphStoreAddedEventListener) {
        GraphStoreCatalog.registerGraphStoreAddedListener(graphStoreAddedEventListener);
    }

    public void registerGraphStoreRemovedListener(GraphStoreRemovedEventListener graphStoreRemovedEventListener) {
        GraphStoreCatalog.registerGraphStoreRemovedListener(graphStoreRemovedEventListener);
    }

    public void setLog(Log log) {
        GraphStoreCatalog.setLog(log);
    }

    public static Collection<NodeLabel> resolveNodeLabels(GraphStore graphStore, Collection<NodeLabel> nodeLabelsFilter) {
        return  nodeLabelsFilter.isEmpty()
            ? graphStore.nodeLabels()
            : nodeLabelsFilter;
    }
    public static Collection<RelationshipType> resolveRelationshipTypes(
        GraphStore graphStore,
        boolean loadAllRelationshipTypes,
        Collection<RelationshipType> relationshipTypesFilter
    ) {

        return loadAllRelationshipTypes
            ? graphStore.relationshipTypes()
            : relationshipTypesFilter;
    }

    /**
     * Predicate around @graphExists
     *
     * @throws java.lang.IllegalArgumentException if graph already exists in graph catalog
     */
    public void ensureGraphDoesNotExist(User user, DatabaseId databaseId, GraphName graphName) {
        if (graphExists(user, databaseId, graphName)) {
            throw new GraphAlreadyExistsException(new GraphStoreCatalog.UserCatalogKey(databaseId.databaseName(), graphName.value()));
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
}

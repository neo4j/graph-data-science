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
import org.neo4j.gds.core.loading.validation.GraphStoreValidation;
import org.neo4j.gds.core.loading.validation.GraphValidation;
import org.neo4j.gds.logging.Log;

import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;


public interface GraphStoreCatalogService {
    boolean graphExists(User user, DatabaseId databaseId, GraphName graphName);

    GraphStoreCatalogEntry removeGraph(
        CatalogRequest request,
        GraphName graphName,
        boolean shouldFailIfMissing
    );

    GraphStoreCatalogEntry get(CatalogRequest catalogRequest, GraphName graphName);


    /**
     * Load GraphStore and graph, with copious validation.
     */
    GraphResources getGraphResources(
        GraphName graphName,
        GraphParameters graphParameters,
        Optional<String> relationshipProperty,
        GraphStoreValidation graphStoreValidation,
        Optional<Iterable<PostLoadValidationHook>> postGraphStoreLoadValidationHooks,
        Optional<Iterable<PostLoadETLHook>> postGraphStoreLoadETLHooks,
        User user,
        DatabaseId databaseId
    );

    GraphResources fetchGraphResources(
        GraphName graphName,
        GraphParameters graphParameters,
        Optional<String> relationshipProperty,
        GraphStoreValidation graphStoreValidation,
        Optional<GraphValidation> graphValidation,
        User user,
        DatabaseId databaseId
    );

    GraphResources fetchGraphStoreOnlyResources(
        GraphName graphName,
        GraphParameters graphParameters,
        Optional<String> relationshipProperty,
        GraphStoreValidation graphStoreValidation,
        User user,
        DatabaseId databaseId
    );

    GraphStoreCatalogEntry getGraphStoreCatalogEntry(
        GraphName graphName,
        User user,
        Optional<String> usernameOverride,
        DatabaseId databaseId
    );


    Optional<Map<String, Object>> getDegreeDistribution(
        User user,
        DatabaseId databaseId,
        GraphName graphName
    );

     void setDegreeDistribution(
        User user,
        DatabaseId databaseId,
        GraphName graphName,
        Map<String, Object> degreeDistribution
    );

     Stream<GraphStoreCatalogEntry> getAllGraphStores();

     long graphStoreCount();

     Collection<GraphStoreCatalogEntry> getGraphStores(User user);

    void set(GraphProjectConfig configuration, GraphStore graphStore);

     void removeAllLoadedGraphs(DatabaseId databaseId);

    void registerGraphStoreAddedListener(GraphStoreAddedEventListener graphStoreAddedEventListener);

    void registerGraphStoreRemovedListener(GraphStoreRemovedEventListener graphStoreRemovedEventListener);

    void setLog(Log log);

    /**
     * Predicate around @graphExists
     *
     * @throws java.lang.IllegalArgumentException if graph already exists in graph catalog
     */
    default void ensureGraphDoesNotExist(User user, DatabaseId databaseId, GraphName graphName) {
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
    default void ensureGraphExists(User user, DatabaseId databaseId, GraphName graphName) {
        if (!graphExists(user, databaseId, graphName)) {
            String message = formatWithLocale(
                "The graph '%s' does not exist.",
                graphName
            );
            throw new IllegalArgumentException(message);
        }
    }

    static Collection<NodeLabel> resolveNodeLabels(GraphStore graphStore,Collection<NodeLabel>  nodeLabelsFilter) {
        return  nodeLabelsFilter.isEmpty()
            ? graphStore.nodeLabels()
            : nodeLabelsFilter;
    }
    static Collection<RelationshipType> resolveRelationshipTypes(GraphStore graphStore, boolean loadAllRelationshipTypes, Collection<RelationshipType> relationshipTypesFilter) {

        return loadAllRelationshipTypes
            ? graphStore.relationshipTypes()
            : relationshipTypesFilter;
    }
}

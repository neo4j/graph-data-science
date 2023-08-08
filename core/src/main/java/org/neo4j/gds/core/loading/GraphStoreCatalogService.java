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

import org.apache.commons.lang3.tuple.Pair;
import org.neo4j.gds.api.DatabaseId;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.GraphName;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.api.User;
import org.neo4j.gds.config.AlgoBaseConfig;
import org.neo4j.gds.config.GraphProjectConfig;

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

    public GraphStoreWithConfig removeGraph(
        CatalogRequest request,
        GraphName graphName,
        boolean shouldFailIfMissing
    ) {
        var result = new AtomicReference<GraphStoreWithConfig>();
        GraphStoreCatalog.remove(
            request,
            graphName.getValue(),
            result::set,
            shouldFailIfMissing
        );
        return result.get();
    }

    public GraphStoreWithConfig get(CatalogRequest catalogRequest, GraphName graphName) {
        return GraphStoreCatalog.get(catalogRequest, graphName.getValue());
    }

    public Pair<Graph, GraphStore> getGraphWithGraphStore(
        GraphName graphName,
        AlgoBaseConfig config,
        Optional<String> relationshipProperty,
        User user,
        DatabaseId databaseId
    ) {
        CatalogRequest catalogRequest = CatalogRequest.of(user, databaseId, config.usernameOverride());
        var graphStoreWithConfig = get(catalogRequest, graphName);
        var graphStore = graphStoreWithConfig.graphStore();
        // TODO: Maybe validation of the graph store, where do this happen? Is this the right place?

        var relationshipTypes = config.relationshipTypesFilter();
        var nodeLabels = config.nodeLabelsFilter();

        // if the relationship types are empty we are getting a graph without relationships which is not what we actually want...
        if (relationshipTypes.isEmpty()) {
            relationshipTypes = graphStore.relationshipTypes();
        }

        //if nodeLabels is empty, we are not getting any node properties
        if (nodeLabels.isEmpty()) {
            nodeLabels = graphStore.nodeLabels();
        }

        var graph = graphStore.getGraph(nodeLabels, relationshipTypes, relationshipProperty);
        return Pair.of(graph, graphStore);
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
        if (! graphExists(user, databaseId, graphName)) {
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

    public Stream<GraphStoreCatalog.GraphStoreWithUserNameAndConfig> getAllGraphStores() {
        return GraphStoreCatalog.getAllGraphStores();
    }

    public Map<GraphProjectConfig, GraphStore> getGraphStores(User user) {
        return GraphStoreCatalog.getGraphStores(user.getUsername());
    }

    public void set(GraphProjectConfig configuration, GraphStore graphStore) {
        GraphStoreCatalog.set(configuration, graphStore);
    }
}

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
package org.neo4j.gds.core.loading.validation;

import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.GraphStore;

import java.util.Collection;
import java.util.List;
import java.util.Optional;

/**
 * When loading a graph from the catalogue, we need some contextual validation/ ETL.
 * There are three steps, as described in {@link org.neo4j.gds.core.loading.GraphStoreCatalogService#loadGraphResources}.
 * These are the corresponding GoF Strategy hooks for that.
 */
public class GraphLoadETL {
    private final List<GraphStoreLoadedETL> graphStoreLoadedHooks;
    private final List<GraphStoreFilteredETL> graphStoreFilteredHooks;
    private final List<GraphLoadedETL> graphLoadedHooks;

    GraphLoadETL(
        List<GraphStoreLoadedETL> graphStoreLoadedHooks,
        List<GraphStoreFilteredETL> graphStoreFilteredHooks,
        List<GraphLoadedETL> graphLoadedHooks
    ) {
        this.graphStoreLoadedHooks = graphStoreLoadedHooks;
        this.graphStoreFilteredHooks = graphStoreFilteredHooks;
        this.graphLoadedHooks = graphLoadedHooks;
    }

    public void onGraphStoreLoaded(GraphStore graphStore) {
        graphStoreLoadedHooks.forEach(hook -> hook.onGraphStoreLoaded(graphStore));
    }

    public void onGraphStoreFiltered(
        GraphStore graphStore,
        Collection<NodeLabel> selectedLabels,
        Collection<RelationshipType> selectedRelationshipTypes,
        Optional<String> relationshipProperty
    ) {
        graphStoreFilteredHooks.forEach(hook -> hook.onGraphStoreFiltered(
            graphStore,
            selectedLabels,
            selectedRelationshipTypes,
            relationshipProperty
        ));
    }

    public void onGraphLoaded(Graph graph) {
        graphLoadedHooks.forEach(hook -> hook.onGraphLoaded(graph));
    }
}

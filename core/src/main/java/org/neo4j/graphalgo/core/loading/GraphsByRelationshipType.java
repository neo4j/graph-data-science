/*
 * Copyright (c) 2017-2019 "Neo4j,"
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

package org.neo4j.graphalgo.core.loading;

import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.huge.HugeGraph;
import org.neo4j.graphalgo.core.huge.UnionGraph;
import org.neo4j.graphalgo.core.utils.RelationshipTypes;
import org.neo4j.helpers.collection.Iterables;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;

public interface GraphsByRelationshipType {

    static GraphsByRelationshipType of(Graph graph) {
        return new NoRelationshipType(graph);
    }

    static GraphsByRelationshipType of(Map<String, Map<String, Graph>> graphs) {
        if (graphs.size() == 1) {
            Map<String, ? extends Graph> byProperty = Iterables.single(graphs.values());
            if (byProperty.size() == 1) {
                return of(Iterables.single(byProperty.values()));
            }
        }
        return new MultipleRelationshipTypes(graphs);
    }

    default Graph getGraph(String relationshipType) {
        return getGraph(relationshipType, Optional.empty());
    }

    Graph getGraph(String relationshipType, Optional<String> maybeRelationshipProperty);

    Graph getUnion();

    String getGraphType();

    void canRelease(boolean canRelease);

    long nodeCount();

    long relationshipCount();

    Set<String> availableRelationshipTypes();

    final class NoRelationshipType implements GraphsByRelationshipType {

        private final Graph graph;

        private NoRelationshipType(Graph graph) {
            this.graph = graph;
        }

        @Override
        public Graph getGraph(String relationshipType, Optional<String> maybeRelationshipProperty) {
            return graph;
        }

        @Override
        public Graph getUnion() {
            return graph;
        }

        @Override
        public void canRelease(boolean canRelease) {
            graph.canRelease(canRelease);
        }

        @Override
        public String getGraphType() {
            return graph.getType();
        }

        @Override
        public long nodeCount() {
            return graph.nodeCount();
        }

        @Override
        public long relationshipCount() {
            return graph.relationshipCount();
        }

        @Override
        public Set<String> availableRelationshipTypes() {
            return Collections.emptySet();
        }
    }

    final class MultipleRelationshipTypes implements GraphsByRelationshipType {

        private final Map<String, Map<String, Graph>> graphs;

        private MultipleRelationshipTypes(Map<String, Map<String, Graph>> graphs) {
            this.graphs = graphs;
            forEach(g -> g.canRelease(false));
        }

        @Override
        public Graph getGraph(String relationshipType, Optional<String> maybeRelationshipProperty) {
            Set<String> types = RelationshipTypes.parse(relationshipType);

            if (types.isEmpty() && !maybeRelationshipProperty.isPresent()) {
                return getUnion();
            }

            Collection<Graph> graphParts = new ArrayList<>();
            if (types.isEmpty()) {
                String weightProperty = maybeRelationshipProperty.get();
                for (Map<String, ? extends Graph> graphsByProperty : graphs.values()) {
                    Graph graph = getExistingByProperty(weightProperty, graphsByProperty);
                    graphParts.add(graph);
                }
            } else {
                if (maybeRelationshipProperty.isPresent()) {
                    String weightProperty = maybeRelationshipProperty.get();
                    for (String type : types) {
                        Map<String, ? extends Graph> graphsByProperty = getExistingByType(type);
                        Graph graph = getExistingByProperty(weightProperty, graphsByProperty);
                        graphParts.add(graph);
                    }
                } else {
                    for (String type : types) {
                        Map<String, ? extends Graph> graphsByProperty = getExistingByType(type);
                        graphParts.addAll(graphsByProperty.values());
                    }
                }
            }

            return UnionGraph.of(graphParts);
        }

        @Override
        public Graph getUnion() {
            Collection<Graph> graphParts = new ArrayList<>();
            forEach(graphParts::add);
            return UnionGraph.of(graphParts);
        }

        private Map<String, ? extends Graph> getExistingByType(String singleType) {
            return getExisting(singleType, "type", graphs);
        }

        private Graph getExistingByProperty(String property, Map<String, ? extends Graph> graphs) {
            return getExisting(property, "property", graphs);
        }

        private <T> T getExisting(String key, String type, Map<String, ? extends T> graphs) {
            T graph = graphs.get(key);
            if (graph == null) {
                throw new IllegalArgumentException(String.format("No graph was loaded for %s %s", type, key));
            }
            return graph;
        }

        @Override
        public void canRelease(boolean canRelease) {
            forEach(g -> g.canRelease(canRelease));
        }

        @Override
        public String getGraphType() {
            return HugeGraph.TYPE;
        }

        @Override
        public long nodeCount() {
            return graphs
                    .values().stream()
                    .flatMap(g -> g.values().stream())
                    .mapToLong(Graph::nodeCount)
                    .findFirst()
                    .orElse(0);
        }

        @Override
        public long relationshipCount() {
            return graphs
                    .values().stream()
                    .mapToLong(g -> g.values().stream().mapToLong(Graph::relationshipCount).max().orElse(0L))
                    .sum();
        }

        @Override
        public Set<String> availableRelationshipTypes() {
            return graphs.keySet();
        }

        private void forEach(Consumer<? super Graph> action) {
            for (Map<String, ? extends Graph> graphsByProperty : graphs.values()) {
                for (Graph graph : graphsByProperty.values()) {
                    action.accept(graph);
                }
            }
        }
    }
}

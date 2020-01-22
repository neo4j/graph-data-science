/*
 * Copyright (c) 2017-2020 "Neo4j,"
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

import org.jetbrains.annotations.TestOnly;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.ProcedureConstants;
import org.neo4j.graphalgo.core.huge.HugeGraph;
import org.neo4j.graphalgo.core.huge.UnionGraph;
import org.neo4j.graphalgo.core.utils.ProjectionParser;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static org.neo4j.graphalgo.api.GraphFactory.ANY_REL_TYPE;

public final class GraphsByRelationshipType {

    @TestOnly
    public static GraphsByRelationshipType of(Graph graph) {
        Map<String, Map<String, Graph>> mapping = Collections.singletonMap(
            "RELATIONSHIP_TYPE",
            Collections.singletonMap(
                "PROPERTY",
                graph
            )
        );
        return GraphsByRelationshipType.of(mapping);
    }

    public static GraphsByRelationshipType of(Map<String, Map<String, Graph>> graphs) {
        return new GraphsByRelationshipType(graphs);
    }

    private final Map<String, Map<String, Graph>> graphs;

    private GraphsByRelationshipType(Map<String, Map<String, Graph>> graphs) {
        this.graphs = graphs;
    }

    @Deprecated
    public Graph getGraph(String relationshipType) {
        return getGraph(relationshipType, Optional.empty());
    }

    @Deprecated
    public Graph getGraph(String relationshipType, Optional<String> maybeRelationshipProperty) {
        Set<String> types = ProjectionParser.parse(relationshipType);

        ArrayList<String> relationshipTypes = new ArrayList<>(types);
        if (types.isEmpty()) {
            relationshipTypes.add("*");
        }
        return getGraphProjection(relationshipTypes, maybeRelationshipProperty);
    }

    public Graph getGraphProjection(String relationshipType) {
        return getGraphProjection(Collections.singletonList(relationshipType), Optional.empty());
    }

    public Graph getGraphProjection(List<String> relationshipTypes, Optional<String> maybeRelationshipProperty) {
        if (relationshipTypes.isEmpty()) {
            throw new IllegalArgumentException(String.format("The parameter %s should not be empty. Use `*` to load all relationship types.",
                ProcedureConstants.RELATIONSHIP_TYPES
            ));
        }

        Map<String, Map<String, Graph>> graphsWithRelTypes = relationshipTypes.contains("*") ?
            graphs :
            graphs.entrySet()
                .stream()
                .filter(entry -> relationshipTypes.contains(entry.getKey()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        List<Graph> filteredGraphs = graphsWithRelTypes.values().stream().map(graphsByProperty -> {
            if (maybeRelationshipProperty.isPresent()) {
                return getExistingByProperty(maybeRelationshipProperty.get(), graphsByProperty);
            } else {
                if (graphsByProperty.containsKey(ANY_REL_TYPE)) {
                    return graphsByProperty.get(ANY_REL_TYPE);
                } else {
                    return graphsByProperty
                        .get(graphsByProperty.keySet().iterator().next())
                        .withoutProperties();
                }
            }
        }).collect(Collectors.toList());

        if (filteredGraphs.isEmpty()) {
            throw new NoSuchElementException(String.format(
                "Cannot find graphs for relationship types: '%s' and relationship properties '%s'.",
                relationshipTypes, maybeRelationshipProperty.orElse("<NOT DEFINED>")
            ));
        } else {
            return UnionGraph.of(filteredGraphs);
        }
    }

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

    public void canRelease(boolean canRelease) {
        forEach(g -> g.canRelease(canRelease));
    }

    public String getGraphType() {
        return HugeGraph.TYPE;
    }

    public long nodeCount() {
        return graphs
                .values().stream()
                .flatMap(g -> g.values().stream())
                .mapToLong(Graph::nodeCount)
                .findFirst()
                .orElse(0);
    }

    public long relationshipCount() {
        return graphs
                .values().stream()
                .mapToLong(g -> g.values().stream().mapToLong(Graph::relationshipCount).max().orElse(0L))
                .sum();
    }

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


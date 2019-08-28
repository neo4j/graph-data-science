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

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public final class GraphsByRelationshipType implements GraphByType {

    public static final String ALL_IDENTIFIER = "  __ALL__  ";

    private final Map<String, HugeGraph> graphs;

    public GraphsByRelationshipType(Map<String, HugeGraph> graphs) {
        this.graphs = graphs;
        for (Graph graph : graphs.values()) {
            graph.canRelease(false);
        }
    }

    @Override
    public Graph loadGraph(String relationship) {
        Set<String> types = RelationshipTypes.parse(relationship);
        if (types.isEmpty() || ALL_IDENTIFIER.equals(relationship)) {
            return new UnionGraph(graphs.values());
        }
        if (types.size() == 1) {
            return getExisting(Iterables.single(types));
        }
        List<HugeGraph> graphs = types.stream().map(this::getExisting).collect(Collectors.toList());
        return new UnionGraph(graphs);
    }

    private HugeGraph getExisting(String singleType) {
        HugeGraph graph = this.graphs.get(singleType);
        if (graph == null) {
            throw new IllegalArgumentException("No graph was loaded for " + singleType);
        }
        return graph;
    }

    @Override
    public void canRelease(boolean canRelease) {
        for (HugeGraph graph : graphs.values()) {
            graph.canRelease(canRelease);
        }
    }

    @Override
    public void release() {
        for (Graph graph : graphs.values()) {
            graph.canRelease(true);
            graph.release();
        }
        graphs.clear();
    }

    @Override
    public String getType() {
        return HugeGraph.TYPE;
    }

    public long nodeCount() {
        return graphs.values().stream().mapToLong(Graph::nodeCount).findFirst().orElse(0);
    }

    public long relationshipCount() {
        return graphs.values().stream().mapToLong(Graph::relationshipCount).sum();
    }

    public Set<String> availableGraphs() {
        return graphs.keySet();
    }
}

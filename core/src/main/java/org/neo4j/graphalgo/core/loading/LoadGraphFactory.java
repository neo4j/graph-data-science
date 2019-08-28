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
import org.neo4j.graphalgo.api.GraphFactory;
import org.neo4j.graphalgo.api.GraphSetup;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraphFactory;
import org.neo4j.graphalgo.core.huge.HugeGraph;
import org.neo4j.graphalgo.core.huge.loader.HugeGraphFactory;
import org.neo4j.graphalgo.core.utils.mem.MemoryEstimation;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static org.neo4j.graphalgo.core.loading.GraphsByRelationshipType.ALL_IDENTIFIER;

public final class LoadGraphFactory extends GraphFactory {

    private static final ConcurrentHashMap<String, GraphByType> graphs = new ConcurrentHashMap<>();

    public LoadGraphFactory(
            final GraphDatabaseAPI api,
            final GraphSetup setup) {
        super(api, setup);
    }

    @Override
    protected Graph importGraph() {
        return get(setup.name, setup.relationshipType);
    }

    @Override
    public Graph build() {
        return importGraph();
    }

    public final MemoryEstimation memoryEstimation() {
        Graph graph = get(setup.name, setup.relationshipType);
        dimensions.nodeCount(graph.nodeCount());
        dimensions.maxRelCount(graph.relationshipCount());

        if (HugeGraph.TYPE.equals(graph.getType())) {
            return HugeGraphFactory.getMemoryEstimation(setup, dimensions);
        } else {
            return HeavyGraphFactory.getMemoryEstimation(setup, dimensions);
        }
    }

    public static void set(String name, GraphByType graph) {
        if (name == null || graph == null) {
            throw new IllegalArgumentException("Both name and graph must be not null");
        }
        if (graphs.putIfAbsent(name, graph) != null) {
            throw new IllegalStateException("Graph name " + name + " already loaded");
        }
        graph.canRelease(false);
    }

    public static Graph get(String name, String relationshipType) {
        if (name == null) {
            return null;
        }
        GraphByType graphsByRelationshipType = graphs.get(name);
        if (graphsByRelationshipType == null) {
            return null;
        }
        return graphsByRelationshipType.loadGraph(relationshipType);
    }

    public static boolean exists(String name) {
        return name != null && graphs.containsKey(name);
    }

    public static Graph remove(String name) {
        if (!exists(name)) {
            return null;
        }
        Graph graph = graphs.remove(name).loadGraph(ALL_IDENTIFIER);
        graph.canRelease(true);
        graph.release();
        return graph;
    }

    public static String getType(String name) {
        if (name == null) return null;
        GraphByType graph = graphs.get(name);
        return graph == null ? null : graph.getType();
    }

    public static Map<String, Graph> getLoadedGraphs() {
        return graphs.entrySet().stream().collect(Collectors.toMap(
                Map.Entry::getKey,
                e -> e.getValue().loadGraph(ALL_IDENTIFIER)
        ));
    }
}

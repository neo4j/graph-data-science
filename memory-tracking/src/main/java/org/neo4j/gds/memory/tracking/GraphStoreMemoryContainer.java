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
package org.neo4j.gds.memory.tracking;

import org.neo4j.gds.api.graph.store.catalog.GraphStoreAddedEvent;
import org.neo4j.gds.api.graph.store.catalog.GraphStoreRemovedEvent;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

class GraphStoreMemoryContainer {
    private final Map<String /* username */, ConcurrentHashMap<String /* graph name */, Long>> graphStoresMemory = new ConcurrentHashMap<>();
    private final AtomicLong graphStoreReservedMemory = new AtomicLong();

    long addGraph(GraphStoreAddedEvent graphStoreAddedEvent) {
        var addedGraphMemory = graphStoreAddedEvent.memoryInBytes();
        var graphsMemory = graphStoreReservedMemory.addAndGet(addedGraphMemory);
        graphStoresMemory.putIfAbsent(graphStoreAddedEvent.user(), new ConcurrentHashMap<>());
        graphStoresMemory.get(graphStoreAddedEvent.user()).put(
            graphStoreAddedEvent.graphName(),
            graphStoreAddedEvent.memoryInBytes()
        );
        return graphsMemory;
    }

    long removeGraph(GraphStoreRemovedEvent graphStoreRemovedEvent) {
        var username = graphStoreRemovedEvent.user();
        var graphMemoryToRemove = graphStoresMemory.get(username).remove(graphStoreRemovedEvent.graphName());
        if (graphMemoryToRemove == null) {
            return graphStoreReservedMemory.get();
        }
        return graphStoreReservedMemory.addAndGet(-graphMemoryToRemove);
    }

    long graphStoreReservedMemory() {
        return graphStoreReservedMemory.get();
    }

    Stream<UserEntityMemory> listGraphs(String username) {
        return graphStoresMemory
            .getOrDefault(username, new ConcurrentHashMap<>())
            .entrySet()
            .stream()
            .map(entry -> UserEntityMemory.createGraph(username, entry.getKey(), entry.getValue()));
    }

    Stream<UserEntityMemory> listGraphs() {
        return graphStoresMemory.keySet().stream().flatMap(this::listGraphs);
    }

    long memoryOfGraphs(String username) {
        return graphStoresMemory
            .getOrDefault(username, new ConcurrentHashMap<>())
            .values()
            .stream()
            .reduce(0L, Long::sum);
    }

    Set<String> graphUsers() {
        return graphStoresMemory.keySet();
    }
}

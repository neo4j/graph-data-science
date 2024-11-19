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
package org.neo4j.gds.mem;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.api.graph.store.catalog.GraphStoreAddedEvent;
import org.neo4j.gds.api.graph.store.catalog.GraphStoreRemovedEvent;

import static org.assertj.core.api.Assertions.assertThat;

class GraphStoreMemoryContainerTest {

    @Test
    void shouldAddGraphs(){
         GraphStoreMemoryContainer graphStoreMemoryContainer=new GraphStoreMemoryContainer();
         assertThat(graphStoreMemoryContainer.addGraph(new GraphStoreAddedEvent("foo","DB","buzz",10))).isEqualTo(10);
         assertThat(graphStoreMemoryContainer.addGraph(new GraphStoreAddedEvent("foo","DB","buzz2",20))).isEqualTo(30);
         assertThat(graphStoreMemoryContainer.graphStoreReservedMemory()).isEqualTo(30L);

    }

    @Test
    void shouldRemoveGraphs(){
        GraphStoreMemoryContainer graphStoreMemoryContainer=new GraphStoreMemoryContainer();
        graphStoreMemoryContainer.addGraph(new GraphStoreAddedEvent("foo","DB","buzz",10));
        graphStoreMemoryContainer.addGraph(new GraphStoreAddedEvent("foo","DB","buzz2",20));
        assertThat(graphStoreMemoryContainer.removeGraph(new GraphStoreRemovedEvent("foo","DB","buzz2",20))).isEqualTo(10);
    }

    @Test
    void shouldListForUser(){
        GraphStoreMemoryContainer graphStoreMemoryContainer=new GraphStoreMemoryContainer();
        graphStoreMemoryContainer.addGraph(new GraphStoreAddedEvent("Alice","DB","graph1",10));
        graphStoreMemoryContainer.addGraph(new GraphStoreAddedEvent("Alice","DB","graph2",15));

        graphStoreMemoryContainer.addGraph(new GraphStoreAddedEvent("Bob","DB","graph3",20));
        var aliceList =graphStoreMemoryContainer.listGraphs("Alice").toList();
        assertThat(aliceList).hasSize(2);
        assertThat(aliceList.stream().map(UserEntityMemory::graph).toList()).containsExactlyInAnyOrder("graph1","graph2");
        assertThat(aliceList.stream().map(UserEntityMemory::memoryInBytes).toList()).containsExactlyInAnyOrder(10L,15L);

    }

    @Test
    void shouldListAll(){
        GraphStoreMemoryContainer graphStoreMemoryContainer=new GraphStoreMemoryContainer();
        graphStoreMemoryContainer.addGraph(new GraphStoreAddedEvent("Alice","DB","graph1",10));
        graphStoreMemoryContainer.addGraph(new GraphStoreAddedEvent("Alice","DB","graph2",15));

        graphStoreMemoryContainer.addGraph(new GraphStoreAddedEvent("Bob","DB","graph3",20));
        var graphList =graphStoreMemoryContainer.listGraphs().toList();
        assertThat(graphList).hasSize(3);
        assertThat(graphList.stream().map(UserEntityMemory::graph).toList()).containsExactlyInAnyOrder("graph1","graph2","graph3");
        assertThat(graphList.stream().map(UserEntityMemory::memoryInBytes).toList()).containsExactlyInAnyOrder(10L,15L,20L);

    }

}

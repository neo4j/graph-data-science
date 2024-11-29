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
package org.neo4j.gds.procedures.memory;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.api.User;
import org.neo4j.gds.mem.MemoryTracker;
import org.neo4j.gds.mem.UserEntityMemory;
import org.neo4j.gds.mem.UserMemorySummary;

import java.util.stream.Stream;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class MemoryFacadeTest {

    @Test
    void shouldListAsAdmin(){
        var memTrackerMock = mock(MemoryTracker.class);
        var entityOfa = new UserEntityMemory("a", "graph1", "graph",10);
        var entityOfb = new UserEntityMemory("b", "graph2", "graph",20);
        when(memTrackerMock.listAll())
            .thenReturn(Stream.of(
                entityOfa,
                entityOfb
            ));

        var userMock  = mock(User.class);
        when(userMock.getUsername()).thenReturn("a");
        when(userMock.isAdmin()).thenReturn(true);

        var facade =new MemoryFacade(  userMock,memTrackerMock);
        var list = facade.list().toList();

        assertThat(list.toArray()).containsExactly(
            entityOfa,
            entityOfb
        );
    }


    @Test
    void shouldSummarizeAsAdmin(){
        var memTrackerMock = mock(MemoryTracker.class);
        var summaryOfa = new UserMemorySummary("a", 1, 1);
        var summaryOfb = new UserMemorySummary("b", 2, 2);
        when(memTrackerMock.memorySummary())
            .thenReturn(Stream.of(
                summaryOfa,
                summaryOfb
            ));

        var userMock  = mock(User.class);
        when(userMock.getUsername()).thenReturn("a");
        when(userMock.isAdmin()).thenReturn(true);

        var facade =new MemoryFacade(  userMock,memTrackerMock);
        var list = facade.memorySummary().toList();

        assertThat(list.toArray()).containsExactly(
            summaryOfa,
            summaryOfb
        );
    }

}

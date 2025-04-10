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
package org.neo4j.gds.traversal;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.core.utils.progress.tasks.LeafTask;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class RandomWalkCountingNodeVisitsProgressTaskFactoryTest {

    @Test
    void shouldHaveDegreeCentralitySubTaskWhenTheGraphHasRelationshipProperty() {

        var graphMock = mock(Graph.class);
        when(graphMock.hasRelationshipProperty()).thenReturn(true);
        var task = RandomWalkCountingNodeVisitsProgressTaskFactory.create(graphMock);

        assertThat(task).isNotInstanceOf(LeafTask.class);

        assertThat(task.subTasks())
            .isNotEmpty()
            .hasSize(1)
            .satisfiesExactly(subTask -> assertThat(subTask.description()).isEqualTo("DegreeCentrality"));
    }

    @Test
    void shouldNotHaveDegreeCentralitySubTaskWhenTheGraphDoesntHaveRelationshipProperty() {

        var graphMock = mock(Graph.class);
        when(graphMock.hasRelationshipProperty()).thenReturn(false);

        var task = RandomWalkCountingNodeVisitsProgressTaskFactory.create(graphMock);

        assertThat(task).isInstanceOf(LeafTask.class);
        assertThat(task.subTasks())
            .isEmpty();

    }

}

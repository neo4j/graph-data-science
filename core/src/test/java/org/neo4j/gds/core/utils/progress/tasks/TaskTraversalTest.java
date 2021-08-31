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
package org.neo4j.gds.core.utils.progress.tasks;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class TaskTraversalTest {

    @Test
    void shouldTraverseWithDepthInformation() {
        var task = Tasks.task("root", Tasks.task("node", Tasks.leaf("leaf2")), Tasks.leaf("leaf1"));
        var depthCollectingVisitor = new DepthCollectingVisitorVisitor();
        TaskTraversal.visitPreOrderWithDepth(task, depthCollectingVisitor);

        var actual = depthCollectingVisitor.depthInformation;
        var expected = List.of(0, 1, 2, 1);
        assertThat(actual).containsExactlyElementsOf(expected);
    }

    static class DepthCollectingVisitorVisitor extends DepthAwareTaskVisitor {

        List<Integer> depthInformation = new ArrayList<>();

        @Override
        public void visit(Task task) {
            depthInformation.add(depth());
        }
    }

}

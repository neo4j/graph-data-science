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
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.logging.Log;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThatNoException;

class TaskProgressLoggerTest {

    @Test
    void shouldNotEliminateParentTaskIfCommonPreffix(){

        var taskA = Tasks.leaf("A");
        var taskAB = Tasks.task("A B", List.of(taskA));
        var task = Tasks.task("T", List.of(taskA));

        var logger =new TaskProgressLogger(Log.noOpLog(),task,new Concurrency(1));

        assertThatNoException().isThrownBy(
            ()-> {
                logger.startSubTask("T");
                logger.startSubTask("A B");
                logger.startSubTask("A ");
                logger.logEndSubTask(taskA, taskAB);
                logger.logEndSubTask(taskAB, task);
                logger.finishSubTask("T");
            }
        );

    }

}

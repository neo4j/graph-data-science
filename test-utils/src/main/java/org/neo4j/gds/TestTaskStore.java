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
package org.neo4j.gds;

import org.neo4j.gds.core.utils.progress.GlobalTaskStore;
import org.neo4j.gds.core.utils.progress.JobId;
import org.neo4j.gds.core.utils.progress.tasks.Task;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TestTaskStore extends GlobalTaskStore {

    private Map<JobId, String> tasks = new HashMap<>();
    private List<String> tasksSeen = new ArrayList<>();

    @Override
    public void store(String username, JobId jobId, Task task) {
        super.store(username, jobId, task);

        tasks.put(jobId, task.description());
        tasksSeen.add(task.description());
    }

    @Override
    public void remove(String username, JobId jobId) {
        tasks.remove(jobId);
    }

    public List<String> tasksSeen() {
        return tasksSeen;
    }

    public Map<JobId, String> tasks() {
        return tasks;
    }
}

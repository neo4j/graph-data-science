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

import com.carrotsearch.hppc.ObjectLongHashMap;
import com.carrotsearch.hppc.ObjectLongMap;
import com.carrotsearch.hppc.procedures.LongProcedure;
import org.neo4j.gds.core.utils.progress.JobId;
import org.neo4j.gds.core.utils.progress.TaskStoreListener;
import org.neo4j.gds.core.utils.progress.UserTask;

import java.util.concurrent.atomic.LongAdder;

import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

public class MemoryTracker implements TaskStoreListener {
    private final long initialMemory;

    private final ObjectLongMap<JobId> memoryInUse = new ObjectLongHashMap<>();

    public MemoryTracker(long initialMemory) {
        assertPositiveInitialMemory(initialMemory);
        this.initialMemory = initialMemory;
    }

    public long initialMemory() {
        return initialMemory;
    }

    public void track(JobId jobId, long memoryEstimate) {
        memoryInUse.put(jobId, memoryEstimate);
    }

    public synchronized long availableMemory() {
        var reservedMemory = new LongAdder();
        memoryInUse.values().forEach((LongProcedure) reservedMemory::add);
        return initialMemory - reservedMemory.longValue();
    }

    private static void assertPositiveInitialMemory(long initialMemory) {
        if (initialMemory < 0) {
            throw new IllegalArgumentException(
                formatWithLocale("Negative values are not allowed. Trying to use: `%s`", initialMemory)
            );
        }
    }

    @Override
    public void onTaskAdded(UserTask userTask) {
        // do nothing, we add the memory explicitly prior to execution
    }

    @Override
    public void onTaskRemoved(UserTask userTask) {
        var jobId = userTask.jobId();
        memoryInUse.remove(jobId);
    }
}

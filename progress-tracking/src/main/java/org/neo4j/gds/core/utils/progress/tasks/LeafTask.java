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

import java.util.List;
import java.util.concurrent.atomic.LongAdder;

public class LeafTask extends Task {

    private long volume;
    private final LongAdder currentProgress;

    LeafTask(String description, long volume) {
        super(description, List.of());
        this.volume = volume;
        this.currentProgress = new LongAdder();
    }

    @Override
    public void finish() {
        super.finish();

        // This task should now be considered to have 100% progress.
        if (volume == UNKNOWN_VOLUME) {
            volume = currentProgress.longValue();
        }
        currentProgress.add(volume - currentProgress.longValue());
    }

    @Override
    public void setVolume(long volume) {
        this.volume = volume;
    }

    @Override
    public void logProgress(long value) {
        currentProgress.add(value);
    }

    @Override
    public Progress getProgress() {
        return ImmutableProgress.of(currentProgress.longValue(), volume);
    }

    @Override
    public void visit(TaskVisitor taskVisitor) {
        taskVisitor.visitLeafTask(this);
    }
}

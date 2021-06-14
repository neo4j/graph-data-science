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

import org.eclipse.collections.api.tuple.Pair;
import org.eclipse.collections.impl.tuple.Tuples;
import org.neo4j.graphalgo.core.utils.progress.ProgressEventTracker;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public final class TestProgressEventTracker implements ProgressEventTracker {
    private int releaseCalls = 0;
    private final List<Pair<String, String>> logEvents = new ArrayList<>();

    @Override
    public void addLogEvent(String taskName, String message) {
        logEvents.add(Tuples.pair(taskName, message));
    }

    @Override
    public void release() {
        releaseCalls++;
    }

    public int releaseCalls() {
        return releaseCalls;
    }

    public List<Pair<String, String>> logEvents() {
        return logEvents;
    }

    public List<String> taskNames() {
        return logEvents.stream().map(Pair::getOne).collect(Collectors.toList());
    }

    public List<String> messages() {
        return logEvents.stream().map(Pair::getTwo).collect(Collectors.toList());
    }
}

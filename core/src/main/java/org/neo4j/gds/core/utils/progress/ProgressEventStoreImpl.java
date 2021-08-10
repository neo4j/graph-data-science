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
package org.neo4j.gds.core.utils.progress;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static java.util.Collections.emptyMap;
import static java.util.function.Predicate.not;
import static java.util.stream.Collectors.toList;

public class ProgressEventStoreImpl implements ProgressEventStore {

    private final Map<String, Map<JobId, List<LogEvent>>> events;

    ProgressEventStoreImpl() {
        this.events = new HashMap<>();
    }

    @Override
    public List<LogEvent> query(String username) {
        return events
            .getOrDefault(username, emptyMap())
            .values()
            .stream()
            .filter(not(List::isEmpty))
            .map(items -> items.get(items.size() - 1))
            .collect(toList());
    }

    @Override
    public void accept(LogEvent event) {
        if (event.isEndOfStream()) {
            if (events.containsKey(event.username())) {
                events.get(event.username()).remove(event.jobId());
            }
        } else {
            events
                .computeIfAbsent(event.username(), __ -> new ConcurrentHashMap<>())
                .computeIfAbsent(event.jobId(), __ -> new ArrayList<>())
                .add(event);
        }
    }

    @Override
    public boolean isEmpty() {
        return events
            .values()
            .stream()
            .flatMap(it -> it.values().stream())
            .allMatch(List::isEmpty);
    }
}

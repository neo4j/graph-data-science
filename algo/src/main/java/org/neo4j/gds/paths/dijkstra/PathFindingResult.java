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
package org.neo4j.gds.paths.dijkstra;

import org.neo4j.gds.paths.PathResult;

import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class PathFindingResult {
    private final Stream<PathResult> paths;

    private final Runnable closeStreamAction;

    private final AtomicBoolean consumptionTriggered;

    public PathFindingResult(Stream<PathResult> paths) {
        this(paths, () -> {});
    }

    public PathFindingResult(Stream<PathResult> paths, Runnable closeStreamAction) {
        this.paths = paths;
        this.closeStreamAction = closeStreamAction;
        this.consumptionTriggered = new AtomicBoolean(false);
    }

    public Optional<PathResult> findFirst() {
        var first = paths.findFirst();
        runConsumptionAction();
        return first;
    }

    public void forEachPath(Consumer<PathResult> resultConsumer) {
        paths.forEach(resultConsumer);
        runConsumptionAction();
    }

    public <T> Stream<T> mapPaths(Function<PathResult, T> fn) {
        return paths.map(fn).onClose(this::runConsumptionAction);
    }

    public Set<PathResult> pathSet() {
        var resultSet = paths.collect(Collectors.toSet());
        runConsumptionAction();
        return resultSet;
    }

    private void runConsumptionAction() {
        if (consumptionTriggered.compareAndSet(false, true)) {
            closeStreamAction.run();
        }
    }
}

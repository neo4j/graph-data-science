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
package org.neo4j.gds.community;

import org.neo4j.gds.async.AsyncAlgorithmCaller;
import org.neo4j.gds.progress.tracking.ProgressTrackerFactory;
import org.neo4j.gds.termination.TerminationFlag;

import java.util.concurrent.CompletableFuture;

public class CommunityComputeFacade {
    // Global dependencies
    // This is created with its own ExecutorService workerPool,
    // which determines how many algorithms can run in parallel.
    private final AsyncAlgorithmCaller algorithmCaller;
    private final ProgressTrackerFactory progressTrackerFactory;

    // Request scope dependencies
    private final TerminationFlag terminationFlag;

    public CommunityComputeFacade(
        AsyncAlgorithmCaller algorithmCaller,
        ProgressTrackerFactory progressTrackerFactory,
        TerminationFlag terminationFlag
    ) {
        this.algorithmCaller = algorithmCaller;
        this.progressTrackerFactory = progressTrackerFactory;
        this.terminationFlag = terminationFlag;
    }
}

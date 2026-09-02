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
package org.neo4j.gds.applications.algorithms.execution;

import org.neo4j.gds.logging.Log;

import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

/**
 * This is just a small utility that does sync -> async with some logging.
 * Verbiage is _work_, because it is an algorithm run, plus any side effects, plus result rendering.
 */
public class CompletionConvenience {
    private final Log log;

    public CompletionConvenience(Log log) {
        this.log = log;
    }

    /**
     * Block and wait for work to complete, log any errors encountered (NB including the cause bit), propagate errors.
     */
    public <RESULT> RESULT completeWork(CompletableFuture<RESULT> launchedWork) {
        try {
            return launchedWork.join();
        } catch (CancellationException e) {
            log.error("your work was cancelled", e);
            throw e;
        } catch (CompletionException e) {
            log.error("execution error, something went wrong while executing your work", e.getCause());
            throw e;
        }
    }
}

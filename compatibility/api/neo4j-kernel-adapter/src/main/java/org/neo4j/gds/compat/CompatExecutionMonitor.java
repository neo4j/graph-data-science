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
package org.neo4j.gds.compat;

import org.neo4j.common.DependencyResolver;
import org.neo4j.internal.batchimport.staging.StageExecution;

import java.time.Clock;

/**
 * Gets notified now and then about {@link org.neo4j.internal.batchimport.staging.StageExecution}, where statistics can be read and displayed,
 * aggregated or in other ways make sense of the data of {@link org.neo4j.internal.batchimport.staging.StageExecution}.
 */
public interface CompatExecutionMonitor {
    /**
     * Signals start of import. Called only once and before any other method.
     *
     * @param dependencyResolver {@link org.neo4j.common.DependencyResolver} for getting dependencies from.
     */
    void initialize(DependencyResolver dependencyResolver);

    /**
     * Signals the start of a {@link org.neo4j.internal.batchimport.staging.StageExecution}.
     */
    void start(StageExecution execution);

    /**
     * Signals the end of the execution previously {@link #start(StageExecution) started}.
     */
    void end(StageExecution execution, long totalTimeMillis);

    /**
     * Called after all {@link StageExecution stage executions} have run.
     */
    void done(boolean successful, long totalTimeMillis, String additionalInformation);

    /**
     * Called periodically while executing a {@link StageExecution}.
     */
    void check(StageExecution execution);

    /**
     * Clock to use for interval checks
     */
    Clock clock();

    /**
     * @return rough time interval this monitor needs checking.
     */
    long checkIntervalMillis();
}

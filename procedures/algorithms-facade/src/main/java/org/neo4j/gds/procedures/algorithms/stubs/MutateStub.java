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
package org.neo4j.gds.procedures.algorithms.stubs;

import org.neo4j.gds.mem.MemoryEstimation;
import org.neo4j.gds.applications.algorithms.machinery.MemoryEstimateResult;

import java.util.Map;
import java.util.stream.Stream;

/**
 * Mutate mode is used from Neo4j Procedures as well as from pipelines, so there is variation in the logic we need.
 */
public interface MutateStub<CONFIGURATION, RESULT> {
    /**
     * For pipelines, when adding steps, we need to validate them without considering the current user.
     * So we effectively get validation against global defaults and limits.
     */
    void validateConfiguration(Map<String, Object> configuration);

    /**
     * Bog-standard configuration parsing with implicit validation,
     * using the current user and globally configured defaults and limits
     */
    CONFIGURATION parseConfiguration(Map<String, Object> configuration);

    /**
     * We need this directly for pipelines,
     * and also for procedure calls where we enrich and transform before returning results.
     * This captures the correct (delegation of) business logic.
     */
    MemoryEstimation getMemoryEstimation(String username, Map<String, Object> configuration);

    /**
     * Plain old Neo4j Procedure style memory estimate mode
     */
    Stream<MemoryEstimateResult> estimate(Object graphName, Map<String, Object> configuration);

    /**
     * Bog-standard execution of the algorithm in mutate mode, with ordinary input validation, defaults application,
     * limit checks, memory guards etc.
     */
    Stream<RESULT> execute(String graphName, Map<String, Object> configuration);
}

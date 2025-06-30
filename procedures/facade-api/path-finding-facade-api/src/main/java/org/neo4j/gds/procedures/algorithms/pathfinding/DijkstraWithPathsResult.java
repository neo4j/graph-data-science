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
package org.neo4j.gds.procedures.algorithms.pathfinding;

import org.neo4j.gds.applications.algorithms.machinery.AlgorithmProcessingTimings;
import org.neo4j.gds.procedures.algorithms.results.MutateResult;

import java.util.Map;

public class DijkstraWithPathsResult implements MutateResult {

    public final long preProcessingMillis;
    public final long computeMillis;
    public final long mutateMillis;
    public final long postProcessingMillis;
    public final Map<String, Object> configuration;

    public DijkstraWithPathsResult(
        long preProcessingMillis,
        long computeMillis,
        long mutateMillis,
        long postProcessingMillis,
        Map<String, Object> configuration
    ) {
        this.preProcessingMillis = preProcessingMillis;
        this.computeMillis = computeMillis;
        this.mutateMillis = mutateMillis;
        this.postProcessingMillis = postProcessingMillis;
        this.configuration = configuration;
    }

    public static DijkstraWithPathsResult create(
        AlgorithmProcessingTimings timings,
        Map<String, Object> configuration
    ){
        return new DijkstraWithPathsResult(
            timings.preProcessingMillis,
            timings.computeMillis,
            timings.sideEffectMillis,
            0L,
            configuration
        );
    }

    //add getters to maintain the interface
    @Override
    public long mutateMillis() {
        return mutateMillis;
    }

    @Override
    public long postProcessingMillis() {
        return postProcessingMillis;
    }

    @Override
    public long preProcessingMillis() {
        return preProcessingMillis;
    }

    @Override
    public long computeMillis() {
        return computeMillis;
    }

    @Override
    public Map<String, Object> configuration() {
        return configuration;
    }
}

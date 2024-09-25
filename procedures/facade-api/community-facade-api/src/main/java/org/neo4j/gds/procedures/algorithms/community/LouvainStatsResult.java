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
package org.neo4j.gds.procedures.algorithms.community;

import org.neo4j.gds.applications.algorithms.machinery.AlgorithmProcessingTimings;
import org.neo4j.gds.procedures.algorithms.results.StandardStatsResult;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class LouvainStatsResult extends StandardStatsResult {
    public final double modularity;
    public final List<Double> modularities;
    public final long ranLevels;
    public final long communityCount;
    public final Map<String, Object> communityDistribution;

    public LouvainStatsResult(
        double modularity,
        List<Double> modularities,
        long ranLevels,
        long communityCount,
        Map<String, Object> communityDistribution,
        long preProcessingMillis,
        long computeMillis,
        long postProcessingMillis,
        Map<String, Object> configuration
    ) {
        super(preProcessingMillis, computeMillis, postProcessingMillis, configuration);
        this.modularity = modularity;
        this.modularities = modularities;
        this.ranLevels = ranLevels;
        this.communityCount = communityCount;
        this.communityDistribution = communityDistribution;
    }

    static LouvainStatsResult emptyFrom(AlgorithmProcessingTimings timings, Map<String, Object> configurationMap) {
        return new LouvainStatsResult(
            0,
            Collections.emptyList(),
            0,
            0,
            Collections.emptyMap(),
            timings.preProcessingMillis,
            timings.computeMillis,
            0,
            configurationMap
        );
    }

    static LouvainStatsResult create(
        double modularity,
        double[] modularities,
        int ranLevels,
        long communityCount,
        Map<String, Object> communityDistribution,
        long preProcessingMillis,
        long computeMillis,
        long postProcessingMillis,
        Map<String, Object> configurationMap
    ) {
        var modularitiesAsList = Arrays.stream(modularities).boxed().collect(Collectors.toList());

        return new LouvainStatsResult(
            modularity,
            modularitiesAsList,
            ranLevels,
            communityCount,
            communityDistribution,
            preProcessingMillis,
            computeMillis,
            postProcessingMillis,
            configurationMap
        );
    }
}

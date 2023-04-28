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
package org.neo4j.gds.louvain;

import org.neo4j.gds.results.StandardStatsResult;

import java.util.List;
import java.util.Map;

public class StatsResult extends StandardStatsResult {
    public final double modularity;
    public final List<Double> modularities;
    public final long ranLevels;
    public final long communityCount;
    public final Map<String, Object> communityDistribution;

    StatsResult(
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
}

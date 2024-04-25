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
package org.neo4j.gds.algorithms.centrality;

import org.neo4j.gds.core.concurrency.DefaultPool;
import org.neo4j.gds.pagerank.PageRankConfig;
import org.neo4j.gds.pagerank.PageRankResult;
import org.neo4j.gds.result.CentralityStatistics;
import org.neo4j.gds.scaling.LogScaler;

import java.util.HashMap;
import java.util.Map;

import static org.neo4j.gds.utils.StringFormatting.toUpperCaseWithLocale;

final class PageRankDistributionComputer {
    private static final String HISTOGRAM_ERROR_KEY = "Error";

    private PageRankDistributionComputer() {}

    static PageRankDistribution computeDistribution(
        PageRankResult result,
        PageRankConfig configuration,
        boolean shouldComputeCentralityDistribution
    ) {
        Map<String, Object> centralitySummary = new HashMap<>();
        long postProcessingMillis = 0;
        if (shouldComputeCentralityDistribution) {
            var usingLogScaler = configuration.scaler().type().equals(LogScaler.TYPE);
            if (usingLogScaler) {
                centralitySummary.put(
                    HISTOGRAM_ERROR_KEY,
                    "Unable to create histogram when using scaler of type " + toUpperCaseWithLocale(LogScaler.TYPE)
                );
            } else {
                // Compute result statistics
                var centralityStatistics = CentralityStatistics.centralityStatistics(
                    result.nodeCount(),
                    result.centralityScoreProvider(),
                    DefaultPool.INSTANCE,
                    configuration.concurrency(),
                    true
                );

                centralitySummary = CentralityStatistics.centralitySummary(centralityStatistics.histogram());
                postProcessingMillis = centralityStatistics.computeMilliseconds();
            }
        }
        return new PageRankDistribution(centralitySummary, postProcessingMillis);
    }

}

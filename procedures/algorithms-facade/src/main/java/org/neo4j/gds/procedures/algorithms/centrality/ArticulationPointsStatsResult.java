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
package org.neo4j.gds.procedures.algorithms.centrality;

import org.neo4j.gds.applications.algorithms.machinery.AlgorithmProcessingTimings;
import org.neo4j.gds.procedures.algorithms.results.StandardStatsResult;
import org.neo4j.gds.result.AbstractResultBuilder;

import java.util.Map;

public class ArticulationPointsStatsResult extends StandardStatsResult {
    public final long articulationPointCount;

    public ArticulationPointsStatsResult(
        long computeMillis,
        Map<String, Object> configuration,
        long articulationPointCount
    ) {
        super(0,computeMillis,0,configuration);
        this.articulationPointCount = articulationPointCount;
    }
    public static Builder builder() {
        return new Builder();
    }


    public static ArticulationPointsStatsResult emptyFrom(AlgorithmProcessingTimings timings, Map<String, Object> configurationMap) {
        return new ArticulationPointsStatsResult(
            timings.computeMillis,
            configurationMap,
            0
        );
    }

    public static class Builder extends AbstractResultBuilder<ArticulationPointsStatsResult> {
        private long articulationPointCount;;

        public ArticulationPointsStatsResult.Builder withArticulationPointCount(long articulationPointCount) {
            this.articulationPointCount = articulationPointCount;
            return this;
        }

        public ArticulationPointsStatsResult build() {
            return new ArticulationPointsStatsResult(
                computeMillis,
                config.toMap(),
                articulationPointCount
            );
        }
    }
}

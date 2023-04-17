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
package org.neo4j.gds.kcore;

import org.neo4j.gds.result.AbstractResultBuilder;
import org.neo4j.gds.results.StandardStatsResult;

import java.util.Map;

public class StatsResult extends StandardStatsResult {

    public final long nodePropertiesWritten;
    public final long degeneracy;

    public StatsResult(
        long nodePropertiesWritten,
        long degeneracy,
        long preProcessingMillis,
        long computeMillis,
        long postProcessingMillis,
        Map<String, Object> configuration
    ) {
        super(preProcessingMillis, computeMillis, postProcessingMillis,  configuration);
        this.nodePropertiesWritten = nodePropertiesWritten;
        this.degeneracy = degeneracy;
    }

    static final class Builder extends AbstractResultBuilder<StatsResult> {

        private long degeneracy;

        Builder withDegeneracy(long degeneracy) {
            this.degeneracy = degeneracy;
            return this;
        }

        public StatsResult build() {
            return new StatsResult(
                nodePropertiesWritten,
                degeneracy,
                preProcessingMillis,
                computeMillis,
                -1L,
                config.toMap()
            );
        }
    }

}

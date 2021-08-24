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
package org.neo4j.gds.pregel.proc;

import org.neo4j.gds.results.StandardStatsResult;

import java.util.Map;

@SuppressWarnings("unused")
public final class PregelStatsResult extends StandardStatsResult {

    public final long ranIterations;
    public final boolean didConverge;

    private PregelStatsResult(
        long createMillis,
        long computeMillis,
        long ranIterations,
        boolean didConverge,
        Map<String, Object> configuration
    ) {
        super(createMillis, computeMillis, 0L, configuration);
        this.ranIterations = ranIterations;
        this.didConverge = didConverge;
    }

    public static class Builder extends AbstractPregelResultBuilder<PregelStatsResult> {

        @Override
        public PregelStatsResult build() {
            return new PregelStatsResult(createMillis, computeMillis, ranIterations, didConverge, config.toMap());
        }
    }
}

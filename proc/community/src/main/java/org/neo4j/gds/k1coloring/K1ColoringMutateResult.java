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
package org.neo4j.gds.k1coloring;

import org.neo4j.gds.api.ProcedureReturnColumns;

import java.util.Map;

@SuppressWarnings("unused")
public class K1ColoringMutateResult {

    public static final K1ColoringMutateResult EMPTY = new K1ColoringMutateResult(
        0,
        0,
        0,
        0,
        0,
        0,
        false,
        null
    );

    public final long preProcessingMillis;
    public final long computeMillis;
    public final long mutateMillis;

    public final long nodeCount;
    public final long colorCount;
    public final long ranIterations;
    public final boolean didConverge;

    public Map<String, Object> configuration;

    K1ColoringMutateResult(
        long preProcessingMillis,
        long computeMillis,
        long mutateMillis,
        long nodeCount,
        long colorCount,
        long ranIterations,
        boolean didConverge,
        Map<String, Object> configuration
    ) {
        this.preProcessingMillis = preProcessingMillis;
        this.computeMillis = computeMillis;
        this.mutateMillis = mutateMillis;
        this.nodeCount = nodeCount;
        this.colorCount = colorCount;
        this.ranIterations = ranIterations;
        this.didConverge = didConverge;
        this.configuration = configuration;
    }

    static class Builder extends K1ColoringResultBuilder<K1ColoringMutateResult> {
        Builder(ProcedureReturnColumns returnColumns, int concurrency) {
            super(returnColumns, concurrency);
        }

        @Override
        protected K1ColoringMutateResult buildResult() {
            return new K1ColoringMutateResult(
                preProcessingMillis,
                computeMillis,
                mutateMillis,
                nodeCount,
                colorCount,
                ranIterations,
                didConverge,
                config.toMap()
            );
        }
    }
}

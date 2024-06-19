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
package org.neo4j.gds.procedures.community.modularityoptimization;

import org.neo4j.gds.api.ProcedureReturnColumns;
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.procedures.algorithms.community.ModularityOptimizationResultBuilder;

import java.util.Map;

public class ModularityOptimizationWriteResult {

    public final long preProcessingMillis;
    public final long computeMillis;
    public final long writeMillis;
    public final long postProcessingMillis;
    public final long nodes;
    public boolean didConverge;
    public long ranIterations;
    public double modularity;
    public final long communityCount;
    public final Map<String, Object> communityDistribution;
    public final Map<String, Object> configuration;

    public ModularityOptimizationWriteResult(
        long preProcessingMillis,
        long computeMillis,
        long postProcessingMillis,
        long writeMillis,
        long nodes,
        boolean didConverge,
        long ranIterations,
        double modularity,
        long communityCount,
        Map<String, Object> communityDistribution,
        Map<String, Object> configuration
    ) {
        this.preProcessingMillis = preProcessingMillis;
        this.computeMillis = computeMillis;
        this.writeMillis = writeMillis;
        this.postProcessingMillis = postProcessingMillis;
        this.nodes = nodes;
        this.didConverge = didConverge;
        this.ranIterations = ranIterations;
        this.modularity = modularity;
        this.communityCount = communityCount;
        this.communityDistribution = communityDistribution;
        this.configuration = configuration;
    }

    public static class Builder extends ModularityOptimizationResultBuilder<ModularityOptimizationWriteResult> {

        public Builder(
            ProcedureReturnColumns returnColumns,
            Concurrency concurrency
        ) {
            super(returnColumns, concurrency);
        }

        @Override
        protected ModularityOptimizationWriteResult buildResult() {
            return new ModularityOptimizationWriteResult(
                preProcessingMillis,
                computeMillis,
                postProcessingDuration,
                writeMillis,
                nodeCount,
                didConverge,
                ranIterations,
                modularity,
                maybeCommunityCount.orElse(0),
                communityHistogramOrNull(),
                config.toMap()
            );
        }
    }
}

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
package org.neo4j.gds.similarity;

import org.HdrHistogram.DoubleHistogram;
import org.neo4j.gds.Algorithm;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.ProcedureReturnColumns;
import org.neo4j.gds.config.AlgoBaseConfig;
import org.neo4j.gds.executor.ComputationResult;

import java.util.function.Function;

import static org.neo4j.gds.core.ProcedureConstants.HISTOGRAM_PRECISION_DEFAULT;

public final class SimilarityProc {

    private SimilarityProc() {}

    public static boolean shouldComputeHistogram(ProcedureReturnColumns returnColumns) {
        return returnColumns.contains("similarityDistribution");
    }

    public static <RESULT, PROC_RESULT, CONFIG extends AlgoBaseConfig> SimilarityResultBuilder<PROC_RESULT> withGraphsizeAndTimings(
        SimilarityResultBuilder<PROC_RESULT> procResultBuilder,
        ComputationResult<? extends Algorithm<?>, RESULT, CONFIG> computationResult,
        Function<RESULT, SimilarityGraphResult> graphResultFunc
    ) {
        computationResult.result().ifPresent(result -> {
            SimilarityGraphResult graphResult = graphResultFunc.apply(result);
            resultBuilderWithTimings(procResultBuilder, computationResult)
                .withNodesCompared(graphResult.comparedNodes())
                .withRelationshipsWritten(graphResult.similarityGraph().relationshipCount());
        });

        return procResultBuilder;
    }

    public static <PROC_RESULT, CONFIG extends AlgoBaseConfig> SimilarityResultBuilder<PROC_RESULT> resultBuilderWithTimings(
        SimilarityResultBuilder<PROC_RESULT> procResultBuilder,
        ComputationResult<? extends Algorithm<?>, ?, CONFIG> computationResult
    ) {
        procResultBuilder
            .withPreProcessingMillis(computationResult.preProcessingMillis())
            .withComputeMillis(computationResult.computeMillis())
            .withConfig(computationResult.config());

        return procResultBuilder;
    }

    public static DoubleHistogram computeHistogram(Graph similarityGraph) {
        DoubleHistogram histogram = new DoubleHistogram(HISTOGRAM_PRECISION_DEFAULT);
        similarityGraph.forEachNode(nodeId -> {
            similarityGraph.forEachRelationship(nodeId, Double.NaN, (node1, node2, property) -> {
                histogram.recordValue(property);
                return true;
            });
            return true;
        });
        return histogram;
    }

}

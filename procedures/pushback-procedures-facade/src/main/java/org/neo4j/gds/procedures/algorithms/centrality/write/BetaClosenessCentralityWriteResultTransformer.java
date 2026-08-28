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
package org.neo4j.gds.procedures.algorithms.centrality.write;

import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.api.ResultStore;
import org.neo4j.gds.centrality.GenericCentralityWriteStep;
import org.neo4j.gds.closeness.ClosenessCentralityResult;
import org.neo4j.gds.core.JobId;
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.procedures.algorithms.centrality.BetaClosenessCentralityWriteResult;
import org.neo4j.gds.result.TimedAlgorithmResult;
import org.neo4j.gds.results.ResultTransformer;

import java.util.Map;
import java.util.stream.Stream;

public class BetaClosenessCentralityWriteResultTransformer implements ResultTransformer<TimedAlgorithmResult<ClosenessCentralityResult>, Stream<BetaClosenessCentralityWriteResult>>  {

    private final GenericCentralityWriteResultTransformer<ClosenessCentralityResult> genericCentralityWriteResultTransformer;
    private final String writeProperty;

    public BetaClosenessCentralityWriteResultTransformer(
        Graph graph,
        GraphStore graphStore,
        Map<String, Object> configuration,
        boolean shouldComputeDistribution,
        Concurrency concurrency,
        GenericCentralityWriteStep<ClosenessCentralityResult> writeStep,
        JobId jobId,
        ResultStore resultStore,
        String writeProperty
    ) {
        this.genericCentralityWriteResultTransformer  = new GenericCentralityWriteResultTransformer<>(
            graph,
            graphStore,
            configuration,
            shouldComputeDistribution,
            concurrency,
            writeStep,
            jobId,
            resultStore
        );
        this.writeProperty = writeProperty;

    }


    @Override
    public Stream<BetaClosenessCentralityWriteResult> apply(TimedAlgorithmResult<ClosenessCentralityResult> timedAlgorithmResult) {
        var centralityWriteResult = genericCentralityWriteResultTransformer.apply(timedAlgorithmResult).findFirst().orElseThrow();

        return Stream.of(
            new BetaClosenessCentralityWriteResult(
                    centralityWriteResult.nodePropertiesWritten(),
                    centralityWriteResult.preProcessingMillis(),
                    centralityWriteResult.computeMillis(),
                centralityWriteResult.postProcessingMillis(),
                centralityWriteResult.writeMillis(),
                writeProperty,
                centralityWriteResult.centralityDistribution(),
                centralityWriteResult.configuration()
            )
        );
    }
}

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

import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.api.ResultStore;
import org.neo4j.gds.centrality.HitsWriteStep;
import org.neo4j.gds.core.JobId;
import org.neo4j.gds.hits.HitsResultWithGraph;
import org.neo4j.gds.procedures.algorithms.WriteStepExecute;
import org.neo4j.gds.procedures.algorithms.centrality.HitsWriteResult;
import org.neo4j.gds.result.TimedAlgorithmResult;
import org.neo4j.gds.results.ResultTransformer;

import java.util.Map;
import java.util.stream.Stream;

public class HitsWriteResultTransformer implements ResultTransformer<TimedAlgorithmResult<HitsResultWithGraph>, Stream<HitsWriteResult>> {

    private final GraphStore graphStore;
    private final Map<String, Object> configuration;
    private final HitsWriteStep writeStep;
    private final JobId jobId;
    private final ResultStore resultStore;

    public HitsWriteResultTransformer(
        GraphStore graphStore,
        Map<String, Object> configuration,
        HitsWriteStep writeStep,
        JobId jobId,
        ResultStore resultStore
    ) {
        this.graphStore = graphStore;
        this.configuration = configuration;
        this.writeStep = writeStep;
        this.jobId = jobId;
        this.resultStore = resultStore;
    }


    @Override
    public Stream<HitsWriteResult> apply(TimedAlgorithmResult<HitsResultWithGraph> timedAlgorithmResult) {
        var result = timedAlgorithmResult.result();
        var graph = result.graph();

        var writeMetadata = WriteStepExecute.executeWriteNodePropertyStep(
            writeStep,
            graph,
            graphStore,
            jobId,
            result.pregelResult(),
            resultStore
        );

        return Stream.of(
            new HitsWriteResult(
                result.pregelResult().ranIterations(),
                result.pregelResult().didConverge(),
                0,
                timedAlgorithmResult.computeMillis(),
                writeMetadata.writeMillis(),
                writeMetadata.nodePropertiesWritten().value(),
                configuration
            )
        );
    }
}

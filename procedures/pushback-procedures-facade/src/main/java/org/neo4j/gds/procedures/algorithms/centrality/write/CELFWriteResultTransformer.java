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
import org.neo4j.gds.centrality.CelfWriteStep;
import org.neo4j.gds.core.JobId;
import org.neo4j.gds.influenceMaximization.CELFResult;
import org.neo4j.gds.procedures.algorithms.WriteStepExecute;
import org.neo4j.gds.procedures.algorithms.centrality.CELFWriteResult;
import org.neo4j.gds.result.TimedAlgorithmResult;
import org.neo4j.gds.results.ResultTransformer;

import java.util.Map;
import java.util.stream.Stream;

public class CELFWriteResultTransformer implements ResultTransformer<TimedAlgorithmResult<CELFResult>, Stream<CELFWriteResult>> {

    private final Graph graph;
    private final GraphStore graphStore;
    private final Map<String, Object> configuration;
    private final CelfWriteStep writeStep;
    private final JobId jobId;
    private final ResultStore resultStore;

    public CELFWriteResultTransformer(
        Graph graph,
        GraphStore graphStore,
        Map<String, Object> configuration,
        CelfWriteStep writeStep,
        JobId jobId,
        ResultStore resultStore
    ) {
        this.graph = graph;
        this.graphStore = graphStore;
        this.configuration = configuration;
        this.writeStep = writeStep;
        this.jobId = jobId;
        this.resultStore = resultStore;
    }


    @Override
    public Stream<CELFWriteResult> apply(TimedAlgorithmResult<CELFResult> timedAlgorithmResult) {
        var result = timedAlgorithmResult.result();

        var writeMetadata = WriteStepExecute.executeWriteNodePropertyStep(
            writeStep,
            graph,
            graphStore,
            jobId,
            result,
            resultStore
        );

        return Stream.of(
            new CELFWriteResult(
                writeMetadata.writeMillis(),
                writeMetadata.nodePropertiesWritten().value(),
                timedAlgorithmResult.computeMillis(),
                result.totalSpread(),
                graph.nodeCount(),
                configuration
            )
        );
    }
}

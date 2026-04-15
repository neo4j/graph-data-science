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
package org.neo4j.gds.procedures.pipelines;

import org.neo4j.gds.Aggregation;
import org.neo4j.gds.Orientation;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.api.ResultStore;
import org.neo4j.gds.api.ResultStoreEntry;
import org.neo4j.gds.applications.algorithms.machinery.WriteStep;
import org.neo4j.gds.applications.algorithms.metadata.RelationshipsWritten;
import org.neo4j.gds.core.JobId;
import org.neo4j.gds.core.concurrency.DefaultPool;
import org.neo4j.gds.core.concurrency.ParallelUtil;
import org.neo4j.gds.core.loading.construction.GraphFactory;
import org.neo4j.gds.logging.Log;
import org.neo4j.gds.ml.linkmodels.LinkPredictionResult;

class LinkPredictionPipelineWriteToResultStoreStep implements WriteStep<LinkPredictionResult, RelationshipsWritten> {
    private final Log log;
    private final LinkPredictionPredictPipelineWriteConfig configuration;
    private final org.neo4j.gds.termination.TerminationFlag terminationFlag;
    private final TrainedLPPipelineModel trainedLPPipelineModel;

    LinkPredictionPipelineWriteToResultStoreStep(
        Log log,
        LinkPredictionPredictPipelineWriteConfig configuration,
        org.neo4j.gds.termination.TerminationFlag terminationFlag,
        TrainedLPPipelineModel trainedLPPipelineModel
    ) {
        this.log = log;
        this.configuration = configuration;
        this.terminationFlag = terminationFlag;
        this.trainedLPPipelineModel = trainedLPPipelineModel;
    }

    @Override
    public RelationshipsWritten execute(
        Graph unused,
        GraphStore graphStore,
        ResultStore resultStore,
        LinkPredictionResult result,
        JobId jobId
    ) {
        var model = trainedLPPipelineModel.get(
            configuration.modelName(),
            configuration.username()
        );

        var lpGraphStoreFilter = LPGraphStoreFilterFactory.generate(
            log,
            model.trainConfig(),
            configuration,
            graphStore
        );

        var filteredGraph = graphStore.getGraph(lpGraphStoreFilter.predictNodeLabels());
        var relationshipsBuilder = GraphFactory.initRelationshipsBuilder()
            .aggregation(Aggregation.SINGLE)
            .nodes(filteredGraph)
            .relationshipType(RelationshipType.of(configuration.writeRelationshipType()))
            .orientation(Orientation.NATURAL)
            .addPropertyConfig(GraphFactory.PropertyConfig.of(configuration.writeProperty()))
            .concurrency(configuration.concurrency())
            .executorService(DefaultPool.INSTANCE)
            .build();

        ParallelUtil.parallelStreamConsume(
            result.stream(),
            configuration.concurrency(),
            terminationFlag,
            stream -> stream.forEach(predictedLink -> relationshipsBuilder.addFromInternal(
                filteredGraph.toRootNodeId(predictedLink.sourceId()),
                filteredGraph.toRootNodeId(predictedLink.targetId()),
                predictedLink.probability()
            ))
        );

        var relationships = relationshipsBuilder.build();
        var resultGraph = GraphFactory.create(filteredGraph.rootIdMap(), relationships);

        resultStore.add(
            jobId,
            new ResultStoreEntry.RelationshipsFromGraph(
                configuration.writeRelationshipType(),
                configuration.writeProperty(),
                resultGraph,
                filteredGraph::toOriginalNodeId
            )
        );

        return new RelationshipsWritten(resultGraph.relationshipCount());
    }
}

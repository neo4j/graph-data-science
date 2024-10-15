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

import org.neo4j.gds.Orientation;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.applications.algorithms.machinery.MutateStep;
import org.neo4j.gds.applications.algorithms.metadata.RelationshipsWritten;
import org.neo4j.gds.core.Aggregation;
import org.neo4j.gds.core.concurrency.DefaultPool;
import org.neo4j.gds.core.concurrency.ParallelUtil;
import org.neo4j.gds.core.loading.construction.GraphFactory;
import org.neo4j.gds.logging.Log;
import org.neo4j.gds.ml.linkmodels.LinkPredictionResult;
import org.neo4j.gds.termination.TerminationFlag;

class LinkPredictionPipelineMutateStep implements MutateStep<LinkPredictionResult, LinkPredictionMutateMetadata> {
    private final Log log;
    private final LinkPredictionPredictPipelineMutateConfig configuration;
    private final TerminationFlag terminationFlag;
    private final TrainedLPPipelineModel trainedLPPipelineModel;
    private final boolean shouldProduceHistogram;

    LinkPredictionPipelineMutateStep(
        Log log,
        LinkPredictionPredictPipelineMutateConfig configuration,
        TerminationFlag terminationFlag,
        TrainedLPPipelineModel trainedLPPipelineModel,
        boolean shouldProduceHistogram
    ) {
        this.log = log;
        this.configuration = configuration;
        this.terminationFlag = terminationFlag;
        this.trainedLPPipelineModel = trainedLPPipelineModel;
        this.shouldProduceHistogram = shouldProduceHistogram;
    }

    @Override
    public LinkPredictionMutateMetadata execute(
        Graph unused,
        GraphStore graphStore,
        LinkPredictionResult result
    ) {
        var model = trainedLPPipelineModel.get(
            configuration.modelName(),
            configuration.username()
        );

        var lpGraphStoreFilter = LPGraphStoreFilterFactory.generate(
            log, model.trainConfig(),
            configuration,
            graphStore
        );

        var filteredGraph = graphStore.getGraph(lpGraphStoreFilter.predictNodeLabels());

        var mutateRelationshipType = RelationshipType.of(configuration.mutateRelationshipType());

        var relationshipsBuilder = GraphFactory.initRelationshipsBuilder()
            .aggregation(Aggregation.SINGLE)
            .nodes(filteredGraph)
            .relationshipType(mutateRelationshipType)
            .orientation(Orientation.UNDIRECTED)
            .addPropertyConfig(GraphFactory.PropertyConfig.of(configuration.mutateProperty()))
            .concurrency(configuration.concurrency())
            .executorService(DefaultPool.INSTANCE)
            .build();

        var predictedLinkStream = result.stream();

        var histogram = readyHistogram(shouldProduceHistogram);

        ParallelUtil.parallelStreamConsume(
            predictedLinkStream,
            configuration.concurrency(),
            terminationFlag,
            stream -> stream.forEach(predictedLink -> {
                relationshipsBuilder.addFromInternal(
                    filteredGraph.toRootNodeId(predictedLink.sourceId()),
                    filteredGraph.toRootNodeId(predictedLink.targetId()),
                    predictedLink.probability()
                );
                histogram.onPredictedLink(predictedLink.probability());
            })
        );

        var relationships = relationshipsBuilder.build();

        // effect
        graphStore.addRelationshipType(relationships);

        // report metadata
        var relationshipsWritten = new RelationshipsWritten(relationships.topology().elementCount());
        var probabilityDistribution = histogram.finalise();
        return new LinkPredictionMutateMetadata(relationshipsWritten, probabilityDistribution);
    }

    private GdsHistogram readyHistogram(boolean shouldProduceHistogram) {
        if (shouldProduceHistogram) return new HdrBackedGdsHistogram();

        return GdsHistogram.DISABLED;
    }
}

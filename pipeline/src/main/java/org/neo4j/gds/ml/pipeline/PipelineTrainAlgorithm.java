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
package org.neo4j.gds.ml.pipeline;

import org.neo4j.gds.Algorithm;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.config.AlgoBaseConfig;
import org.neo4j.gds.core.model.CatalogModelContainer;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.model.ModelConfig;

import java.util.Set;

public abstract class PipelineTrainAlgorithm<
    RESULT,
    MODEL_RESULT extends CatalogModelContainer<?, CONFIG, ?>,
    CONFIG extends AlgoBaseConfig & ModelConfig,
    FEATURE_STEP extends FeatureStep> extends Algorithm<MODEL_RESULT> {
    protected final TrainingPipeline<FEATURE_STEP> pipeline;
    protected final GraphStore graphStore;
    protected final CONFIG config;

    private final PipelineTrainer<RESULT> pipelineTrainer;
    private final ResultToModelConverter<MODEL_RESULT, RESULT> toCatalogModelConverter;

    public PipelineTrainAlgorithm(
        PipelineTrainer<RESULT> pipelineTrainer,
        TrainingPipeline<FEATURE_STEP> pipeline,
        ResultToModelConverter<MODEL_RESULT, RESULT> toCatalogModelConverter,
        GraphStore graphStore,
        CONFIG config,
        ProgressTracker progressTracker
    ) {
        super(progressTracker);
        this.pipelineTrainer = pipelineTrainer;
        this.pipeline = pipeline;
        this.toCatalogModelConverter = toCatalogModelConverter;
        this.graphStore = graphStore;
        this.config = config;
    }

    @Override
    public MODEL_RESULT compute() {
        pipelineTrainer.setTerminationFlag(terminationFlag);

        pipeline.validateTrainingParameterSpace();
        pipeline.validateBeforeExecution(graphStore, config.nodeLabelIdentifiers(graphStore));

        var originalSchema = graphStore
            .schema()
            .filterNodeLabels(Set.copyOf(config.nodeLabelIdentifiers(graphStore)))
            .filterRelationshipTypes(Set.copyOf(config.internalRelationshipTypes(graphStore)));

        RESULT pipelineTrainResult = pipelineTrainer.run();
        return toCatalogModelConverter.toModel(pipelineTrainResult, originalSchema);
    }

    @Override
    public void release() {

    }
}

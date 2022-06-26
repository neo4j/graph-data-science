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
import org.neo4j.gds.api.schema.GraphSchema;
import org.neo4j.gds.config.AlgoBaseConfig;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.model.ModelConfig;

public abstract class PipelineTrainAlgorithm<
    RESULT,
    MODEL_RESULT,
    CONFIG extends AlgoBaseConfig & ModelConfig,
    FEATURE_STEP extends FeatureStep> extends Algorithm<MODEL_RESULT> {
    protected final TrainingPipeline<FEATURE_STEP> pipeline;
    protected final GraphStore graphStore;
    protected final CONFIG config;

    private final PipelineTrainer<RESULT> pipelineTrainer;

    public PipelineTrainAlgorithm(
        PipelineTrainer<RESULT> pipelineTrainer,
        TrainingPipeline<FEATURE_STEP> pipeline,
        GraphStore graphStore,
        CONFIG config,
        ProgressTracker progressTracker
    ) {
        super(progressTracker);
        this.pipelineTrainer = pipelineTrainer;
        this.pipelineTrainer.setTerminationFlag(terminationFlag);
        this.pipeline = pipeline;
        this.graphStore = graphStore;
        this.config = config;
    }

    @Override
    public MODEL_RESULT compute() {
        PipelineExecutor.validateTrainingParameterSpace(pipeline);
        pipeline.validateBeforeExecution(graphStore, config);
        var originalSchema = config.filteredSchema(graphStore);
        RESULT pipelineTrainResult = pipelineTrainer.run();
        return transformResult(pipelineTrainResult, config, originalSchema);
    }

    protected abstract MODEL_RESULT transformResult(RESULT result, CONFIG config, GraphSchema originalSchema);

    @Override
    public void release() {

    }
}

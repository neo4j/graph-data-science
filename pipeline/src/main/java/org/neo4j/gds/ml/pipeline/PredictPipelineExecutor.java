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
 import org.neo4j.gds.config.GraphNameConfig;
 import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
 import org.neo4j.gds.executor.ExecutionContext;

public abstract class PredictPipelineExecutor<
    PIPELINE_CONFIG extends AlgoBaseConfig & GraphNameConfig,
    PIPELINE extends Pipeline<?>,
    RESULT
    > extends Algorithm<RESULT> {

    protected final PIPELINE pipeline;
    protected final PIPELINE_CONFIG config;
    protected final ExecutionContext executionContext;
    protected final GraphStore graphStore;

    protected PredictPipelineExecutor(
        PIPELINE pipeline,
        PIPELINE_CONFIG config,
        ExecutionContext executionContext,
        GraphStore graphStore,
        ProgressTracker progressTracker
    ) {
        super(progressTracker);
        this.pipeline = pipeline;
        this.config = config;
        this.executionContext = executionContext;
        this.graphStore = graphStore;
    }

    protected abstract RESULT execute();

    protected abstract PipelineGraphFilter nodePropertyStepFilter();

    @Override
    public RESULT compute() {
        progressTracker.beginSubTask();

        PipelineGraphFilter nodePropertyStepFilter = nodePropertyStepFilter();

        //featureInput nodeLabels contain source&target nodeLabel used in training/testing plus contextNodeLabels
        pipeline.validateBeforeExecution(graphStore, nodePropertyStepFilter.nodeLabels());

        var nodePropertyStepExecutor = NodePropertyStepExecutor.of(
            executionContext,
            graphStore,
            config,
            nodePropertyStepFilter.nodeLabels(),
            nodePropertyStepFilter.intermediateRelationshipTypes(),
            progressTracker
        );

        try {
            // we are not validating the size of the feature-input graph as not every nodePropertyStep needs relationships
            nodePropertyStepExecutor.executeNodePropertySteps(pipeline.nodePropertySteps());
            pipeline.validateFeatureProperties(graphStore, config.nodeLabelIdentifiers(graphStore));

            var result = execute();
            progressTracker.endSubTask();
            return result;
        } finally {
            nodePropertyStepExecutor.cleanupIntermediateProperties(pipeline.nodePropertySteps());
        }
    }

    @Override
    public void release() {

    }
}

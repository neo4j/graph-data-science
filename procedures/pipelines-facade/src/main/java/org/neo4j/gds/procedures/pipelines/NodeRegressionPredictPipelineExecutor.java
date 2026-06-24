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


import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.collections.ha.HugeDoubleArray;
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.progress.tracking.ProgressTracker;
import org.neo4j.gds.progress.tasks.Task;
import org.neo4j.gds.progress.tasks.Tasks;
import org.neo4j.gds.executor.ExecutionContext;
import org.neo4j.gds.ml.models.Features;
import org.neo4j.gds.ml.models.FeaturesFactory;
import org.neo4j.gds.ml.models.Regressor;
import org.neo4j.gds.ml.nodePropertyPrediction.regression.NodeRegressionPredict;
import org.neo4j.gds.ml.pipeline.NodePropertyStepExecutor;
import org.neo4j.gds.ml.pipeline.PipelineGraphFilter;
import org.neo4j.gds.ml.pipeline.PredictPipelineExecutor;
import org.neo4j.gds.ml.pipeline.nodePipeline.NodePropertyPredictPipeline;
import org.neo4j.gds.termination.TerminationFlag;
import org.neo4j.gds.utils.StringJoining;

import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

public class NodeRegressionPredictPipelineExecutor extends PredictPipelineExecutor<
    NodeRegressionPredictPipelineBaseConfig,
    NodePropertyPredictPipeline,
    HugeDoubleArray
    > {
    private final ProgressTracker progressTracker;
    private final TerminationFlag terminationFlag;
    private final Regressor regressor;
    private final PipelineGraphFilter predictGraphFilter;

    public NodeRegressionPredictPipelineExecutor(
        ProgressTracker progressTracker,
        TerminationFlag terminationFlag,
        NodePropertyPredictPipeline pipeline,
        NodeRegressionPredictPipelineBaseConfig config,
        ExecutionContext executionContext,
        GraphStore graphStore,
        Regressor regressor
    ) {
        super(progressTracker, pipeline, config, executionContext, graphStore);
        this.progressTracker = progressTracker;
        this.terminationFlag = terminationFlag;
        this.regressor = regressor;
        this.predictGraphFilter = new PipelineGraphFilter(
            config.nodeLabelIdentifiers(graphStore),
            config.internalRelationshipTypes(graphStore)
        );
    }

    public static Task progressTask(
        String taskName,
        Concurrency concurrency,
        NodePropertyPredictPipeline pipeline, GraphStore graphStore
    ) {
        return Tasks.task(
            taskName,
            concurrency,
            NodePropertyStepExecutor.tasks(concurrency, pipeline.nodePropertySteps(), graphStore.nodeCount()),
            NodeRegressionPredict.progressTask(concurrency, graphStore.nodeCount())
        );
    }

    @Override
    protected PipelineGraphFilter nodePropertyStepFilter() {
        return predictGraphFilter;
    }

    @Override
    protected HugeDoubleArray execute() {
        var nodesGraph = graphStore.getGraph(predictGraphFilter.nodeLabels());
        Features features = FeaturesFactory.extractLazyFeatures(nodesGraph, pipeline.featureProperties());

        if (features.featureDimension() != regressor.data().featureDimension()) {
            throw new IllegalArgumentException(formatWithLocale(
                "Model expected features %s to have a total dimension of `%d`, but got `%d`.",
                StringJoining.join(pipeline.featureProperties()),
                regressor.data().featureDimension(),
                features.featureDimension()
            ));
        }

        return new NodeRegressionPredict(
            regressor,
            features,
            config.concurrency(),
            progressTracker,
            terminationFlag
        ).compute();
    }
}

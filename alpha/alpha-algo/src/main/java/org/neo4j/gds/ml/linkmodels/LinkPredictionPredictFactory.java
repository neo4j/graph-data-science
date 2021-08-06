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
package org.neo4j.gds.ml.linkmodels;

import org.jetbrains.annotations.TestOnly;
import org.neo4j.gds.AbstractAlgorithmFactory;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.core.model.ModelCatalog;
import org.neo4j.gds.core.utils.ProgressLogger;
import org.neo4j.gds.core.utils.mem.AllocationTracker;
import org.neo4j.gds.core.utils.mem.MemoryEstimation;
import org.neo4j.gds.core.utils.progress.v2.tasks.ProgressTracker;
import org.neo4j.gds.core.utils.progress.v2.tasks.Task;
import org.neo4j.gds.core.utils.progress.v2.tasks.Tasks;
import org.neo4j.gds.ml.core.features.FeatureExtraction;
import org.neo4j.gds.ml.linkmodels.logisticregression.LinkLogisticRegressionData;
import org.neo4j.gds.ml.linkmodels.logisticregression.LinkLogisticRegressionPredictor;

import static org.neo4j.gds.ml.linkmodels.LinkPredictionTrainEstimation.ASSUMED_MIN_NODE_FEATURES;

class LinkPredictionPredictFactory<CONFIG extends LinkPredictionPredictBaseConfig> extends AbstractAlgorithmFactory<LinkPredictionPredict, CONFIG> {

    public LinkPredictionPredictFactory() {
        super();
    }

    @TestOnly
    LinkPredictionPredictFactory(ProgressLogger.ProgressLoggerFactory loggerFactory) {
        super(loggerFactory);
    }

    @Override
    protected String taskName() {
        return "LinkPrediction";
    }

    @Override
    protected LinkPredictionPredict build(
        Graph graph,
        CONFIG configuration,
        AllocationTracker tracker,
        ProgressTracker progressTracker
    ) {
        var model = ModelCatalog.get(
            configuration.username(),
            configuration.modelName(),
            LinkLogisticRegressionData.class,
            LinkPredictionTrainConfig.class,
            LinkPredictionModelInfo.class
        );

        var extractors = FeatureExtraction.propertyExtractors(graph, model.trainConfig().featureProperties());

        return new LinkPredictionPredict(
            new LinkLogisticRegressionPredictor(model.data(), model.trainConfig().featureProperties(), extractors),
            graph,
            configuration.batchSize(),
            configuration.concurrency(),
            configuration.topN(),
            progressTracker,
            configuration.threshold()
        );
    }

    @Override
    public MemoryEstimation memoryEstimation(CONFIG configuration) {
        var model = ModelCatalog.get(
            configuration.username(),
            configuration.modelName(),
            LinkLogisticRegressionData.class,
            LinkPredictionTrainConfig.class,
            LinkPredictionModelInfo.class
        );
        int linkFeatureDimension = model.data().weights().dimension(1);
        var nodeFeatureDimension = Math.max(model.trainConfig().featureProperties().size(), ASSUMED_MIN_NODE_FEATURES);
        return LinkPredictionPredict.memoryEstimation(configuration.topN(), linkFeatureDimension, nodeFeatureDimension);
    }

    @Override
    public Task progressTask(Graph graph, CONFIG config) {
        return Tasks.leaf(taskName(), graph.nodeCount());
    }
}

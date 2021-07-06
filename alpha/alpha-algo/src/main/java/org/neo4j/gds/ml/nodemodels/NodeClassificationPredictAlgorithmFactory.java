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
package org.neo4j.gds.ml.nodemodels;

import org.jetbrains.annotations.TestOnly;
import org.neo4j.gds.ml.nodemodels.logisticregression.NodeLogisticRegressionData;
import org.neo4j.gds.ml.nodemodels.logisticregression.NodeLogisticRegressionPredictor;
import org.neo4j.graphalgo.AbstractAlgorithmFactory;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.model.ModelCatalog;
import org.neo4j.graphalgo.core.utils.ProgressLogger;
import org.neo4j.graphalgo.core.utils.mem.AllocationTracker;
import org.neo4j.graphalgo.core.utils.mem.MemoryEstimation;
import org.neo4j.graphalgo.core.utils.progress.v2.tasks.ProgressTracker;
import org.neo4j.graphalgo.core.utils.progress.v2.tasks.Task;
import org.neo4j.graphalgo.core.utils.progress.v2.tasks.Tasks;

public class NodeClassificationPredictAlgorithmFactory<CONFIG extends NodeClassificationPredictConfig> extends AbstractAlgorithmFactory<NodeClassificationPredict, CONFIG> {

    public NodeClassificationPredictAlgorithmFactory() {
        super();
    }

    @Override
    protected long taskVolume(Graph graph, NodeClassificationPredictConfig configuration) {
        return graph.nodeCount();
    }

    @Override
    protected String taskName() {
        return "NodeLogisticRegressionPredict";
    }

    @Override
    protected NodeClassificationPredict build(
        Graph graph,
        NodeClassificationPredictConfig configuration,
        AllocationTracker tracker,
        ProgressTracker progressTracker
    ) {
        var model = ModelCatalog.get(
            configuration.username(),
            configuration.modelName(),
            NodeLogisticRegressionData.class,
            NodeClassificationTrainConfig.class
        );
        var featureProperties = model.trainConfig().featureProperties();
        return new NodeClassificationPredict(
            new NodeLogisticRegressionPredictor(model.data(), featureProperties),
            graph,
            configuration.batchSize(),
            configuration.concurrency(),
            configuration.includePredictedProbabilities(),
            featureProperties,
            tracker,
            progressTracker
        );
    }

    @Override
    public MemoryEstimation memoryEstimation(NodeClassificationPredictConfig configuration) {

        var fudgedFeatureCount = 500;

        var classCount = ModelCatalog
            .get(configuration.username(),
                configuration.modelName(),
                NodeLogisticRegressionData.class,
                NodeClassificationTrainConfig.class
            )
            .data()
            .classIdMap()
            .originalIds()
            .length;

        return NodeClassificationPredict.memoryEstimation(configuration.includePredictedProbabilities(), configuration.batchSize(), fudgedFeatureCount, classCount);
    }

    @Override
    public Task progressTask(Graph graph, CONFIG config) {
        return Tasks.leaf("compute", taskVolume(graph, config));
    }

    @TestOnly
    NodeClassificationPredictAlgorithmFactory(ProgressLogger.ProgressLoggerFactory factory) {
        super(factory);
    }
}

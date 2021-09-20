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

import org.neo4j.gds.AlgorithmFactory;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.core.model.ModelCatalog;
import org.neo4j.gds.core.utils.mem.AllocationTracker;
import org.neo4j.gds.core.utils.mem.MemoryEstimation;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.core.utils.progress.tasks.Task;
import org.neo4j.gds.core.utils.progress.tasks.Tasks;
import org.neo4j.gds.ml.nodemodels.logisticregression.NodeLogisticRegressionData;
import org.neo4j.gds.ml.nodemodels.logisticregression.NodeLogisticRegressionPredictor;

public class NodeClassificationPredictAlgorithmFactory<CONFIG extends NodeClassificationPredictConfig> extends AlgorithmFactory<NodeClassificationPredict, CONFIG> {

    public NodeClassificationPredictAlgorithmFactory() {
        super();
    }

    @Override
    protected String taskName() {
        return "NodeLogisticRegressionPredict";
    }

    @Override
    protected NodeClassificationPredict build(
        Graph graph,
        NodeClassificationPredictConfig configuration,
        AllocationTracker allocationTracker,
        ProgressTracker progressTracker
    ) {
        var model = ModelCatalog.get(
            configuration.username(),
            configuration.modelName(),
            NodeLogisticRegressionData.class,
            NodeClassificationTrainConfig.class,
            NodeClassificationModelInfo.class
        );
        var featureProperties = model.trainConfig().featureProperties();
        return new NodeClassificationPredict(
            new NodeLogisticRegressionPredictor(model.data(), featureProperties),
            graph,
            configuration.batchSize(),
            configuration.concurrency(),
            configuration.includePredictedProbabilities(),
            featureProperties,
            allocationTracker,
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
                NodeClassificationTrainConfig.class,
                NodeClassificationModelInfo.class
            )
            .data()
            .classIdMap()
            .originalIds()
            .length;

        return NodeClassificationPredict.memoryEstimation(configuration.includePredictedProbabilities(), configuration.batchSize(), fudgedFeatureCount, classCount);
    }

    @Override
    public Task progressTask(Graph graph, CONFIG config) {
        return Tasks.leaf(taskName(), graph.nodeCount());
    }
}

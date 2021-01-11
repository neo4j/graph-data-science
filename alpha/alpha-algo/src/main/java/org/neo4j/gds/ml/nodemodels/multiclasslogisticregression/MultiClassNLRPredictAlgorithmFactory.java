/*
 * Copyright (c) 2017-2021 "Neo4j,"
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
package org.neo4j.gds.ml.nodemodels.multiclasslogisticregression;

import org.jetbrains.annotations.TestOnly;
import org.neo4j.gds.ml.nodemodels.logisticregression.NodeLogisticRegressionTrainConfig;
import org.neo4j.graphalgo.AbstractAlgorithmFactory;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.model.ModelCatalog;
import org.neo4j.graphalgo.core.utils.ProgressLogger;
import org.neo4j.graphalgo.core.utils.mem.AllocationTracker;
import org.neo4j.graphalgo.core.utils.mem.MemoryEstimation;
import org.neo4j.graphalgo.exceptions.MemoryEstimationNotImplementedException;

public class MultiClassNLRPredictAlgorithmFactory extends AbstractAlgorithmFactory<MultiClassNLRPredictAlgorithm, MultiClassNLRPredictMutateConfig> {

    public MultiClassNLRPredictAlgorithmFactory() {
        super();
    }

    @Override
    protected long taskVolume(
        Graph graph, MultiClassNLRPredictMutateConfig configuration
    ) {
        return graph.nodeCount();
    }

    @Override
    protected String taskName() {
        return "MultiClassNodeLogisticRegressionPredict";
    }

    @Override
    protected MultiClassNLRPredictAlgorithm build(
        Graph graph,
        MultiClassNLRPredictMutateConfig configuration,
        AllocationTracker tracker,
        ProgressLogger progressLogger
    ) {
        var model = ModelCatalog.get(
            configuration.username(),
            configuration.modelName(),
            MultiClassNLRData.class,
            NodeLogisticRegressionTrainConfig.class
        );
        return new MultiClassNLRPredictAlgorithm(
            new MultiClassNLRPredictor(model.data()),
            graph,
            configuration.batchSize(),
            configuration.concurrency(),
            configuration.predictedProbabilityProperty().isPresent(),
            progressLogger
        );
    }

    @Override
    public MemoryEstimation memoryEstimation(MultiClassNLRPredictMutateConfig configuration) {
        throw new MemoryEstimationNotImplementedException();
    }

    @TestOnly
    MultiClassNLRPredictAlgorithmFactory(ProgressLogger.ProgressLoggerFactory factory) {
        super(factory);
    }
}

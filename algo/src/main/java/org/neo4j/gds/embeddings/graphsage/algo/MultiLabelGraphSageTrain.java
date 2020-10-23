/*
 * Copyright (c) 2017-2020 "Neo4j,"
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
package org.neo4j.gds.embeddings.graphsage.algo;

import org.neo4j.gds.embeddings.graphsage.FeatureFunction;
import org.neo4j.gds.embeddings.graphsage.GraphSageModelTrainer;
import org.neo4j.gds.embeddings.graphsage.ModelData;
import org.neo4j.gds.embeddings.graphsage.MultiLabelFeatureFunction;
import org.neo4j.gds.embeddings.graphsage.ddl4j.functions.Weights;
import org.neo4j.gds.embeddings.graphsage.ddl4j.tensor.Tensor;
import org.neo4j.graphalgo.NodeLabel;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.model.Model;
import org.neo4j.graphalgo.core.utils.ProgressLogger;
import org.neo4j.graphalgo.core.utils.mem.AllocationTracker;

import java.util.Map;
import java.util.stream.Collectors;

import static org.neo4j.gds.embeddings.graphsage.GraphSageHelper.initializeFeatures;
import static org.neo4j.gds.embeddings.graphsage.GraphSageHelper.propertyKeysPerNodeLabel;
import static org.neo4j.gds.embeddings.graphsage.LayerFactory.generateWeights;

public class MultiLabelGraphSageTrain extends GraphSageTrain {

    private static final double WEIGHT_BOUND = 1.0D;

    private final Graph graph;
    private final GraphSageTrainConfig config;
    private final AllocationTracker tracker;
    private Map<NodeLabel, Weights<? extends Tensor<?>>> weightsByLabel;

    public MultiLabelGraphSageTrain(
        Graph graph,
        GraphSageTrainConfig config,
        ProgressLogger progressLogger,
        AllocationTracker tracker
    ) {
        this.graph = graph;
        this.config = config;
        this.progressLogger = progressLogger;
        this.tracker = tracker;
        this.weightsByLabel = makeWeightsByLabel();
    }

    @Override
    public Model<ModelData, GraphSageTrainConfig> compute() {
        FeatureFunction featureFunction = new MultiLabelFeatureFunction(
            graph,
            weightsByLabel,
            config.projectedFeatureDimension().orElseThrow()
        );
        GraphSageModelTrainer trainer = new GraphSageModelTrainer(config, progressLogger, featureFunction, weightsByLabel.values());

        GraphSageModelTrainer.ModelTrainResult trainResult = trainer.train(
            graph,
            initializeFeatures(graph, config, tracker)
        );

        return Model.of(
            config.username(),
            config.modelName(),
            GraphSage.MODEL_TYPE,
            graph.schema(),
            ModelData.of(trainResult.layers(), featureFunction),
            config
        );
    }

    @Override
    public void release() {
        this.weightsByLabel = null;
    }

    private Map<NodeLabel, Weights<? extends Tensor<?>>> makeWeightsByLabel() {
        return propertyKeysPerNodeLabel(graph)
            .entrySet()
            .stream()
            .collect(Collectors.toMap(Map.Entry::getKey, e -> {
                int numProperties = (int) config.featureProperties().stream().filter(e.getValue()::contains).count();
                if (config.degreeAsProperty()) {
                    numProperties += 1;
                }
                // Label is used as a property
                numProperties += 1;
                //TODO: how should we initialize the values in the matrix?
                return generateWeights(config.projectedFeatureDimension().orElseThrow(), numProperties, WEIGHT_BOUND);
            }));
    }
}

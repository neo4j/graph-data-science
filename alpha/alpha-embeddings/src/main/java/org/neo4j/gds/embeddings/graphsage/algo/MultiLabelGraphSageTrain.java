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

import org.neo4j.gds.embeddings.graphsage.GraphSageModelTrainer;
import org.neo4j.gds.embeddings.graphsage.Layer;
import org.neo4j.gds.embeddings.graphsage.ddl4j.Variable;
import org.neo4j.gds.embeddings.graphsage.ddl4j.functions.LabelwiseFeatureProjection;
import org.neo4j.gds.embeddings.graphsage.ddl4j.functions.Weights;
import org.neo4j.gds.embeddings.graphsage.ddl4j.tensor.Matrix;
import org.neo4j.gds.embeddings.graphsage.ddl4j.tensor.Tensor;
import org.neo4j.graphalgo.Algorithm;
import org.neo4j.graphalgo.NodeLabel;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.NodeProperties;
import org.neo4j.graphalgo.core.model.Model;
import org.neo4j.graphalgo.core.utils.mem.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.HugeObjectArray;
import org.neo4j.logging.Log;

import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.DoubleStream;

import static org.neo4j.gds.embeddings.graphsage.LayerFactory.generateWeights;

public class MultiLabelGraphSageTrain extends Algorithm<MultiLabelGraphSageTrain, Model<Layer[], MultiLabelGraphSageTrainConfig>> {

    private final Graph graph;
    private final MultiLabelGraphSageTrainConfig config;
    private final AllocationTracker tracker;
    private final Log log;
    private Map<NodeLabel, Weights<? extends Tensor<?>>> weightsByLabel;

    public MultiLabelGraphSageTrain(
        Graph graph,
        MultiLabelGraphSageTrainConfig config,
        AllocationTracker tracker,
        Log log
    ) {
        this.graph = graph;
        this.config = config;
        this.tracker = tracker;
        this.log = log;
        this.weightsByLabel = makeWeightsByLabel();
    }

    @Override
    public Model<Layer[], MultiLabelGraphSageTrainConfig> compute() {
        GraphSageModelTrainer trainer = new GraphSageModelTrainer(config, log, this::apply, weightsByLabel.values());

        GraphSageModelTrainer.ModelTrainResult trainResult = trainer.train(
            graph,
            initializeFeatures()
        );

        return Model.of(
            config.username(),
            config.modelName(),
            GraphSage.MODEL_TYPE,
            graph.schema(),
            trainResult.layers(),
            config
        );
    }

    @Override
    public MultiLabelGraphSageTrain me() {
        throw new UnsupportedOperationException(
            "org.neo4j.gds.embeddings.graphsage.algo.MultiLabelGraphSageTrain.me is not implemented.");
    }

    @Override
    public void release() {
        throw new UnsupportedOperationException(
            "org.neo4j.gds.embeddings.graphsage.algo.MultiLabelGraphSageTrain.release is not implemented.");
    }

    public HugeObjectArray<double[]> initializeFeatures() {
        Map<NodeLabel, Set<NodeProperties>> propertiesPerNodeLabel = propertiesPerNodeLabel()
            .entrySet()
            .stream()
            .collect(Collectors.toMap(
                Map.Entry::getKey,
                e -> config.nodePropertyNames()
                    .stream()
                    .filter(e.getValue()::contains)
                    .map(graph::nodeProperties)
                    .collect(Collectors.toSet())
            ));

        HugeObjectArray<double[]> features = HugeObjectArray.newArray(
            double[].class,
            graph.nodeCount(),
            tracker
        );

        features.setAll(n -> {
            var relevantProperties = propertiesPerNodeLabel.get(labelOf(n));
            DoubleStream nodeFeatures = relevantProperties.stream().mapToDouble(p -> p.doubleValue(n));
            if (config.degreeAsProperty()) {
                nodeFeatures = DoubleStream.concat(nodeFeatures, DoubleStream.of(graph.degree(n)));
            }
            return nodeFeatures.toArray();
        });
        return features;
    }

    private Map<NodeLabel, Weights<? extends Tensor<?>>> makeWeightsByLabel() {
        return propertiesPerNodeLabel()
            .entrySet()
            .stream()
            .collect(Collectors.toMap(Map.Entry::getKey, e -> {
                int numProperties = (int) config.nodePropertyNames().stream().filter(e.getValue()::contains).count();
                if (config.degreeAsProperty()) {
                    numProperties += 1;
                }
                //TODO: how should we initialize the values in the matrix?
                return generateWeights(config.projectedFeatureSize(), numProperties, 1.0D);
            }));
    }

    private Map<NodeLabel, Set<String>> propertiesPerNodeLabel() {
        return graph.schema().nodeSchema().properties()
            .entrySet()
            .stream()
            .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().keySet()));
    }

    private Variable<Matrix> apply(long[] nodeIds, HugeObjectArray<double[]> features) {
        NodeLabel[] labels = new NodeLabel[nodeIds.length];
        for (int i = 0; i < nodeIds.length; i++) {
            labels[i] = graph.nodeLabels(nodeIds[i]).stream().findFirst().get();
        }
        return new LabelwiseFeatureProjection(nodeIds, features, weightsByLabel, config.projectedFeatureSize(), labels);
    }

    private NodeLabel labelOf(long nodeId) {
        return graph.nodeLabels(nodeId).stream().findFirst().get();
    }
}

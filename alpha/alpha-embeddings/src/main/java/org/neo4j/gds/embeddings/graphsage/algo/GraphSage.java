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

import org.neo4j.graphalgo.Algorithm;
import org.neo4j.graphalgo.annotation.ValueClass;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.NodeProperties;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.HugeObjectArray;
import org.neo4j.gds.embeddings.graphsage.GraphSageModel;
import org.neo4j.logging.Log;

import java.util.List;
import java.util.Map;
import java.util.stream.DoubleStream;
import java.util.stream.LongStream;

import static java.util.stream.Collectors.toList;

public class GraphSage extends Algorithm<GraphSage, GraphSage.GraphSageResult> {

    private final GraphSageModel graphSageModel;
    private final Graph graph;
    private final List<NodeProperties> nodeProperties;
    private final boolean useDegreeAsProperty;

    public GraphSage(Graph graph, GraphSageBaseConfig config, Log log) {
        this.useDegreeAsProperty = config.degreeAsProperty();
        this.graph = graph;

        nodeProperties = config
            .nodePropertyNames()
            .stream()
            .map(graph::nodeProperties)
            .collect(toList());

        graphSageModel = new GraphSageModel(config, log);
    }

    @Override
    public GraphSageResult compute() {
        // TODO: Split training into its own procedure?
        HugeObjectArray<double[]> features = initializeFeatures();
        GraphSageModel.TrainResult trainResult = graphSageModel.train(graph, features);
        HugeObjectArray<double[]> embeddings = graphSageModel.makeEmbeddings(graph, features);
        return GraphSageResult.of(trainResult.startLoss(), trainResult.epochLosses(), embeddings);
    }

    @Override
    public GraphSage me() {
        return this;
    }

    @Override
    public void release() {

    }

    private HugeObjectArray<double[]> initializeFeatures() {
        HugeObjectArray<double[]> features = HugeObjectArray.newArray(
            double[].class,
            graph.nodeCount(),
            AllocationTracker.EMPTY
        );
        features.setAll(n -> {
            DoubleStream nodeFeatures = this.nodeProperties.stream().mapToDouble(p -> p.nodeProperty(n));
            if (useDegreeAsProperty) {
                nodeFeatures = DoubleStream.concat(nodeFeatures, DoubleStream.of(graph.degree(n)));
            }
            return nodeFeatures.toArray();
        });
        return features;
    }

    @ValueClass
    public interface GraphSageResult {

        Map<String, Double> epochLosses();

        HugeObjectArray<double[]> embeddings();

        double startLoss();

        static GraphSageResult of(
            double startLoss,
            Map<String, Double> epochLosses,
            HugeObjectArray<double[]> embeddings
        ) {
            return ImmutableGraphSageResult.builder()
                .startLoss(startLoss)
                .epochLosses(epochLosses)
                .embeddings(embeddings)
                .build();
        }
    }
}

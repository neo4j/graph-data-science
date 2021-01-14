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
package org.neo4j.gds.ml.nodemodels;

import org.neo4j.gds.ml.nodemodels.logisticregression.MultiClassNLRTrainConfig;
import org.neo4j.gds.ml.nodemodels.logisticregression.NodeClassificationTrainConfig;
import org.neo4j.gds.ml.nodemodels.multiclasslogisticregression.MultiClassNLRData;
import org.neo4j.gds.ml.nodemodels.multiclasslogisticregression.MultiClassNLRTrain;
import org.neo4j.graphalgo.Algorithm;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.model.Model;
import org.neo4j.logging.Log;

import java.util.List;
import java.util.stream.Collectors;

public class NodeClassificationTrain
    extends Algorithm<NodeClassificationTrain, Model<MultiClassNLRData, NodeClassificationTrainConfig>> {

    public static final String MODEL_TYPE = "multiClassNodeLogisticRegression";

    private final Graph graph;
    private final NodeClassificationTrainConfig config;
    private final Log log;

    public NodeClassificationTrain(
        Graph graph,
        NodeClassificationTrainConfig config,
        Log log
    ) {
        this.graph = graph;
        this.config = config;
        this.log = log;
    }

    @Override
    public Model<MultiClassNLRData, NodeClassificationTrainConfig> compute() {
        var concreteConfig = modelSelect(graph, concreteConfigs(), log);
        var concreteTrain = new MultiClassNLRTrain(graph, concreteConfig, log);
        var modelData = concreteTrain.compute();

        return Model.of(
            config.username(),
            config.modelName(),
            MODEL_TYPE,
            graph.schema(),
            modelData,
            config
        );
    }

    private MultiClassNLRTrainConfig modelSelect(
        Graph graph,
        List<MultiClassNLRTrainConfig> concreteConfigs,
        Log log
    ) {
        // TODO: do real model selection
        return concreteConfigs.get(0);
    }

    private List<MultiClassNLRTrainConfig> concreteConfigs() {
        return config.params().stream()
            .map(singleParams -> MultiClassNLRTrainConfig.of(
                    config.featureProperties(),
                    config.targetProperty(),
                    singleParams
                )
            ).collect(Collectors.toList());
    }

    @Override
    public NodeClassificationTrain me() {
        return this;
    }

    @Override
    public void release() {

    }
}

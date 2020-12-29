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
package org.neo4j.gds.ml.nodemodels.logisticregression;

import org.neo4j.gds.ml.ImmutableTrainingSettings;
import org.neo4j.gds.ml.TrainingSettings;
import org.neo4j.gds.embeddings.graphsage.ddl4j.functions.Weights;
import org.neo4j.gds.embeddings.graphsage.ddl4j.tensor.Tensor;
import org.neo4j.graphalgo.AlgoBaseProc;
import org.neo4j.graphalgo.AlgorithmFactory;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.config.GraphCreateConfig;
import org.neo4j.graphalgo.core.CypherMapWrapper;
import org.neo4j.graphalgo.core.utils.mem.AllocationTracker;
import org.neo4j.graphalgo.core.utils.mem.MemoryEstimation;
import org.neo4j.graphalgo.exceptions.MemoryEstimationNotImplementedException;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

public class NodeLogisticRegressionTrainProc extends AlgoBaseProc<NodeLogisticRegressionTrain, NodeLogisticRegressionPredictor, NodeLogisticRegressionTrainConfig> {

    @Procedure(name = "gds.alpha.ml.nodeLogisticRegression.train", mode = Mode.READ)
    @Description("Trains a binary logistic regression model for a target node property")
    public Stream<StreamResult> train(
        @Name(value = "graphName") Object graphNameOrConfig,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        var result = compute(
            graphNameOrConfig,
            configuration
        );
        Weights<? extends Tensor<?>> weights = result.result().modelData().weights();
        double[] weightValues = weights.data().data();
        var config = result.config();
        List<String> props = config.featureProperties();
        var resultString = "";
        for (int i = 0; i < props.size(); i++) {
            resultString += " + " + weightValues[i] + props.get(i);
        }
        resultString += " + " + weightValues[weightValues.length - 1];
        return Stream.of(new StreamResult(resultString));
    }

    @Override
    protected NodeLogisticRegressionTrainConfig newConfig(
        String username,
        Optional<String> graphName,
        Optional<GraphCreateConfig> maybeImplicitCreate,
        CypherMapWrapper config
    ) {
        return NodeLogisticRegressionTrainConfig.of(username, graphName, maybeImplicitCreate, config);
    }

    @Override
    protected AlgorithmFactory<NodeLogisticRegressionTrain, NodeLogisticRegressionTrainConfig> algorithmFactory() {
        return new AlgorithmFactory<>() {
            @Override
            public NodeLogisticRegressionTrain build(
                Graph graph, NodeLogisticRegressionTrainConfig configuration, AllocationTracker tracker, Log log
            ) {
                //TODO: make configuration extend TrainingSettings
                TrainingSettings trainingSettings = ImmutableTrainingSettings.builder().build();
                return new NodeLogisticRegressionTrain(graph, trainingSettings, configuration, log);
            }

            @Override
            public MemoryEstimation memoryEstimation(NodeLogisticRegressionTrainConfig configuration) {
                throw new MemoryEstimationNotImplementedException();
            }
        };
    }

    public static final class StreamResult {
        public final String modelDescription;

        StreamResult(String modelDescription) {
            this.modelDescription = modelDescription;
        }
    }
}

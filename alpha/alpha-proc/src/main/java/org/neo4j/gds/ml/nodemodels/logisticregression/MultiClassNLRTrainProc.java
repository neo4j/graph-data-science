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
package org.neo4j.gds.ml.nodemodels.logisticregression;

import org.neo4j.gds.ml.nodemodels.multiclasslogisticregression.MultiClassNLRData;
import org.neo4j.gds.ml.nodemodels.multiclasslogisticregression.MultiClassNLRTrain;
import org.neo4j.graphalgo.AlgorithmFactory;
import org.neo4j.graphalgo.TrainProc;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.config.AlgoBaseConfig;
import org.neo4j.graphalgo.config.GraphCreateConfig;
import org.neo4j.graphalgo.config.ModelConfig;
import org.neo4j.graphalgo.core.CypherMapWrapper;
import org.neo4j.graphalgo.core.model.Model;
import org.neo4j.graphalgo.core.model.ModelCatalog;
import org.neo4j.graphalgo.core.utils.mem.AllocationTracker;
import org.neo4j.graphalgo.core.utils.mem.MemoryEstimation;
import org.neo4j.graphalgo.core.utils.progress.ProgressEventTracker;
import org.neo4j.graphalgo.exceptions.MemoryEstimationNotImplementedException;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static org.neo4j.graphalgo.config.ModelConfig.MODEL_NAME_KEY;
import static org.neo4j.graphalgo.config.ModelConfig.MODEL_TYPE_KEY;

public class MultiClassNLRTrainProc extends TrainProc<MultiClassNLRTrain, MultiClassNLRData, MultiClassNLRTrainConfig> {

    // TODO:
    // new result columns
    // train loss, etc

    @Procedure(name = "gds.alpha.ml.node.logisticRegression.train", mode = Mode.READ)
    @Description("Trains a multi-class logistic regression model for a target node property")
    public Stream<TrainResult> train(
        @Name(value = "graphName") Object graphNameOrConfig,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        var result = compute(
            graphNameOrConfig,
            configuration
        );
        ModelCatalog.set(result.result());
        return Stream.of(new TrainResult(result.result(), result.computeMillis()));
    }

    @Override
    protected MultiClassNLRTrainConfig newConfig(
        String username,
        Optional<String> graphName,
        Optional<GraphCreateConfig> maybeImplicitCreate,
        CypherMapWrapper config
    ) {
        return MultiClassNLRTrainConfig.of(username, graphName, maybeImplicitCreate, config);
    }

    @Override
    protected AlgorithmFactory<MultiClassNLRTrain, MultiClassNLRTrainConfig> algorithmFactory() {
        return new AlgorithmFactory<>() {
            @Override
            public MultiClassNLRTrain build(
                Graph graph,
                MultiClassNLRTrainConfig configuration,
                AllocationTracker tracker,
                Log log,
                ProgressEventTracker eventTracker
            ) {
                return new MultiClassNLRTrain(graph, configuration, log);
            }

            @Override
            public MemoryEstimation memoryEstimation(MultiClassNLRTrainConfig configuration) {
                throw new MemoryEstimationNotImplementedException();
            }
        };
    }

    public static class TrainResult {

        public final long trainMillis;
        public final Map<String, Object> modelInfo;
        public final Map<String, Object> configuration;

        public <TRAIN_RESULT, TRAIN_CONFIG extends ModelConfig & AlgoBaseConfig> TrainResult(
            Model<TRAIN_RESULT, TRAIN_CONFIG> trainedModel,
            long trainMillis
        ) {
            TRAIN_CONFIG trainConfig = trainedModel.trainConfig();

            this.modelInfo = new HashMap<>();
            modelInfo.put(MODEL_NAME_KEY, trainedModel.name());
            modelInfo.put(MODEL_TYPE_KEY, trainedModel.algoType());
            this.configuration = trainConfig.toMap();
            this.trainMillis = trainMillis;
        }
    }

}

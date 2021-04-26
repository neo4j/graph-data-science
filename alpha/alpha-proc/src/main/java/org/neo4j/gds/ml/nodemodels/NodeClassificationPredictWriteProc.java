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

import org.neo4j.gds.ml.nodemodels.logisticregression.NodeLogisticRegressionResult;
import org.neo4j.graphalgo.AlgorithmFactory;
import org.neo4j.graphalgo.WriteProc;
import org.neo4j.graphalgo.api.nodeproperties.DoubleArrayNodeProperties;
import org.neo4j.graphalgo.config.GraphCreateConfig;
import org.neo4j.graphalgo.core.CypherMapWrapper;
import org.neo4j.graphalgo.core.write.NodePropertyExporter;
import org.neo4j.graphalgo.result.AbstractResultBuilder;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static org.neo4j.procedure.Mode.WRITE;

public class NodeClassificationPredictWriteProc
    extends WriteProc<NodeClassificationPredict, NodeLogisticRegressionResult, NodeClassificationPredictWriteProc.Result, NodeClassificationPredictWriteConfig> {

    @Procedure(name = "gds.alpha.ml.nodeClassification.predict.write", mode = WRITE)
    @Description("Predicts classes for all nodes based on a previously trained model")
    public Stream<Result> write(
        @Name(value = "graphName") Object graphNameOrConfig,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        var result = compute(graphNameOrConfig, configuration);
        return write(result);
    }

    @Override
    protected List<NodePropertyExporter.NodeProperty> nodePropertyList(ComputationResult<NodeClassificationPredict, NodeLogisticRegressionResult, NodeClassificationPredictWriteConfig> computationResult) {
        var config = computationResult.config();
        var writeProperty = config.writeProperty();
        var result = computationResult.result();
        var classProperties = result.predictedClasses().asNodeProperties();
        var nodeProperties = new ArrayList<NodePropertyExporter.NodeProperty>();
        nodeProperties.add(NodePropertyExporter.NodeProperty.of(writeProperty, classProperties));
        if (result.predictedProbabilities().isPresent()) {
            var probabilityPropertyKey = config.predictedProbabilityProperty().orElseThrow();
            var probabilityProperties = result.predictedProbabilities().get();
            nodeProperties.add(NodePropertyExporter.NodeProperty.of(
                probabilityPropertyKey,
                (DoubleArrayNodeProperties) probabilityProperties::get
            ));
        }
        return nodeProperties;
    }

    @Override
    protected NodeClassificationPredictWriteConfig newConfig(
        String username,
        Optional<String> graphName,
        Optional<GraphCreateConfig> maybeImplicitCreate,
        CypherMapWrapper config
    ) {
        return NodeClassificationPredictWriteConfig.of(username, graphName, maybeImplicitCreate, config);
    }

    @Override
    protected AlgorithmFactory<NodeClassificationPredict, NodeClassificationPredictWriteConfig> algorithmFactory() {
        return new NodeClassificationPredictAlgorithmFactory<>();
    }

    @Override
    protected AbstractResultBuilder<Result> resultBuilder(ComputationResult<NodeClassificationPredict, NodeLogisticRegressionResult, NodeClassificationPredictWriteConfig> computeResult) {
        return new Result.Builder();
    }

    public static class Result {

        public final long writeMillis;
        public final long nodePropertiesWritten;
        public final long createMillis;
        public final long computeMillis;
        public final Map<String, Object> configuration;

        public Result(
            long createMillis,
            long computeMillis,
            long writeMillis,
            long nodePropertiesWritten,
            Map<String, Object> configuration
        ) {
            this.createMillis = createMillis;
            this.computeMillis = computeMillis;
            this.writeMillis = writeMillis;
            this.nodePropertiesWritten = nodePropertiesWritten;
            this.configuration = configuration;
        }
        static class Builder extends AbstractResultBuilder<NodeClassificationPredictWriteProc.Result> {

            @Override
            public NodeClassificationPredictWriteProc.Result build() {
                return new NodeClassificationPredictWriteProc.Result(
                    createMillis,
                    computeMillis,
                    writeMillis,
                    nodePropertiesWritten,
                    config.toMap()
                );
            }
        }
    }


}

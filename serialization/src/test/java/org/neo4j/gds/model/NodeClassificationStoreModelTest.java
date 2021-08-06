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
package org.neo4j.gds.model;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.api.schema.GraphSchema;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.core.model.Model;
import org.neo4j.gds.ml.core.functions.Weights;
import org.neo4j.gds.ml.core.subgraph.LocalIdMap;
import org.neo4j.gds.ml.core.tensor.Matrix;
import org.neo4j.gds.ml.nodemodels.ImmutableModelStats;
import org.neo4j.gds.ml.nodemodels.MetricData;
import org.neo4j.gds.ml.nodemodels.NodeClassificationModelInfo;
import org.neo4j.gds.ml.nodemodels.NodeClassificationTrain;
import org.neo4j.gds.ml.nodemodels.NodeClassificationTrainConfig;
import org.neo4j.gds.ml.nodemodels.logisticregression.NodeLogisticRegressionData;
import org.neo4j.gds.ml.nodemodels.logisticregression.NodeLogisticRegressionTrainConfig;
import org.neo4j.gds.ml.nodemodels.logisticregression.NodeLogisticRegressionTrainConfigImpl;
import org.neo4j.gds.ml.nodemodels.metrics.AllClassMetric;
import org.neo4j.gds.ml.nodemodels.metrics.MetricSpecification;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class NodeClassificationStoreModelTest extends BaseStoreModelTest<NodeLogisticRegressionData, NodeClassificationTrainConfig, NodeClassificationModelInfo> {

    @Test
    @Override
    void testLoadingData() throws IOException {
        var storedModel = new StoredModel(tempDir);

        storedModel.load();

        assertThat(storedModel.loaded()).isTrue();
        assertThat(storedModel.data()).isInstanceOf(NodeLogisticRegressionData.class);
    }

    @Override
    Model<NodeLogisticRegressionData, NodeClassificationTrainConfig, NodeClassificationModelInfo> model() {
        var featureProperties = List.of("a", "b");
        var targetProperty = "t";
        var trainConfig = NodeClassificationTrainConfig.builder()
            .modelName(MODEL)
            .metrics(List.of(MetricSpecification.parse(AllClassMetric.ACCURACY.name())))
            .featureProperties(featureProperties)
            .targetProperty(targetProperty)
            .validationFolds(2)
            .holdoutFraction(0.19)
            .addParam(Map.of("penalty", 1.0))
            .build();

        var modelData = NodeLogisticRegressionData.builder()
            .weights(new Weights<>(Matrix.create(0.19, 3, 4)))
            .classIdMap(new LocalIdMap())
            .build();

        var params = new NodeLogisticRegressionTrainConfigImpl(
            featureProperties,
            targetProperty,
            CypherMapWrapper.create(Map.of("penalty", 1))
        );

        var trainStats = ImmutableModelStats.<NodeLogisticRegressionTrainConfig>of(params, 0.5, 0.0, 1.0);
        var validationStats = ImmutableModelStats.<NodeLogisticRegressionTrainConfig>of(params, 0.4, 0.0, 0.8);
        var metricData = MetricData.of(List.of(trainStats), List.of(validationStats), 4.0, 4.1);

        var info = NodeClassificationModelInfo.of(
            List.of(1L, 2L),
            params,
            Map.of(AllClassMetric.F1_WEIGHTED, metricData)
        );

        return Model.of(
            USER,
            MODEL,
            NodeClassificationTrain.MODEL_TYPE,
            GraphSchema.empty(),
            modelData,
            trainConfig,
            info
        );
    }
}

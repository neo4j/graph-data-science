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
package org.neo4j.gds.core.model;

import com.google.protobuf.GeneratedMessageV3;
import org.neo4j.gds.ModelInfoSerializer;
import org.neo4j.gds.ml.model.proto.CommonML;
import org.neo4j.gds.ml.nodemodels.ImmutableMetricData;
import org.neo4j.gds.ml.nodemodels.ImmutableModelStats;
import org.neo4j.gds.ml.nodemodels.ImmutableNodeClassificationModelInfo;
import org.neo4j.gds.ml.nodemodels.NodeClassificationModelInfo;
import org.neo4j.gds.ml.nodemodels.logisticregression.NodeLogisticRegressionTrainConfig;
import org.neo4j.gds.ml.nodemodels.metrics.Metric;
import org.neo4j.gds.ml.nodemodels.metrics.MetricSpecification;

import java.util.function.BiConsumer;

import static org.neo4j.gds.config.ConfigSerializers.multiClassNLRTrainConfig;

public final class NodeClassificationModelInfoSerializer implements ModelInfoSerializer<NodeClassificationModelInfo> {

    public NodeClassificationModelInfoSerializer() {}


    public GeneratedMessageV3 toSerializable(Model.Mappable mappable) {
        var modelInfo = (NodeClassificationModelInfo) mappable;
        var builder = CommonML.NodeClassificationModelInfo.newBuilder()
            .addAllClasses(modelInfo.classes())
            .setBestParameters(multiClassNLRTrainConfig(modelInfo.bestParameters()));


        ModelInfoSerializer.serializeMetrics(
            modelInfo.metrics(),
            Metric::name,
            (paramsBuilder, modelStats) -> paramsBuilder.setNodeClassificationParams(multiClassNLRTrainConfig(modelStats.params())),
            builder::putMetrics
        );

        return builder.build();
    }

    public NodeClassificationModelInfo fromSerializable(GeneratedMessageV3 generatedMessageV3) {
        var protoModelInfo = (CommonML.NodeClassificationModelInfo) generatedMessageV3;
        var builder = ImmutableNodeClassificationModelInfo.builder()
            .classes(protoModelInfo.getClassesList())
            .bestParameters(multiClassNLRTrainConfig(protoModelInfo.getBestParameters()));

        protoModelInfo.getMetricsMap().forEach((protoMetric, protoMetricData) -> {
            var metric = MetricSpecification
                .parse(protoMetric)
                .createMetrics(protoModelInfo.getClassesList())
                .findFirst()
                .get();

            var metricDataBuilder = ImmutableMetricData.<NodeLogisticRegressionTrainConfig>builder()
                .test(protoMetricData.getTest())
                .outerTrain(protoMetricData.getOuterTrain());

            BiConsumer<ImmutableModelStats.Builder<NodeLogisticRegressionTrainConfig>, CommonML.MetricScores> metricScoresBiConsumer =
                (modelStatsBuilder, protoMetricScores) -> modelStatsBuilder.params(multiClassNLRTrainConfig(
                    protoMetricScores.getNodeClassificationParams()));

            ModelInfoSerializer.deserializeMetricStores(
                protoMetricData.getTrainList(),
                metricDataBuilder::addTrain,
                metricScoresBiConsumer);

            ModelInfoSerializer.deserializeMetricStores(
                protoMetricData.getValidationList(),
                metricDataBuilder::addValidation,
                metricScoresBiConsumer);

            builder.putMetric(
                metric,
                metricDataBuilder.build()
            );
        });

        return builder.build();
    }

    @Override
    public Class<CommonML.NodeClassificationModelInfo> serializableClass() {
        return CommonML.NodeClassificationModelInfo.class;
    }
}

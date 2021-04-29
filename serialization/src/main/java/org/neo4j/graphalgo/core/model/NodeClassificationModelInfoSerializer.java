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
package org.neo4j.graphalgo.core.model;

import org.neo4j.gds.ModelInfoSerializer;
import org.neo4j.gds.ml.nodemodels.ImmutableMetricData;
import org.neo4j.gds.ml.nodemodels.ImmutableModelStats;
import org.neo4j.gds.ml.nodemodels.ImmutableNodeClassificationModelInfo;
import org.neo4j.gds.ml.nodemodels.ModelStats;
import org.neo4j.gds.ml.nodemodels.NodeClassificationModelInfo;
import org.neo4j.gds.ml.nodemodels.metrics.MetricSpecification;
import org.neo4j.gds.ml.nodemodels.logisticregression.NodeLogisticRegressionTrainConfig;
import org.neo4j.graphalgo.ml.model.proto.CommonML;

import static org.neo4j.graphalgo.config.ConfigSerializers.multiClassNLRTrainConfig;

public final class NodeClassificationModelInfoSerializer implements ModelInfoSerializer<NodeClassificationModelInfo, CommonML.NodeClassificationModelInfo> {

    public NodeClassificationModelInfoSerializer() {}

    public CommonML.NodeClassificationModelInfo toSerializable(NodeClassificationModelInfo modelInfo) {
        var builder = CommonML.NodeClassificationModelInfo.newBuilder()
            .addAllClasses(modelInfo.classes())
            .setBestParameters(multiClassNLRTrainConfig(modelInfo.bestParameters()));


        modelInfo.metrics().forEach(((metric, metricData) -> {
            var metricName = metric.name();

            var infoMetricBuilder = CommonML.InfoMetric.newBuilder()
                .setTest(metricData.test())
                .setOuterTrain(metricData.outerTrain());

            metricData.train().forEach(datum -> {
                infoMetricBuilder.addTrain(buildMetricScores(datum));
            });

            metricData.validation().forEach(datum -> {
                infoMetricBuilder.addValidation(buildMetricScores(datum));
            });

            builder.putMetrics(metricName, infoMetricBuilder.build());
        }));


        return builder.build();
    }

    public NodeClassificationModelInfo fromSerializable(CommonML.NodeClassificationModelInfo protoModelInfo) {
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

            protoMetricData.getTrainList().forEach(protoTrain -> {
                metricDataBuilder.addTrain(
                    ImmutableModelStats.<NodeLogisticRegressionTrainConfig>builder()
                        .avg(protoTrain.getAvg())
                        .min(protoTrain.getMin())
                        .max(protoTrain.getMax())
                        .params(multiClassNLRTrainConfig(protoTrain.getNodeClassificationParams()))
                        .build()
                );
            });

            protoMetricData.getValidationList().forEach(protoTrain -> {
                metricDataBuilder.addValidation(
                    ImmutableModelStats.<NodeLogisticRegressionTrainConfig>builder()
                        .avg(protoTrain.getAvg())
                        .min(protoTrain.getMin())
                        .max(protoTrain.getMax())
                        .params(multiClassNLRTrainConfig(protoTrain.getNodeClassificationParams()))
                        .build()
                );
            });

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

    private CommonML.MetricScores.Builder buildMetricScores(ModelStats<NodeLogisticRegressionTrainConfig> datum) {
        return CommonML.MetricScores.newBuilder()
            .setAvg(datum.avg())
            .setMax(datum.max())
            .setMin(datum.min())
            .setNodeClassificationParams(multiClassNLRTrainConfig(datum.params()));
    }
}

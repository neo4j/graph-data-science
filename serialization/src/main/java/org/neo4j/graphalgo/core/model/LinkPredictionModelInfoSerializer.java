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
import org.neo4j.gds.ml.linkmodels.ImmutableLinkPredictionModelInfo;
import org.neo4j.gds.ml.linkmodels.LinkPredictionModelInfo;
import org.neo4j.gds.ml.linkmodels.logisticregression.LinkLogisticRegressionTrainConfig;
import org.neo4j.gds.ml.linkmodels.metrics.LinkMetric;
import org.neo4j.gds.ml.nodemodels.ImmutableMetricData;
import org.neo4j.gds.ml.nodemodels.ImmutableModelStats;
import org.neo4j.gds.ml.nodemodels.ModelStats;
import org.neo4j.graphalgo.ml.model.proto.CommonML;

import static org.neo4j.graphalgo.config.ConfigSerializers.linkLogisticRegressionTrainConfig;

public class LinkPredictionModelInfoSerializer implements ModelInfoSerializer<LinkPredictionModelInfo, CommonML.LinkPredictionModelInfo> {
    @Override
    public CommonML.LinkPredictionModelInfo toSerializable(LinkPredictionModelInfo linkPredictionModelInfo) {
        var builder = CommonML.LinkPredictionModelInfo.newBuilder().setBestParameters(linkLogisticRegressionTrainConfig(linkPredictionModelInfo.bestParameters()));

        linkPredictionModelInfo.metrics().forEach(((metric, metricData) -> {
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

    @Override
    public LinkPredictionModelInfo fromSerializable(CommonML.LinkPredictionModelInfo linkPredictionModelInfo) {
        var builder = ImmutableLinkPredictionModelInfo.builder()
            .bestParameters(linkLogisticRegressionTrainConfig(linkPredictionModelInfo.getBestParameters()));

        linkPredictionModelInfo.getMetricsMap().forEach((protoMetric, protoMetricData) -> {
            var metric = LinkMetric.valueOf(protoMetric);

            var metricDataBuilder = ImmutableMetricData.<LinkLogisticRegressionTrainConfig>builder()
                .test(protoMetricData.getTest())
                .outerTrain(protoMetricData.getOuterTrain());

            protoMetricData.getTrainList().forEach(protoTrain -> {
                metricDataBuilder.addTrain(
                    ImmutableModelStats.<LinkLogisticRegressionTrainConfig>builder()
                        .avg(protoTrain.getAvg())
                        .min(protoTrain.getMin())
                        .max(protoTrain.getMax())
                        .params(linkLogisticRegressionTrainConfig(protoTrain.getLinkPredictionParams()))
                        .build()
                );
            });

            protoMetricData.getValidationList().forEach(protoTrain -> {
                metricDataBuilder.addValidation(
                    ImmutableModelStats.<LinkLogisticRegressionTrainConfig>builder()
                        .avg(protoTrain.getAvg())
                        .min(protoTrain.getMin())
                        .max(protoTrain.getMax())
                        .params(linkLogisticRegressionTrainConfig(protoTrain.getLinkPredictionParams()))
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
    public Class<CommonML.LinkPredictionModelInfo> serializableClass() {
        return CommonML.LinkPredictionModelInfo.class;
    }

    private CommonML.MetricScores.Builder buildMetricScores(ModelStats<LinkLogisticRegressionTrainConfig> datum) {
        return CommonML.MetricScores.newBuilder()
            .setAvg(datum.avg())
            .setMax(datum.max())
            .setMin(datum.min())
            .setLinkPredictionParams(linkLogisticRegressionTrainConfig(datum.params()));
    }
}

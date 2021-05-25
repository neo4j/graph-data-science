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

import com.google.protobuf.GeneratedMessageV3;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.neo4j.gds.ModelInfoSerializer;
import org.neo4j.gds.ml.linkmodels.ImmutableLinkPredictionModelInfo;
import org.neo4j.gds.ml.linkmodels.LinkPredictionModelInfo;
import org.neo4j.gds.ml.linkmodels.logisticregression.LinkLogisticRegressionTrainConfig;
import org.neo4j.gds.ml.linkmodels.metrics.LinkMetric;
import org.neo4j.gds.ml.nodemodels.ImmutableMetricData;
import org.neo4j.gds.ml.nodemodels.ImmutableModelStats;
import org.neo4j.graphalgo.ml.model.proto.CommonML;

import java.util.function.BiConsumer;

import static org.neo4j.graphalgo.config.ConfigSerializers.linkLogisticRegressionTrainConfig;

@SuppressFBWarnings("BC_UNCONFIRMED_CAST")
public class LinkPredictionModelInfoSerializer implements ModelInfoSerializer/*<LinkPredictionModelInfo, CommonML.LinkPredictionModelInfo>*/ {

    public GeneratedMessageV3 toSerializable(Model.Mappable mappable) {
        LinkPredictionModelInfo linkPredictionModelInfo = (LinkPredictionModelInfo) mappable;
        var builder = CommonML.LinkPredictionModelInfo.newBuilder()
            .setBestParameters(linkLogisticRegressionTrainConfig(linkPredictionModelInfo.bestParameters()));

        ModelInfoSerializer.serializeMetrics(
            linkPredictionModelInfo.metrics(),
            Enum::name,
            (paramsBuilder, modelStats) -> paramsBuilder.setLinkPredictionParams(linkLogisticRegressionTrainConfig(modelStats.params())),
            builder::putMetrics
        );

        return builder.build();
    }

    public Model.Mappable fromSerializable(GeneratedMessageV3 generatedMessageV3) {
        var linkPredictionModelInfo = (CommonML.LinkPredictionModelInfo) generatedMessageV3;
        var builder = ImmutableLinkPredictionModelInfo.builder()
            .bestParameters(linkLogisticRegressionTrainConfig(linkPredictionModelInfo.getBestParameters()));

        linkPredictionModelInfo.getMetricsMap().forEach((protoMetric, protoMetricData) -> {
            var metric = LinkMetric.valueOf(protoMetric);

            var metricDataBuilder = ImmutableMetricData.<LinkLogisticRegressionTrainConfig>builder()
                .test(protoMetricData.getTest())
                .outerTrain(protoMetricData.getOuterTrain());

            BiConsumer<ImmutableModelStats.Builder<LinkLogisticRegressionTrainConfig>, CommonML.MetricScores> metricScoresBiConsumer =
                (modelStatsBuilder, protoMetricScores) -> modelStatsBuilder.params(linkLogisticRegressionTrainConfig(
                    protoMetricScores.getLinkPredictionParams()));

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
    public Class<CommonML.LinkPredictionModelInfo> serializableClass() {
        return CommonML.LinkPredictionModelInfo.class;
    }
}

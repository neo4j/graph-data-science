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
package org.neo4j.gds;

import com.google.protobuf.Any;
import com.google.protobuf.GeneratedMessageV3;
import com.google.protobuf.InvalidProtocolBufferException;
import org.neo4j.gds.core.model.Model;
import org.neo4j.gds.ml.TrainingConfig;
import org.neo4j.gds.ml.model.proto.CommonML;
import org.neo4j.gds.ml.nodemodels.ImmutableModelStats;
import org.neo4j.gds.ml.nodemodels.MetricData;
import org.neo4j.gds.ml.nodemodels.ModelStats;

import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;

public interface ModelInfoSerializer<CUSTOM_INFO extends Model.Mappable> {

    GeneratedMessageV3 toSerializable(Model.Mappable mappable);

    CUSTOM_INFO fromSerializable(GeneratedMessageV3 generatedMessageV3);

    Class<? extends GeneratedMessageV3> serializableClass();

    default CUSTOM_INFO fromSerializable(Any protoModelInfo) {
        try {
            var unpacked = protoModelInfo.unpack(serializableClass());
            return fromSerializable(unpacked);
        } catch (InvalidProtocolBufferException e) {
            throw new RuntimeException(e);
        }
    }

    static <M, C extends TrainingConfig> void serializeMetrics(
        Map<M, MetricData<C>> metrics,
        Function<M, String> metricNameFunction,
        BiConsumer<CommonML.MetricScores.Builder, ModelStats<C>> paramsConsumer,
        BiConsumer<String, CommonML.InfoMetric> metricBiConsumer
    ) {
        metrics.forEach(((metric, metricData) -> {
            var metricName = metricNameFunction.apply(metric);

            var infoMetricBuilder = CommonML.InfoMetric.newBuilder()
                .setTest(metricData.test())
                .setOuterTrain(metricData.outerTrain());

            metricData.train().forEach(datum -> {
                infoMetricBuilder.addTrain(buildMetricScores(datum, paramsConsumer));
            });

            metricData.validation().forEach(datum -> {
                infoMetricBuilder.addValidation(buildMetricScores(datum, paramsConsumer));
            });

            metricBiConsumer.accept(metricName, infoMetricBuilder.build());
        }));
    }

    static <C extends TrainingConfig> void deserializeMetricStores(
        List<CommonML.MetricScores> metricScores,
        Consumer<ModelStats<C>> modelStatsConsumer,
        BiConsumer<ImmutableModelStats.Builder<C>, CommonML.MetricScores> paramsConsumer
    ) {
        metricScores.forEach(protoTrain -> modelStatsConsumer.accept(deserializeModelStats(protoTrain, paramsConsumer)));
    }

    static <C extends TrainingConfig> ModelStats<C> deserializeModelStats(
        CommonML.MetricScores protoTrain,
        BiConsumer<ImmutableModelStats.Builder<C>, CommonML.MetricScores> paramsConsumer
    ) {
        var builder = ImmutableModelStats.<C>builder()
            .avg(protoTrain.getAvg())
            .min(protoTrain.getMin())
            .max(protoTrain.getMax());
        paramsConsumer.accept(builder, protoTrain);
        return builder.build();
    }

    private static <C extends TrainingConfig> CommonML.MetricScores.Builder buildMetricScores(
        ModelStats<C> modelStats,
        BiConsumer<CommonML.MetricScores.Builder, ModelStats<C>> paramsConsumer
    ) {
        var builder = CommonML.MetricScores.newBuilder()
            .setAvg(modelStats.avg())
            .setMax(modelStats.max())
            .setMin(modelStats.min());
        paramsConsumer.accept(builder, modelStats);
        return builder;
    }
}

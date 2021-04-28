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
package org.neo4j.gds.ml.linkmodels;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.ml.linkmodels.logisticregression.LinkLogisticRegressionTrainConfig;
import org.neo4j.gds.ml.linkmodels.logisticregression.LinkLogisticRegressionTrainConfigImpl;
import org.neo4j.gds.ml.linkmodels.metrics.LinkMetric;
import org.neo4j.gds.ml.nodemodels.ImmutableModelStats;
import org.neo4j.gds.ml.nodemodels.MetricData;
import org.neo4j.graphalgo.core.CypherMapWrapper;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class LinkPredictionModelInfoTest {

    @Test
    void shouldCreateMapWithStats() {
        LinkLogisticRegressionTrainConfig trainConfig = new LinkLogisticRegressionTrainConfigImpl(List.of(), CypherMapWrapper.empty().withNumber("penalty", 1));
        var trainStats = ImmutableModelStats.of(trainConfig, 0.5, 0.0, 1.0);
        var validationStats = ImmutableModelStats.of(trainConfig, 0.4, 0.0, 0.8);
        var metricData = MetricData.of(List.of(trainStats), List.of(validationStats), 4.0, 4.1);
        var info = LinkPredictionModelInfo.of(
            trainConfig,
            Map.of(LinkMetric.AUCPR, metricData)
        );

        var expected = Map.of(
            "bestParameters", trainConfig.toMap(),
            "metrics", Map.of(
                "AUCPR", Map.of(
                    "outerTrain", 4.0,
                    "test", 4.1,
                    "train", List.of(Map.of(
                        "avg", 0.5,
                        "max", 1.0,
                        "min", 0.0,
                        "params", trainConfig.toMap()
                    )),
                    "validation", List.of(Map.of(
                        "avg", 0.4,
                        "max", 0.8,
                        "min", 0.0,
                        "params", trainConfig.toMap()
                    ))
                )
            )
        );
        assertThat(info.toMap()).containsExactlyInAnyOrderEntriesOf(expected);
    }

}

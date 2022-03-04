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
package org.neo4j.gds.ml.pipeline.linkPipeline.train;

import org.neo4j.gds.mem.MemoryUsage;
import org.neo4j.gds.ml.linkmodels.metrics.LinkMetric;
import org.neo4j.gds.models.logisticregression.LogisticRegressionTrainConfig;
import org.neo4j.gds.ml.nodemodels.ImmutableModelStats;
import org.neo4j.gds.ml.nodemodels.ModelStats;

import java.util.EnumMap;
import java.util.HashMap;
import java.util.Map;

import static org.neo4j.gds.mem.MemoryUsage.sizeOfInstance;

class LinkModelStatsBuilder {
    private final Map<LinkMetric, Double> min;
    private final Map<LinkMetric, Double> max;
    private final Map<LinkMetric, Double> sum;
    private final LogisticRegressionTrainConfig modelParams;
    private final int numberOfSplits;

    LinkModelStatsBuilder(LogisticRegressionTrainConfig modelParams, int numberOfSplits) {
        this.modelParams = modelParams;
        this.numberOfSplits = numberOfSplits;
        this.min = new EnumMap<>(LinkMetric.class);
        this.max = new EnumMap<>(LinkMetric.class);
        this.sum = new EnumMap<>(LinkMetric.class);
    }

    static long sizeInBytes(long numberOfMetrics) {
        int numberOfStats = 3;
        long statsMapEntries = numberOfStats * (sizeOfInstance(HashMap.class) + numberOfMetrics * Double.BYTES);
        return MemoryUsage.sizeOfInstance(LinkModelStatsBuilder.class) + statsMapEntries;
    }

    void update(LinkMetric metric, double value) {
        min.merge(metric, value, Math::min);
        max.merge(metric, value, Math::max);
        sum.merge(metric, value, Double::sum);
    }

    ModelStats<LogisticRegressionTrainConfig> modelStats(LinkMetric metric) {
        return ImmutableModelStats.of(
            modelParams,
            sum.get(metric) / numberOfSplits,
            min.get(metric),
            max.get(metric)
        );
    }
}

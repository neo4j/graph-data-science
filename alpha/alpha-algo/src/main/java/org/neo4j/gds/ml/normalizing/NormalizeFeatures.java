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
package org.neo4j.gds.ml.normalizing;

import org.neo4j.graphalgo.Algorithm;
import org.neo4j.graphalgo.annotation.ValueClass;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.config.AlgoBaseConfig;
import org.neo4j.graphalgo.config.FeaturePropertiesConfig;
import org.neo4j.graphalgo.core.utils.mem.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.HugeObjectArray;

public class NormalizeFeatures extends Algorithm<NormalizeFeatures, NormalizeFeatures.NormalizeFeaturesResult> {

    private final Graph graph;
    private final NormalizeFeaturesConfig config;
    private final AllocationTracker tracker;

    public NormalizeFeatures(Graph graph, NormalizeFeaturesConfig config, AllocationTracker tracker) {
        this.graph = graph;
        this.config = config;
        this.tracker = tracker;
    }

    @Override
    public NormalizeFeaturesResult compute() {
        var normalizedProperties = HugeObjectArray.newArray(double[].class, graph.nodeCount(), tracker);
        config.featureProperties().forEach(featureProperty ->
            normalizedProperties.setAll(value -> new double[]{graph.nodeProperties(featureProperty).doubleValue(value)})
        );

        return NormalizeFeaturesResult.of(normalizedProperties);
    }

    @Override
    public NormalizeFeatures me() {
        return this;
    }

    @Override
    public void release() {}

    @ValueClass
    interface NormalizeFeaturesResult {
        HugeObjectArray<double[]> normalizedProperties();

        static NormalizeFeaturesResult of(HugeObjectArray<double[]> properties) {
            return ImmutableNormalizeFeaturesResult.of(properties);
        }
    }

    @ValueClass
    interface NormalizeFeaturesConfig extends AlgoBaseConfig, FeaturePropertiesConfig {
    }
}

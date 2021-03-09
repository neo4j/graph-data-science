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

import java.util.ArrayList;
import java.util.List;

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
        normalizedProperties.setAll(nodeId -> {
            var propertyCount = config.featureProperties().size();
            var resultProperties = new double[propertyCount];
            for (int i = 0; i < propertyCount; i++) {
                var normalizer = resolveNormalizers().get(i);
                var afterValue = normalizer.normalize(nodeId);
                resultProperties[i] = afterValue;
            }
            return resultProperties;
        });

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

    private List<Normalizer> resolveNormalizers() {
        assert config.normalizers().size() == config.featureProperties().size();

        List<Normalizer> normalizers = new ArrayList<>();

        for (int i = 0; i < config.normalizers().size(); i++) {
            String normalizer = config.normalizers().get(i);
            String property = config.featureProperties().get(i);

            var nodeProperties = graph.nodeProperties(property);
            normalizers.add(Normalizer.Factory.create(normalizer, nodeProperties, graph.nodeCount()));
        }
        return normalizers;
    }

    @ValueClass
    interface NormalizeFeaturesConfig extends AlgoBaseConfig, FeaturePropertiesConfig {
        List<String> normalizers();
    }
}

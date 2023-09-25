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
package org.neo4j.gds.ml.models;

import org.neo4j.gds.api.Graph;
import org.neo4j.gds.collections.ha.HugeObjectArray;
import org.neo4j.gds.ml.core.features.FeatureConsumer;
import org.neo4j.gds.ml.core.features.FeatureExtraction;

import java.util.List;

public final class FeaturesFactory {
    private FeaturesFactory() {}

    public static Features extractLazyFeatures(Graph graph, List<String> featureProperties) {
        var featureExtractors = FeatureExtraction.propertyExtractors(graph, featureProperties);
        var numberOfFeatures = FeatureExtraction.featureCount(featureExtractors);

        Features features = new Features() {
            @Override
            public long size() {
                return graph.nodeCount();
            }

            @Override
            public double[] get(long id) {
                var features = new double[numberOfFeatures];
                FeatureExtraction.extract(id, 0, featureExtractors, new FeatureConsumer() {
                    @Override
                    public void acceptScalar(long nodeOffset, int offset, double value) {
                        features[offset] = value;
                    }

                    @Override
                    public void acceptArray(long nodeOffset, int offset, double[] values) {
                        System.arraycopy(values, 0, features, offset, values.length);
                    }
                });
                return features;
            }

            @Override
            public int featureDimension() {
                return numberOfFeatures;
            }
        };

        return features;
    }

    public static Features extractEagerFeatures(Graph graph, List<String> featureProperties) {
        var featureExtractors = FeatureExtraction.propertyExtractors(graph, featureProperties);
        var featuresArray = HugeObjectArray.newArray(double[].class, graph.nodeCount());

        FeatureExtraction.extract(graph, featureExtractors, featuresArray);

        return wrap(featuresArray);
    }

    public static Features wrap(HugeObjectArray<double[]> features) {
        return new Features() {
            @Override
            public long size() {
                return features.size();
            }

            @Override
            public double[] get(long id) {
                return features.get(id);
            }
        };
    }

    public static Features wrap(double[] features) {
        return new Features() {
            @Override
            public long size() {
                return 1;
            }

            @Override
            public double[] get(long id) {
                assert id == 0;
                return features;
            }
        };
    }

    public static Features wrap(List<double[]> features) {
        return new Features() {
            @Override
            public long size() {
                return features.size();
            }

            @Override
            public double[] get(long id) {
                return features.get(Math.toIntExact(id));
            }
        };
    }

}

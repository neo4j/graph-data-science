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
package org.neo4j.gds.ml.features;

import org.neo4j.gds.embeddings.EmbeddingUtils;
import org.neo4j.gds.embeddings.graphsage.ddl4j.functions.MatrixConstant;
import org.neo4j.gds.ml.batch.Batch;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.nodeproperties.ValueType;
import org.neo4j.graphalgo.core.utils.paged.HugeObjectArray;

import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import static org.neo4j.graphalgo.utils.StringFormatting.formatWithLocale;

/**
 * Responsible for extracting features into abstract consumers (FeatureConsumer).
 * Also contains logic for looping on graphs and batches and writing into
 * Matrices and HugeObjectArrays.
 */
public final class FeatureExtraction {

    private FeatureExtraction() {}

    public static void extract(
        long nodeId,
        long nodeOffset,
        List<FeatureExtractor> extractors,
        FeatureConsumer consumer
    ) {
        int offset = 0;
        for (FeatureExtractor extractor : extractors) {
            if (extractor instanceof ScalarFeatureExtractor) {
                consumer.acceptScalar(nodeOffset, offset, ((ScalarFeatureExtractor) extractor).extract(nodeId));
            } else if (extractor instanceof ArrayFeatureExtractor) {
                consumer.acceptArray(nodeOffset, offset, ((ArrayFeatureExtractor) extractor).extract(nodeId));
            } else {
                throw new IllegalStateException("Only ScalarFeatureExtractor and ArrayFeatureExtractor are handled");
            }
            offset += extractor.dimension();
        }
    }

    public static MatrixConstant extract(Batch batch, List<FeatureExtractor> extractors) {
        int rows = batch.size();
        int cols = featureCount(extractors);
        double[] features = new double[rows * cols];
        FeatureConsumer featureConsumer = new FeatureConsumer() {
            @Override
            public void acceptScalar(long nodeOffset, int offset, double value) {
                features[(int)nodeOffset * cols + offset] = value;
            }

            @Override
            public void acceptArray(long nodeOffset, int offset, double[] values) {
                System.arraycopy(values, 0, features, (int)nodeOffset * cols + offset, values.length);
            }
        };
        int nodeOffset = 0;
        for (long nodeId : batch.nodeIds()) {
            extract(nodeId, nodeOffset, extractors, featureConsumer);
            nodeOffset++;
        }
        return new MatrixConstant(features, rows, cols);
    }

    public static HugeObjectArray<double[]> extract(
        Graph graph,
        List<FeatureExtractor> extractors,
        HugeObjectArray<double[]> features
    ) {
        int featureCount = featureCount(extractors);
        features.setAll(i -> new double[featureCount]);
        var featureConsumer = new HugeObjectArrayFeatureConsumer(features);
        graph.forEachNode(nodeId -> {
            extract(nodeId, nodeId, extractors, featureConsumer);
            return true;
        });
        return features;
    }

    public static int featureCount(Collection<FeatureExtractor> extractors) {
        return extractors.stream().mapToInt(FeatureExtractor::dimension).sum();
    }

    public static List<FeatureExtractor> propertyExtractors(Graph graph, Collection<String> featureProperties) {
        return propertyExtractors(graph, featureProperties, 0);
    }

    public static List<FeatureExtractor> propertyExtractors(Graph graph, Collection<String> featureProperties, long initNodeId) {
        return featureProperties.stream()
            .map(propertyKey -> {
                var property = graph.nodeProperties(propertyKey);
                var propertyType = property.valueType();
                if ((ValueType.DOUBLE_ARRAY == propertyType) || (ValueType.FLOAT_ARRAY == propertyType)) {
                    var propertyValues = EmbeddingUtils.getCheckedDoubleArrayNodeProperty(
                        graph,
                        propertyKey,
                        initNodeId
                    );
                    return new ArrayPropertyExtractor(propertyValues.length, graph, propertyKey);
                }
                if (ValueType.LONG_ARRAY == propertyType) {
                    var propertyValues = EmbeddingUtils.getCheckedLongArrayNodeProperty(
                        graph,
                        propertyKey,
                        initNodeId
                    );
                    return new LongArrayPropertyExtractor(propertyValues.length, graph, propertyKey);
                } else if ((ValueType.DOUBLE == propertyType) || (ValueType.LONG == propertyType)) {
                    return new ScalarPropertyExtractor(graph, propertyKey);
                } else {
                    throw new IllegalStateException(formatWithLocale("Unknown ValueType %s", propertyType));
                }
            }).collect(Collectors.toList());
    }

    public static int featureCountWithBias(Graph graph, List<String> featureProperties) {
        var featureExtractors = propertyExtractors(graph, featureProperties);
        featureExtractors.add(new BiasFeature());
        return featureCount(featureExtractors);
    }
}


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
package org.neo4j.gds.ml.linkmodels.pipeline.linkFeatures.linkfunctions;

import org.apache.commons.lang3.mutable.MutableLong;
import org.jetbrains.annotations.TestOnly;
import org.neo4j.gds.ml.linkmodels.pipeline.linkFeatures.LinkFeatureStep;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.NodeProperties;
import org.neo4j.graphalgo.core.utils.paged.HugeObjectArray;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.neo4j.graphalgo.utils.StringFormatting.formatWithLocale;

public class CosineFeatureStep implements LinkFeatureStep {

    private final List<String> featureProperties;

    public CosineFeatureStep(List<String> featureProperties) {
        this.featureProperties = featureProperties;
    }

    public CosineFeatureStep(Map<String, Object> config) {
        this((List<String>) config.get(LinkFeatureStep.FEATURE_PROPERTIES));
    }

    public static void validateConfig(Map<String, Object> config) {
        LinkFeatureStepValidation.validateConfig("Cosine link feature", config);
    }

    @TestOnly
    public List<String> featureProperties() {
        return featureProperties;
    }

    @Override
    public void addFeatures(Graph graph, HugeObjectArray<double[]> linkFeatures, int featureOffset) {
        var currentRelationshipOffset = new MutableLong(0);

        var nodeProperties = featureProperties.stream().map(graph::nodeProperties).collect(Collectors.toList());

        graph.forEachNode(nodeId -> {
            graph.forEachRelationship(nodeId, ((sourceNodeId, targetNodeId) -> {
                double[] linkFeature = linkFeatures.get(currentRelationshipOffset.getAndIncrement());
                addFeature(sourceNodeId, targetNodeId, nodeProperties, featureOffset, linkFeature);
                return true;
            }));

            return true;
        });
    }

    public void addFeature(long sourceNodeId, long targetNodeId, List<NodeProperties> nodeProperties, int startOffset, double[] linkFeature) {
        var sourceSquareNorm = 0.0;
        var targetSquareNorm = 0.0;



        for (NodeProperties props : nodeProperties) {
            var propertyType = props.valueType();
            switch (propertyType) {
                case DOUBLE_ARRAY:
                case FLOAT_ARRAY: {
                    var sourceArrayPropValues = props.doubleArrayValue(sourceNodeId);
                    var targetArrayPropValues = props.doubleArrayValue(targetNodeId);
                    assert sourceArrayPropValues.length == targetArrayPropValues.length;
                    for (int i = 0; i < sourceArrayPropValues.length; i++) {
                        linkFeature[startOffset] += sourceArrayPropValues[i] * targetArrayPropValues[i];
                        sourceSquareNorm += sourceArrayPropValues[i] * sourceArrayPropValues[i];
                        targetSquareNorm += targetArrayPropValues[i] * targetArrayPropValues[i];
                    }
                    break;
                }
                case LONG_ARRAY: {
                    var sourceArrayPropValues = props.longArrayValue(sourceNodeId);
                    var targetArrayPropValues = props.longArrayValue(targetNodeId);
                    assert sourceArrayPropValues.length == targetArrayPropValues.length;
                    for (int i = 0; i < sourceArrayPropValues.length; i++) {
                        linkFeature[startOffset] += sourceArrayPropValues[i] * targetArrayPropValues[i];
                        sourceSquareNorm += sourceArrayPropValues[i] * sourceArrayPropValues[i];
                        targetSquareNorm += targetArrayPropValues[i] * targetArrayPropValues[i];
                    }
                    break;
                }
                case LONG:
                case DOUBLE: {
                    linkFeature[startOffset] += props.doubleValue(sourceNodeId) * props.doubleValue(targetNodeId);
                    sourceSquareNorm += props.doubleValue(sourceNodeId) * props.doubleValue(sourceNodeId);
                    targetSquareNorm += props.doubleValue(targetNodeId) * props.doubleValue(targetNodeId);
                    break;
                }
                case UNKNOWN:
                    throw new IllegalStateException(formatWithLocale("Unknown ValueType %s", propertyType));
            }
        }
        linkFeature[startOffset] /= Math.sqrt(sourceSquareNorm * targetSquareNorm);
    }

    @Override
    public int outputFeatureSize(Graph graph) {
        return 1;
    }

    @Override
    public List<String> inputNodeProperties() {
        return featureProperties;
    }
}

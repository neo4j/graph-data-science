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

import org.jetbrains.annotations.TestOnly;
import org.neo4j.gds.ml.linkmodels.pipeline.linkFeatures.LinkFeatureAppender;
import org.neo4j.gds.ml.linkmodels.pipeline.linkFeatures.LinkFeatureStep;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.NodeProperties;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

public class CosineFeatureStep implements LinkFeatureStep {

    private final List<String> nodeProperties;

    public CosineFeatureStep(List<String> nodeProperties) {
        this.nodeProperties = nodeProperties;
    }

    public CosineFeatureStep(Map<String, Object> config) {
        this((List<String>) config.get(LinkFeatureStep.INPUT_NODE_PROPERTIES));
    }

    public static void validateConfig(Map<String, Object> config) {
        LinkFeatureStepValidation.validateConfig("Cosine link feature", config);
    }

    @TestOnly
    public List<String> nodeProperties() {
        return nodeProperties;
    }

    @Override
    public LinkFeatureAppender linkFeatureAppender(Graph graph) {
        var nodeProperties = this.nodeProperties.stream().map(graph::nodeProperties).collect(Collectors.toList());
        return (source, target, linkFeatures, offset) -> {
            var sourceSquareNorm = 0.0;
            var targetSquareNorm = 0.0;

            for (NodeProperties props : nodeProperties) {
                var propertyType = props.valueType();
                switch (propertyType) {
                    case DOUBLE_ARRAY:
                    case FLOAT_ARRAY: {
                        var sourceArrayPropValues = props.doubleArrayValue(source);
                        var targetArrayPropValues = props.doubleArrayValue(target);
                        assert sourceArrayPropValues.length == targetArrayPropValues.length;
                        for (int i = 0; i < sourceArrayPropValues.length; i++) {
                            linkFeatures[offset] += sourceArrayPropValues[i] * targetArrayPropValues[i];
                            sourceSquareNorm += sourceArrayPropValues[i] * sourceArrayPropValues[i];
                            targetSquareNorm += targetArrayPropValues[i] * targetArrayPropValues[i];
                        }
                        break;
                    }
                    case LONG_ARRAY: {
                        var sourceArrayPropValues = props.longArrayValue(source);
                        var targetArrayPropValues = props.longArrayValue(target);
                        assert sourceArrayPropValues.length == targetArrayPropValues.length;
                        for (int i = 0; i < sourceArrayPropValues.length; i++) {
                            linkFeatures[offset] += sourceArrayPropValues[i] * targetArrayPropValues[i];
                            sourceSquareNorm += sourceArrayPropValues[i] * sourceArrayPropValues[i];
                            targetSquareNorm += targetArrayPropValues[i] * targetArrayPropValues[i];
                        }
                        break;
                    }
                    case LONG:
                    case DOUBLE: {
                        linkFeatures[offset] += props.doubleValue(source) * props.doubleValue(target);
                        sourceSquareNorm += props.doubleValue(source) * props.doubleValue(source);
                        targetSquareNorm += props.doubleValue(target) * props.doubleValue(target);
                        break;
                    }
                    case UNKNOWN:
                        throw new IllegalStateException(formatWithLocale("Unknown ValueType %s", propertyType));
                }
            }
            linkFeatures[offset] /= Math.sqrt(sourceSquareNorm * targetSquareNorm);
        };
    }

    @Override
    public int outputFeatureDimension(Graph graph) {
        return 1;
    }

    @Override
    public List<String> inputNodeProperties() {
        return nodeProperties;
    }
}

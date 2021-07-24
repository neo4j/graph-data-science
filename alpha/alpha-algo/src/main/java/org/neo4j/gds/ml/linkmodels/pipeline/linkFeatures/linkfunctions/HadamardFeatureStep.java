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

import static org.neo4j.graphalgo.utils.StringFormatting.formatWithLocale;

public class HadamardFeatureStep implements LinkFeatureStep {

    private final List<String> featureProperties;

    public HadamardFeatureStep(List<String> featureProperties) {
        this.featureProperties = featureProperties;
    }

    public HadamardFeatureStep(Map<String, Object> config) {
        this((List<String>) config.get(LinkFeatureStep.FEATURE_PROPERTIES));
    }

    public static void validateConfig(Map<String, Object> config) {
        LinkFeatureStepValidation.validateConfig("Hadamard link feature", config);
    }

    @TestOnly
    public List<String> featureProperties() {
        return featureProperties;
    }

    @Override
    public LinkFeatureAppender linkFeatureAppender(Graph graph) {
        var properties = featureProperties.stream().map(graph::nodeProperties).collect(Collectors.toList());
        return (source, target, linkFeatures, offset) -> {
            var offset1 = offset;
            for (NodeProperties props : properties) {
                var propertyType = props.valueType();
                switch (propertyType) {
                    case DOUBLE_ARRAY:
                    case FLOAT_ARRAY: {
                        var sourceArrayPropValues = props.doubleArrayValue(source);
                        var targetArrayPropValues = props.doubleArrayValue(target);
                        assert sourceArrayPropValues.length == targetArrayPropValues.length;
                        for (int i = 0; i < sourceArrayPropValues.length; i++) {
                            linkFeatures[offset1++] = sourceArrayPropValues[i] * targetArrayPropValues[i];
                        }
                        break;
                    }
                    case LONG_ARRAY: {
                        var sourceArrayPropValues = props.longArrayValue(source);
                        var targetArrayPropValues = props.longArrayValue(target);
                        assert sourceArrayPropValues.length == targetArrayPropValues.length;
                        for (int i = 0; i < sourceArrayPropValues.length; i++) {
                            linkFeatures[offset1++] = sourceArrayPropValues[i] * targetArrayPropValues[i];
                        }
                        break;
                    }
                    case LONG:
                    case DOUBLE:
                        linkFeatures[offset1++] = props.doubleValue(source) * props.doubleValue(target);
                        break;
                    case UNKNOWN:
                        throw new IllegalStateException(formatWithLocale("Unknown ValueType %s", propertyType));
                }
            }
        };
    }

    @Override
    public int outputFeatureSize(Graph graph) {
        return FeatureStepUtil.totalPropertyDimension(graph, featureProperties);
    }

    @Override
    public List<String> inputNodeProperties() {
        return featureProperties;
    }
}

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
package org.neo4j.gds.ml.pipeline.linkPipeline.linkfunctions;

import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.properties.nodes.NodePropertyValues;
import org.neo4j.gds.ml.pipeline.FeatureStepUtil;
import org.neo4j.gds.ml.pipeline.linkPipeline.LinkFeatureAppender;
import org.neo4j.gds.ml.pipeline.linkPipeline.LinkFeatureStep;
import org.neo4j.gds.ml.pipeline.linkPipeline.LinkFeatureStepFactory;

import java.util.List;
import java.util.Map;

import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

public class HadamardFeatureStep implements LinkFeatureStep {

    private final List<String> nodeProperties;

    public HadamardFeatureStep(List<String> nodeProperties) {
        this.nodeProperties = nodeProperties;
    }

    @Override
    public LinkFeatureAppender linkFeatureAppender(Graph graph) {
        var appenderPerProperty = new LinkFeatureAppender[nodeProperties.size()];

        for (int idx = 0, nodePropertiesSize = nodeProperties.size(); idx < nodePropertiesSize; idx++) {
            String propertyName = nodeProperties.get(idx);
            var props = graph.nodeProperties(propertyName);
            var propertyType = props.valueType();

            var dimension = FeatureStepUtil.propertyDimension(graph, propertyName);

            switch (propertyType) {
                case DOUBLE_ARRAY:
                    appenderPerProperty[idx] = new HadamardDoubleArrayFeatureAppender(props, dimension);
                    break;
                case FLOAT_ARRAY:
                    appenderPerProperty[idx] = new HadamardFloatArrayFeatureAppender(props, dimension);
                    break;
                case LONG_ARRAY:
                    appenderPerProperty[idx] = new HadamardLongArrayFeatureAppender(props, dimension);
                    break;
                case LONG:
                    appenderPerProperty[idx] = new HadamardFeatureStep.HadamardLongFeatureAppender(props, dimension);
                case DOUBLE:
                    appenderPerProperty[idx] = new HadamardFeatureStep.HadamardDoubleFeatureAppender(props, dimension);
                    break;
                case UNKNOWN:
                    throw new IllegalStateException(formatWithLocale("Unknown ValueType %s", propertyType));
            }
        }

        return new UnionLinkFeatureAppender(appenderPerProperty, name(), nodeProperties);
    }

    @Override
    public List<String> inputNodeProperties() {
        return nodeProperties;
    }

    @Override
    public Map<String, Object> configuration() {
        return Map.of("nodeProperties", nodeProperties);
    }

    @Override
    public String name() {
        return LinkFeatureStepFactory.HADAMARD.name();
    }

    private static class HadamardDoubleArrayFeatureAppender extends SinglePropertyFeatureAppender {
        HadamardDoubleArrayFeatureAppender(NodePropertyValues props, int dimension) {
            super(props, dimension);
        }

        @Override
        public void appendFeatures(long source, long target, double[] linkFeatures, int offset) {
            var sourceArrayPropValues = props.doubleArrayValue(source);
            var targetArrayPropValues = props.doubleArrayValue(target);
            assert sourceArrayPropValues.length == targetArrayPropValues.length;
            for (int i = 0; i < sourceArrayPropValues.length; i++) {
                linkFeatures[offset++] = sourceArrayPropValues[i] * targetArrayPropValues[i];
            }
        }
    }

    private static class HadamardFloatArrayFeatureAppender extends SinglePropertyFeatureAppender {
        HadamardFloatArrayFeatureAppender(NodePropertyValues props, int dimension) {
            super(props, dimension);
        }

        @Override
        public void appendFeatures(long source, long target, double[] linkFeatures, int offset) {
            var sourceArrayPropValues = props.floatArrayValue(source);
            var targetArrayPropValues = props.floatArrayValue(target);
            assert sourceArrayPropValues.length == targetArrayPropValues.length;
            for (int i = 0; i < sourceArrayPropValues.length; i++) {
                linkFeatures[offset++] = sourceArrayPropValues[i] * targetArrayPropValues[i];
            }
        }
    }

    private static class HadamardLongArrayFeatureAppender extends SinglePropertyFeatureAppender {
        HadamardLongArrayFeatureAppender(NodePropertyValues props, int dimension) {
            super(props, dimension);
        }

        @Override
        public void appendFeatures(long source, long target, double[] linkFeatures, int offset) {
            var sourceArrayPropValues = props.longArrayValue(source);
            var targetArrayPropValues = props.longArrayValue(target);
            assert sourceArrayPropValues.length == targetArrayPropValues.length;
            for (int i = 0; i < sourceArrayPropValues.length; i++) {
                linkFeatures[offset++] = sourceArrayPropValues[i] * targetArrayPropValues[i];
            }
        }
    }

    private static class HadamardLongFeatureAppender extends SinglePropertyFeatureAppender {
        HadamardLongFeatureAppender(NodePropertyValues props, int dimension) {
            super(props, dimension);
        }

        @Override
        public void appendFeatures(long source, long target, double[] linkFeatures, int offset) {
            linkFeatures[offset] = props.longValue(source) * props.longValue(target);
        }
    }

    private static class HadamardDoubleFeatureAppender extends SinglePropertyFeatureAppender {
        HadamardDoubleFeatureAppender(NodePropertyValues props, int dimension) {
            super(props, dimension);
        }

        @Override
        public void appendFeatures(long source, long target, double[] linkFeatures, int offset) {
            linkFeatures[offset] = props.doubleValue(source) * props.doubleValue(target);
        }
    }
}

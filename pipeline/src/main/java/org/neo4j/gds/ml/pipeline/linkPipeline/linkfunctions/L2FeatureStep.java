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
import org.neo4j.gds.ml.pipeline.linkPipeline.LinkFeatureAppender;
import org.neo4j.gds.ml.pipeline.linkPipeline.LinkFeatureStep;
import org.neo4j.gds.ml.pipeline.linkPipeline.LinkFeatureStepFactory;

import java.util.List;
import java.util.Map;

public class L2FeatureStep implements LinkFeatureStep {

    private final List<String> nodeProperties;

    public L2FeatureStep(List<String> nodeProperties) {
        this.nodeProperties = nodeProperties;
    }

    @Override
    public LinkFeatureAppender linkFeatureAppender(Graph graph) {
        LinkFeatureAppender[] appenderPerProperty = new L2LinkFeatureAppenderFactory().createAppenders(
            graph,
            nodeProperties
        );

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
        return LinkFeatureStepFactory.L2.name();
    }

    private static class L2LinkFeatureAppenderFactory extends AbstractLinkFeatureAppenderFactory {

        @Override
        protected LinkFeatureAppender doubleArrayAppender(NodePropertyValues props, int dimension) {
            return new L2DoubleArrayFeatureAppender(props, dimension);
        }

        @Override
        protected LinkFeatureAppender floatArrayAppender(NodePropertyValues props, int dimension) {
            return new L2FloatArrayFeatureAppender(props, dimension);
        }

        @Override
        protected LinkFeatureAppender longArrayAppender(NodePropertyValues props, int dimension) {
            return new L2LongArrayFeatureAppender(props, dimension);
        }

        @Override
        protected LinkFeatureAppender longAppender(NodePropertyValues props, int dimension) {
            return new L2LongFeatureAppender(props, dimension);
        }

        @Override
        protected LinkFeatureAppender doubleAppender(NodePropertyValues props, int dimension) {
            return new L2DoubleFeatureAppender(props, dimension);
        }

        private static class L2DoubleArrayFeatureAppender extends SinglePropertyFeatureAppender {

            L2DoubleArrayFeatureAppender(NodePropertyValues props, int dimension) {
                super(props, dimension);
            }

            @Override
            public void appendFeatures(long source, long target, double[] linkFeatures, int offset) {
                var sourceArrayPropValues = props.doubleArrayValue(source);
                var targetArrayPropValues = props.doubleArrayValue(target);
                assert sourceArrayPropValues.length == targetArrayPropValues.length;
                for (int i = 0; i < sourceArrayPropValues.length; i++) {
                    linkFeatures[offset++] = Math.pow(
                        sourceArrayPropValues[i] - targetArrayPropValues[i],
                        2
                    );
                }
            }
        }

        private static class L2FloatArrayFeatureAppender extends SinglePropertyFeatureAppender {

            L2FloatArrayFeatureAppender(NodePropertyValues props, int dimension) {
                super(props, dimension);
            }

            @Override
            public void appendFeatures(long source, long target, double[] linkFeatures, int offset) {
                var sourceArrayPropValues = props.floatArrayValue(source);
                var targetArrayPropValues = props.floatArrayValue(target);
                assert sourceArrayPropValues.length == targetArrayPropValues.length;
                for (int i = 0; i < sourceArrayPropValues.length; i++) {
                    linkFeatures[offset++] = Math.pow(
                        sourceArrayPropValues[i] - targetArrayPropValues[i],
                        2
                    );
                }
            }
        }

        private static class L2LongArrayFeatureAppender extends SinglePropertyFeatureAppender {

            L2LongArrayFeatureAppender(NodePropertyValues props, int dimension) {
                super(props, dimension);
            }

            @Override
            public void appendFeatures(long source, long target, double[] linkFeatures, int offset) {
                var sourceArrayPropValues = props.longArrayValue(source);
                var targetArrayPropValues = props.longArrayValue(target);
                assert sourceArrayPropValues.length == targetArrayPropValues.length;
                for (int i = 0; i < sourceArrayPropValues.length; i++) {
                    linkFeatures[offset++] = Math.pow(
                        sourceArrayPropValues[i] - targetArrayPropValues[i],
                        2
                    );
                }
            }
        }

        private static class L2DoubleFeatureAppender extends SinglePropertyFeatureAppender {

            L2DoubleFeatureAppender(NodePropertyValues props, int dimension) {
                super(props, dimension);
            }

            @Override
            public void appendFeatures(long source, long target, double[] linkFeatures, int offset) {
                linkFeatures[offset] = Math.pow(props.doubleValue(source) - props.doubleValue(target), 2);
            }
        }

        private static class L2LongFeatureAppender extends SinglePropertyFeatureAppender {

            L2LongFeatureAppender(NodePropertyValues props, int dimension) {
                super(props, dimension);
            }

            @Override
            public void appendFeatures(long source, long target, double[] linkFeatures, int offset) {
                linkFeatures[offset] = Math.pow(props.longValue(source) - props.longValue(target), 2);
            }
        }
    }
}

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
import org.neo4j.gds.api.properties.nodes.NodePropertyContainer;
import org.neo4j.gds.api.properties.nodes.NodePropertyValues;
import org.neo4j.gds.ml.pipeline.FeatureStepUtil;
import org.neo4j.gds.ml.pipeline.linkPipeline.LinkFeatureAppender;
import org.neo4j.gds.ml.pipeline.linkPipeline.LinkFeatureStep;
import org.neo4j.gds.ml.pipeline.linkPipeline.LinkFeatureStepFactory;

import java.util.List;
import java.util.Map;

import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

public class CosineFeatureStep implements LinkFeatureStep {

    private final List<String> nodePropertyNames;

    public CosineFeatureStep(List<String> nodeProperties) {
        this.nodePropertyNames = nodeProperties;
    }

    @Override
    public LinkFeatureAppender linkFeatureAppender(Graph graph) {
        PartialL2WithNormsComputer[] partialL2WithNormsComputers = nodePropertyNames
            .stream()
            .map(nodePropertyName -> createComputer(graph, nodePropertyName))
            .toArray(PartialL2WithNormsComputer[]::new);

        return new LinkFeatureAppender() {
            @Override
            public void appendFeatures(long source, long target, double[] linkFeatures, int offset) {
                var partialResults = new CosineComputationResult();

                for (PartialL2WithNormsComputer partialL2WithNormsComputer : partialL2WithNormsComputers) {
                    partialL2WithNormsComputer.compute(source, target, partialResults);
                }
                linkFeatures[offset] = partialResults.dotProduct;

                double l2Norm = Math.sqrt(partialResults.sourceSquareNorm * partialResults.targetSquareNorm);

                if (Double.isNaN(l2Norm)) {
                    FeatureStepUtil.throwNanError(name(), nodePropertyNames, source, target);
                } else if (l2Norm != 0.0) {
                    linkFeatures[offset] = partialResults.dotProduct / l2Norm;
                }
            }

            @Override
            public int dimension() {
                return 1;
            }
        };
    }

    @Override
    public List<String> inputNodeProperties() {
        return nodePropertyNames;
    }

    @Override
    public String name() {
        return LinkFeatureStepFactory.COSINE.name();
    }

    @Override
    public Map<String, Object> configuration() {
        return Map.of("nodeProperties", nodePropertyNames);
    }

    private PartialL2WithNormsComputer createComputer(NodePropertyContainer graph, String propertyName) {
        var values = graph.nodeProperties(propertyName);

        switch (values.valueType()) {
            case DOUBLE_ARRAY:
                return new DoubleArrayComputer(values);
            case FLOAT_ARRAY:
                return new FloatArrayComputer(values);
            case LONG_ARRAY:
                return new LongArrayComputer(values);
            case LONG:
                return new LongComputer(values);
            case DOUBLE:
                return new DoubleComputer(values);
            default:
                throw new IllegalStateException(formatWithLocale("Unsupported ValueType %s", values.valueType()));
        }
    }

    private static class CosineComputationResult {
        double sourceSquareNorm;
        double targetSquareNorm;
        double dotProduct;

        CosineComputationResult() {
            this.sourceSquareNorm = 0.0;
            this.targetSquareNorm = 0.0;
            this.dotProduct = 0.0;
        }
    }

    private abstract static class PartialL2WithNormsComputer {

        protected final NodePropertyValues values;

        PartialL2WithNormsComputer(NodePropertyValues values) {
            this.values = values;
        }

        abstract void compute(
            long source,
            long target,
            CosineComputationResult result
        );
    }

    private static class DoubleArrayComputer extends PartialL2WithNormsComputer {

        DoubleArrayComputer(NodePropertyValues values) {
            super(values);
        }

        @Override
        void compute(
            long source,
            long target,
            CosineComputationResult result
        ) {
            var sourceArrayPropValues = values.doubleArrayValue(source);
            var targetArrayPropValues = values.doubleArrayValue(target);
            assert sourceArrayPropValues.length == targetArrayPropValues.length;
            for (int i = 0; i < sourceArrayPropValues.length; i++) {
                result.dotProduct += sourceArrayPropValues[i] * targetArrayPropValues[i];
                result.sourceSquareNorm += sourceArrayPropValues[i] * sourceArrayPropValues[i];
                result.targetSquareNorm += targetArrayPropValues[i] * targetArrayPropValues[i];
            }
        }
    }

    private static class FloatArrayComputer extends PartialL2WithNormsComputer {

        FloatArrayComputer(NodePropertyValues values) {
            super(values);
        }

        @Override
        void compute(
            long source,
            long target,
            CosineComputationResult result
        ) {
            var sourceArrayPropValues = values.floatArrayValue(source);
            var targetArrayPropValues = values.floatArrayValue(target);
            assert sourceArrayPropValues.length == targetArrayPropValues.length;
            for (int i = 0; i < sourceArrayPropValues.length; i++) {
                result.dotProduct += sourceArrayPropValues[i] * targetArrayPropValues[i];
                result.sourceSquareNorm += sourceArrayPropValues[i] * sourceArrayPropValues[i];
                result.targetSquareNorm += targetArrayPropValues[i] * targetArrayPropValues[i];
            }
        }
    }

    private static class LongArrayComputer extends PartialL2WithNormsComputer {

        LongArrayComputer(NodePropertyValues values) {
            super(values);
        }

        @Override
        void compute(
            long source,
            long target,
            CosineComputationResult result
        ) {
            var sourceArrayPropValues = values.longArrayValue(source);
            var targetArrayPropValues = values.longArrayValue(target);
            assert sourceArrayPropValues.length == targetArrayPropValues.length;
            for (int i = 0; i < sourceArrayPropValues.length; i++) {
                result.dotProduct += sourceArrayPropValues[i] * targetArrayPropValues[i];
                result.sourceSquareNorm += sourceArrayPropValues[i] * sourceArrayPropValues[i];
                result.targetSquareNorm += targetArrayPropValues[i] * targetArrayPropValues[i];
            }
        }
    }

    private static class DoubleComputer extends PartialL2WithNormsComputer {

        DoubleComputer(NodePropertyValues values) {
            super(values);
        }

        @Override
        void compute(
            long source,
            long target,
            CosineComputationResult result
        ) {
            var sourceArrayPropValues = values.doubleValue(source);
            var targetArrayPropValues = values.doubleValue(target);
            result.dotProduct += sourceArrayPropValues * targetArrayPropValues;
            result.sourceSquareNorm += sourceArrayPropValues * sourceArrayPropValues;
            result.targetSquareNorm += targetArrayPropValues * targetArrayPropValues;
        }
    }

    private static class LongComputer extends PartialL2WithNormsComputer {

        LongComputer(NodePropertyValues values) {
            super(values);
        }

        @Override
        void compute(long source, long target, CosineComputationResult result) {
            var sourceArrayPropValues = values.longValue(source);
            var targetArrayPropValues = values.longValue(target);
            result.dotProduct += sourceArrayPropValues * targetArrayPropValues;
            result.sourceSquareNorm += sourceArrayPropValues * sourceArrayPropValues;
            result.targetSquareNorm += targetArrayPropValues * targetArrayPropValues;
        }
    }
}

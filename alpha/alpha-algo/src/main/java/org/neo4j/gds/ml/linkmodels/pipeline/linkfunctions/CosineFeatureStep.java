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
package org.neo4j.gds.ml.linkmodels.pipeline.linkfunctions;

import org.apache.commons.lang3.mutable.MutableLong;
import org.jetbrains.annotations.TestOnly;
import org.neo4j.gds.ml.linkmodels.pipeline.LinkFeatureStep;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.NodeProperties;
import org.neo4j.graphalgo.api.nodeproperties.ValueType;
import org.neo4j.graphalgo.core.utils.paged.HugeObjectArray;

import java.util.List;
import java.util.stream.Collectors;

import static org.neo4j.graphalgo.utils.StringFormatting.formatWithLocale;

public class CosineFeatureStep implements LinkFeatureStep {

    private final List<String> featureProperties;

    public CosineFeatureStep(List<String> featureProperties) {
        this.featureProperties = featureProperties;
    }

    @TestOnly
    public List<String> featureProperties() {
        return featureProperties;
    }

    @Override
    public void execute(Graph graph, HugeObjectArray<double[]> linkFeatures, int offset) {
        var seenRelationships = new MutableLong(0);
        var properties = featureProperties.stream().map(graph::nodeProperties).collect(Collectors.toList());

        graph.forEachNode(nodeId -> {
            graph.forEachRelationship(nodeId, ((sourceNodeId, targetNodeId) -> {
                var lf = (double[]) linkFeatures.get(seenRelationships.getValue());
                var sourceSquareNorm = 0.0;
                var targetSquareNorm = 0.0;

                for (NodeProperties props : properties) {
                    var propertyType = props.valueType();
                    if ((ValueType.DOUBLE_ARRAY == propertyType) || (ValueType.FLOAT_ARRAY == propertyType)) {
                        var sourceArrayPropValues = props.doubleArrayValue(sourceNodeId);
                        var targetArrayPropValues = props.doubleArrayValue(targetNodeId);
                        for (int i = 0; i < sourceArrayPropValues.length; i++) {
                            lf[offset] += sourceArrayPropValues[i] * targetArrayPropValues[i];
                            sourceSquareNorm += sourceArrayPropValues[i] * sourceArrayPropValues[i];
                            targetSquareNorm += targetArrayPropValues[i] * targetArrayPropValues[i];
                        }
                    } else if (ValueType.LONG_ARRAY == propertyType) {
                        var sourceArrayPropValues = props.longArrayValue(sourceNodeId);
                        var targetArrayPropValues = props.longArrayValue(targetNodeId);
                        for (int i = 0; i < sourceArrayPropValues.length; i++) {
                            lf[offset] += sourceArrayPropValues[i] * targetArrayPropValues[i];
                            sourceSquareNorm += sourceArrayPropValues[i] * sourceArrayPropValues[i];
                            targetSquareNorm += targetArrayPropValues[i] * targetArrayPropValues[i];
                        }
                    } else if ((ValueType.DOUBLE == propertyType) || (ValueType.LONG == propertyType)) {
                        lf[offset] += props.doubleValue(sourceNodeId) * props.doubleValue(targetNodeId);
                        sourceSquareNorm += props.doubleValue(sourceNodeId) * props.doubleValue(sourceNodeId);
                        targetSquareNorm += props.doubleValue(targetNodeId) * props.doubleValue(targetNodeId);
                    } else {
                        throw new IllegalStateException(formatWithLocale("Unknown ValueType %s", propertyType));
                    }
                }
                lf[offset] /= Math.sqrt(sourceSquareNorm * targetSquareNorm);
                seenRelationships.increment();

                return true;
            }));

            return true;
        });
    }
}

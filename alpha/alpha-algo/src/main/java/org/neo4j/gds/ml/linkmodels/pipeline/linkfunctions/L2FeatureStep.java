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

import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.commons.lang3.mutable.MutableLong;
import org.jetbrains.annotations.TestOnly;
import org.neo4j.gds.ml.linkmodels.pipeline.LinkFeatureStep;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.NodeProperties;
import org.neo4j.graphalgo.api.nodeproperties.ValueType;
import org.neo4j.graphalgo.core.utils.paged.HugeObjectArray;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.neo4j.graphalgo.utils.StringFormatting.formatWithLocale;

public class L2FeatureStep implements LinkFeatureStep {

    private final List<String> featureProperties;

    public L2FeatureStep(List<String> featureProperties) {
        this.featureProperties = featureProperties;
    }

    public L2FeatureStep(Map<String, Object> config) {
        this((List<String>) config.get(FEATURE_PROPERTIES));
    }

    public static void validateConfig(Map<String, Object> config) {
        LinkFeatureStepValidation.validateConfig("L2 link feature", config);
    }

    @TestOnly
    public List<String> featureProperties() {
        return featureProperties;
    }

    @Override
    public void addFeatures(Graph graph, HugeObjectArray<double[]> linkFeatures, int offset) {
        var seenRelationships = new MutableLong(0);
        var properties = featureProperties.stream().map(graph::nodeProperties).collect(Collectors.toList());

        graph.forEachNode(nodeId -> {
            graph.forEachRelationship(nodeId, ((sourceNodeId, targetNodeId) -> {
                var lf = (double[]) linkFeatures.get(seenRelationships.getValue());
                var currentOffset = new MutableInt(offset);

                for (NodeProperties props : properties) {
                    var propertyType = props.valueType();
                    switch (propertyType) {
                        case DOUBLE_ARRAY:
                        case FLOAT_ARRAY: {
                            var sourceArrayPropValues = props.doubleArrayValue(sourceNodeId);
                            var targetArrayPropValues = props.doubleArrayValue(targetNodeId);
                            for (int i = 0; i < sourceArrayPropValues.length; i++) {
                                lf[currentOffset.getAndIncrement()] = Math.pow(sourceArrayPropValues[i] - targetArrayPropValues[i], 2);
                            }
                            break;
                        }
                        case LONG_ARRAY: {
                            var sourceArrayPropValues = props.longArrayValue(sourceNodeId);
                            var targetArrayPropValues = props.longArrayValue(targetNodeId);
                            for (int i = 0; i < sourceArrayPropValues.length; i++) {
                                lf[currentOffset.getAndIncrement()] = Math.pow(sourceArrayPropValues[i] - targetArrayPropValues[i], 2);
                            }
                            break;
                        }
                        case LONG:
                        case DOUBLE:
                            lf[currentOffset.getAndIncrement()] = Math.pow(props.doubleValue(sourceNodeId) - props.doubleValue(targetNodeId), 2);
                            break;
                        case UNKNOWN:
                            throw new IllegalStateException(formatWithLocale("Unknown ValueType %s", propertyType));
                    }
                }
                seenRelationships.increment();

                return true;
            }));

            return true;
        });
    }
}

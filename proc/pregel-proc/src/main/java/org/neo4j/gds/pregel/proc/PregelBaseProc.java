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
package org.neo4j.gds.pregel.proc;

import org.neo4j.gds.AlgoBaseProc;
import org.neo4j.gds.Algorithm;
import org.neo4j.gds.api.NodeProperties;
import org.neo4j.gds.api.nodeproperties.DoubleArrayNodeProperties;
import org.neo4j.gds.api.nodeproperties.LongArrayNodeProperties;
import org.neo4j.gds.beta.pregel.PregelConfig;
import org.neo4j.gds.beta.pregel.PregelResult;
import org.neo4j.gds.beta.pregel.PregelSchema;
import org.neo4j.gds.core.utils.paged.HugeObjectArray;
import org.neo4j.gds.core.write.ImmutableNodeProperty;
import org.neo4j.gds.core.write.NodeProperty;

import java.util.List;
import java.util.stream.Collectors;

import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

final class PregelBaseProc {

    static <ALGO extends Algorithm<ALGO, PregelResult>, CONFIG extends PregelConfig>
    List<NodeProperty> nodeProperties(
        AlgoBaseProc.ComputationResult<ALGO, PregelResult, CONFIG> computationResult,
        String propertyPrefix
    ) {
        var compositeNodeValue = computationResult.result().nodeValues();
        var schema = compositeNodeValue.schema();
        // TODO change this to generic prefix setting

        return schema.elements()
            .stream()
            .filter(element -> element.visibility() == PregelSchema.Visibility.PUBLIC)
            .map(element -> {
                var propertyKey = element.propertyKey();

                NodeProperties nodeProperties;
                switch (element.propertyType()) {
                    case LONG:
                        nodeProperties = compositeNodeValue.longProperties(propertyKey).asNodeProperties();
                        break;
                    case DOUBLE:
                        nodeProperties = compositeNodeValue.doubleProperties(propertyKey).asNodeProperties();
                        break;
                    case LONG_ARRAY:
                        nodeProperties = new HugeObjectArrayLongArrayProperties(
                            compositeNodeValue.longArrayProperties(propertyKey)
                        );
                        break;
                    case DOUBLE_ARRAY:
                        nodeProperties = new HugeObjectArrayDoubleArrayProperties(
                            compositeNodeValue.doubleArrayProperties(propertyKey)
                        );
                        break;
                    default:
                        throw new IllegalArgumentException("Unsupported property type: " + element.propertyType());
                }

                return ImmutableNodeProperty.of(
                    formatWithLocale("%s%s", propertyPrefix, propertyKey),
                    nodeProperties
                );
            }).collect(Collectors.toList());
    }

    private PregelBaseProc() {}

    static class HugeObjectArrayLongArrayProperties implements LongArrayNodeProperties {
        private final HugeObjectArray<long[]> longArrays;

        HugeObjectArrayLongArrayProperties(HugeObjectArray<long[]> longArrays) {this.longArrays = longArrays;}

        @Override
        public long size() {
            return longArrays.size();
        }

        @Override
        public long[] longArrayValue(long nodeId) {
            return longArrays.get(nodeId);
        }
    }

    static class HugeObjectArrayDoubleArrayProperties implements DoubleArrayNodeProperties {
        private final HugeObjectArray<double[]> doubleArrays;

        HugeObjectArrayDoubleArrayProperties(HugeObjectArray<double[]> doubleArrays) {this.doubleArrays = doubleArrays;}

        @Override
        public long size() {
            return doubleArrays.size();
        }


        @Override
        public double[] doubleArrayValue(long nodeId) {
            return doubleArrays.get(nodeId);
        }
    }

}

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
package org.neo4j.gds;

import org.apache.commons.lang3.tuple.Pair;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.api.properties.nodes.NodeProperty;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.IntStream;

import static java.util.Locale.ENGLISH;
import static java.util.Locale.getDefault;
import static java.util.stream.Collectors.joining;
import static org.assertj.core.api.Assertions.assertThat;

public final class TestSupportGdlRecords {
    public record NodeLabelsAndProperties(
        long nodeId,
        List<String> labels,
        List<Pair<String, NodeProperty>> properties
    ) {
        public String toGdl() {
            Function<String, String> propertiesString = properties ->
                properties.isEmpty() ? "" : String.format(getDefault(), " {%s}", properties);
            Function<Pair<String, NodeProperty>, String> propertyToString = p ->
                String.format(getDefault(), "%s : %s", p.getLeft(), formattedValue(p.getRight()));

            return String.format(
                getDefault(),
                "(a%d%s%s)",
                nodeId,
                labels.stream().map(label -> String.format(getDefault(), ":%s", label)).collect(joining()),
                propertiesString.apply(properties.stream().map(propertyToString).collect(joining(", ")))
            );
        }

        private String formattedValue(NodeProperty nodeProperty) {
            return switch (nodeProperty.valueType()) {
                case LONG -> String.format(getDefault(), "%dL", nodeProperty.values().longValue(nodeId));
                case DOUBLE -> String.format(getDefault(), "%fd", nodeProperty.values().doubleValue(nodeId));
                case LONG_ARRAY -> String.format(
                    getDefault(),
                    "[%s]", Arrays.stream(nodeProperty.values().longArrayValue(nodeId))
                        .mapToObj(l -> String.format(getDefault(), "%dL", l))
                        .collect(joining(", "))
                );
                case DOUBLE_ARRAY -> String.format(
                    getDefault(),
                    "[%s]", Arrays.stream(nodeProperty.values().doubleArrayValue(nodeId))
                        .mapToObj(d -> String.format(getDefault(), "%fd", d))
                        .collect(joining(", "))
                );
                case FLOAT_ARRAY -> {
                    float[] floatArrayValue = nodeProperty.values().floatArrayValue(nodeId);
                    yield IntStream.range(0, floatArrayValue.length)
                        .mapToObj(i -> String.format(getDefault(), "%ff", floatArrayValue[i]))
                        .collect(joining(", "));
                }
                default -> throw new IllegalStateException("Unexpected value: " + nodeProperty.valueType());
            };
        }
    }

    public record RelationshipTypeProperties(
        List<Pair<Long, Long>> edges,
        List<String> propertyKeys,
        List<List<Double>> propertyValues
    ) {
        public static RelationshipTypeProperties of(GraphStore graphStore, RelationshipType relationshipType) {
            var edges = new ArrayList<Pair<Long, Long>>();

            var propertyKeys = graphStore.relationshipPropertyKeys(relationshipType).stream().toList();
            var propertyValues = new ArrayList<List<Double>>();

            var graph = graphStore.getGraph(relationshipType);
            graph.forEachNode(nodeId -> {
                graph.forEachRelationship(
                    nodeId, (s, t) -> {
                        edges.add(Pair.of(s, t));
                        return true;
                    }
                );
                return true;
            });

            propertyKeys.forEach(propertyKey -> {
                var values = new ArrayList<Double>();
                var aGraph = graphStore.getGraph(relationshipType, Optional.of(propertyKey));
                aGraph.forEachNode(nodeId -> {
                    aGraph.forEachRelationship(
                        nodeId, Float.NaN, (s, t, v) -> {
                            values.add(v);
                            return true;
                        }
                    );
                    return true;
                });
                propertyValues.add(values);
            });

            int dim = edges.size();
            propertyValues.forEach(p -> assertThat(p.size())
                .withFailMessage(() -> String.format(ENGLISH, "propertyValues don't have the same length as the edges %d", dim))
                .isEqualTo(dim));

            return new RelationshipTypeProperties(edges, propertyKeys, propertyValues);
        }

        public List<String> toGdl(String label) {
            return IntStream.range(0, edges.size()).mapToObj(i -> toGdl(label, i)).toList();
        }

        private String toGdl(String label, int i) {
            int propertyCount = propertyKeys.size();
            String properties = IntStream.range(0, propertyCount)
                .mapToObj(index -> String.format(getDefault(), "%s : %s", propertyKeys.get(index), propertyValues.get(index).get(i)))
                .collect(joining(", "));
            var labelAndProperties = String.format(
                getDefault(),
                "[:%s%s]",
                label,
                properties.isEmpty() ? "" : String.format(getDefault(), "{%s}", properties)
            );
            return String.format(ENGLISH, "(a%d)-%s->(a%d)", edges.get(i).getLeft(), labelAndProperties, edges.get(i).getRight());
        }
    }
}

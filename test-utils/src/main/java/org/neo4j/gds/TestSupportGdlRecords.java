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

import static java.util.stream.Collectors.joining;
import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.gds.RelationshipType.ALL_RELATIONSHIPS;
import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

public final class TestSupportGdlRecords {
    public record NodeLabelsAndProperties(
        long nodeId,
        List<String> labels,
        List<Pair<String, NodeProperty>> properties
    ) {
        public String toGdl() {
            Function<String, String> propertiesString = properties ->
                properties.isEmpty() ? "" : formatWithLocale(" {%s}", properties);
            Function<Pair<String, NodeProperty>, String> propertyToString = p ->
                formatWithLocale("%s : %s", p.getLeft(), formattedValue(p.getRight()));

            return formatWithLocale(
                "(a%d%s%s)",
                nodeId,
                labels.stream().map(label -> formatWithLocale(":%s", label)).collect(joining()),
                propertiesString.apply(properties.stream().map(propertyToString).collect(joining(", ")))
            );
        }

        private String formattedValue(NodeProperty nodeProperty) {
            return switch (nodeProperty.valueType()) {
                case LONG -> formatWithLocale("%dL", nodeProperty.values().longValue(nodeId));
                case DOUBLE -> formatWithLocale("%fd", nodeProperty.values().doubleValue(nodeId));
                case LONG_ARRAY -> formatWithLocale(
                    "[%s]", Arrays.stream(nodeProperty.values().longArrayValue(nodeId))
                        .mapToObj(l -> formatWithLocale("%dL", l))
                        .collect(joining(", "))
                );
                case DOUBLE_ARRAY -> formatWithLocale(
                    "[%s]", Arrays.stream(nodeProperty.values().doubleArrayValue(nodeId))
                        .mapToObj(d -> formatWithLocale("%fd", d))
                        .collect(joining(", "))
                );
                case FLOAT_ARRAY -> {
                    float[] floatArrayValue = nodeProperty.values().floatArrayValue(nodeId);
                    yield IntStream.range(0, floatArrayValue.length)
                        .mapToObj(i -> formatWithLocale("%ff", floatArrayValue[i]))
                        .collect(joining(", "));
                }
                default -> throw new IllegalStateException("Unexpected value: " + nodeProperty.valueType());
            };
        }
    }

    public record RelationshipTypeProperties(
        List<Pair<Long, Long>> relationships,
        List<String> propertyKeys,
        List<List<Double>> propertyValues
    ) {
        public static RelationshipTypeProperties of(GraphStore graphStore, RelationshipType relationshipType) {
            var relationships = new ArrayList<Pair<Long, Long>>();

            var propertyKeys = graphStore.relationshipPropertyKeys(relationshipType).stream().toList();
            var propertyValues = new ArrayList<List<Double>>();

            var graph = graphStore.getGraph(relationshipType);
            graph.forEachNode(nodeId -> {
                graph.forEachRelationship(
                    nodeId, (s, t) -> {
                        relationships.add(Pair.of(s, t));
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

            int dim = relationships.size();
            propertyValues.forEach(p -> assertThat(p.size())
                .withFailMessage(() -> formatWithLocale("propertyValues don't have the same length as the relationships %d", dim))
                .isEqualTo(dim));

            return new RelationshipTypeProperties(relationships, propertyKeys, propertyValues);
        }

        public List<String> toGdl(RelationshipType relationshipType) {
            return IntStream.range(0, relationships.size()).mapToObj(relationshipIndex -> toGdl(relationshipType, relationshipIndex)).toList();
        }

        private String toGdl(RelationshipType relationshipType, int i) {
            int propertyCount = propertyKeys.size();
            String properties = IntStream.range(0, propertyCount)
                .filter(index -> !Double.isNaN(propertyValues.get(index).get(i)))
                .mapToObj(index -> formatWithLocale("%s : %s", propertyKeys.get(index), propertyValues.get(index).get(i)))
                .collect(joining(", "));
            var label = relationshipType.equals(ALL_RELATIONSHIPS) ? "" : formatWithLocale(":%s", relationshipType.name());
            String propertiesString = properties.isEmpty() ? "" : formatWithLocale("{%s}", properties);
            var labelAndProperties = formatWithLocale("[%s%s]", label, propertiesString);
            return formatWithLocale("(a%d)-%s->(a%d)", relationships.get(i).getLeft(), labelAndProperties, relationships.get(i).getRight());
        }
    }
}

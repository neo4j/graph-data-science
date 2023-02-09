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
package org.neo4j.gds.ml.splitting;

import org.apache.commons.lang3.mutable.MutableInt;
import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.GraphCharacteristics;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.api.Properties;
import org.neo4j.gds.api.RelationshipProperty;
import org.neo4j.gds.api.schema.MutableGraphSchema;
import org.neo4j.gds.api.schema.MutableNodeSchema;
import org.neo4j.gds.api.schema.MutableRelationshipSchema;
import org.neo4j.gds.core.huge.HugeGraphBuilder;
import org.neo4j.gds.core.loading.SingleTypeRelationships;

import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

abstract class EdgeSplitterBaseTest {

    Graph createGraph(SingleTypeRelationships relationships, GraphStore graphStore) {
        var schema = MutableGraphSchema.of(
            (MutableNodeSchema) graphStore.schema().nodeSchema(),
            new MutableRelationshipSchema(Map.of(
                relationships.relationshipSchemaEntry().identifier(),
                relationships.relationshipSchemaEntry()
            )),
            Map.of()
        );

        var graphCharacteristicsBuilder = GraphCharacteristics
            .builder()
            .withDirection(relationships.relationshipSchemaEntry()
                .direction());
        relationships.inverseTopology().ifPresent(__ -> graphCharacteristicsBuilder.inverseIndexed());

        Set<String> relProps = schema.relationshipSchema().allProperties();
        assert relProps.size() <= 1 : "Cannot derive relationship property";

        Optional<String> maybePropName = relProps.stream().findFirst();
        Optional<Properties> property = maybePropName.flatMap(propName -> relationships
            .properties()
            .map(i -> i.get(propName))
            .map(RelationshipProperty::values));

        var inverseProperty = maybePropName.flatMap(propName -> relationships
            .inverseProperties()
            .map(i -> i.get(propName))
            .map(RelationshipProperty::values));

        return new HugeGraphBuilder().characteristics(graphCharacteristicsBuilder.build())
            .nodes(graphStore.nodes())
            .schema(schema)
            .topology(relationships.topology())
            .inverseTopology(relationships.inverseTopology())
            .relationshipProperties(property)
            .inverseRelationshipProperties(inverseProperty)
            .build();
    }

    void assertRelSamplingProperties(Graph resultGraph, Graph inputGraph) {
        MutableInt positiveCount = new MutableInt();
        inputGraph.forEachNode(source -> {
                resultGraph.forEachRelationship(source, Double.NaN, (src, trg, weight) -> {
                    boolean edgeIsPositive = weight == EdgeSplitter.POSITIVE;
                    if (edgeIsPositive) positiveCount.increment();
                    assertThat(edgeIsPositive)
                        .isEqualTo(inputGraph.exists(source, trg));
                    return true;
                });
        return true;
        });

        assertThat(resultGraph.relationshipCount()).isEqualTo(positiveCount.longValue());
    }

    void assertNodeLabelFilter(
        Graph actualGraph,
        Collection<NodeLabel> sourceLabels,
        Collection<NodeLabel> targetLabels
    ) {
        actualGraph.forEachNode(sourceNode -> {
            if (actualGraph.degree(sourceNode) > 0) {
                assertThat(actualGraph.nodeLabels(sourceNode).stream().filter(sourceLabels::contains)).isNotEmpty();
            }

            actualGraph.forEachRelationship(sourceNode, (src, trg) -> {
                assertThat(actualGraph.nodeLabels(trg).stream().filter(targetLabels::contains)).isNotEmpty();

                return true;
            });

            return true;
        });
    }

    void assertRelInGraph(Graph actualGraph, Graph inputGraph) {
        inputGraph.forEachNode(source -> {
            double fallbackValue = Double.NaN;
            actualGraph.forEachRelationship(source, fallbackValue, (src, trg, weight) -> {
                assertThat(inputGraph.exists(src, trg)).isTrue();
                assertThat(actualGraph.relationshipProperty(src, trg, fallbackValue)).isEqualTo(weight);

                return true;
            });

            return true;
        });
    }
}

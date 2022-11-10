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

import org.junit.jupiter.api.Test;
import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.Orientation;
import org.neo4j.gds.api.DefaultValue;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.Relationships;
import org.neo4j.gds.core.Aggregation;
import org.neo4j.gds.core.loading.construction.GraphFactory;
import org.neo4j.gds.core.loading.construction.RelationshipsBuilder;
import org.neo4j.gds.core.loading.construction.RelationshipsBuilderBuilder;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.ml.negativeSampling.UserInputNegativeSampler;

import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;
import static org.neo4j.gds.ml.negativeSampling.NegativeSampler.NEGATIVE;

@GdlExtension
class UserInputNegativeSamplerTest {

    @GdlGraph(orientation = Orientation.UNDIRECTED)
    static String gdl =
        "(a1:A), " +
        "(a2:A), " +
        "(a3:A), " +
        "(a4:A), " +
        "(a5:A), " +
        "(a1)-->(a2), " +
        "(a1)-->(a3), " +
        "(a2)-->(a3), " +
        "(a3)-->(a4), " +
        "(a3)-->(a5), " +
        "(a4)-->(a5)";

    @Inject
    Graph graph;


    @Test
    void generateNegativeSamples() {
        var sampler = new UserInputNegativeSampler(
            graph,
            (double) 1/3,
            Optional.of(42L),
            List.of(NodeLabel.of("A")),
            List.of(NodeLabel.of("A"))
        );

        RelationshipsBuilder testBuilder = new RelationshipsBuilderBuilder().nodes(graph).addPropertyConfig(
            GraphFactory.PropertyConfig.of(Aggregation.SINGLE, DefaultValue.forDouble())
        ).build();
        RelationshipsBuilder trainBuilder = new RelationshipsBuilderBuilder().nodes(graph).addPropertyConfig(
            GraphFactory.PropertyConfig.of(Aggregation.SINGLE, DefaultValue.forDouble())
        ).build();

        sampler.produceNegativeSamples(testBuilder, trainBuilder);

        Relationships testSet = testBuilder.build().relationships();
        Relationships trainSet = trainBuilder.build().relationships();

        assertThat(testSet.topology().elementCount()).isEqualTo(2);
        assertThat(trainSet.topology().elementCount()).isEqualTo(4);

        assertThat(testSet.properties()).isNotEmpty();
        graph.forEachNode(nodeId -> {
            try (var propertyCursor = testSet.properties().get().propertiesList().propertyCursor(nodeId)) {
                while (propertyCursor.hasNextLong()) {
                    assertThat(Double.longBitsToDouble(propertyCursor.nextLong())).isEqualTo(NEGATIVE);
                }
            }
            return true;
        });

        graph.forEachNode(nodeId -> {
            try (var propertyCursor = trainSet.properties().get().propertiesList().propertyCursor(nodeId)) {
                while (propertyCursor.hasNextLong()) {
                    assertThat(Double.longBitsToDouble(propertyCursor.nextLong())).isEqualTo(NEGATIVE);
                }
            }
            return true;
        });
    }

    @Test
    void shouldValidateNegativeExamplesRespectNodeLabels() {
        assertThatThrownBy(() -> new UserInputNegativeSampler(
            graph,
            (double) 1 / 3,
            Optional.of(42L),
            List.of(NodeLabel.of("B")),
            List.of(NodeLabel.of("A"))
        ))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("There is a relationship of negativeRelationshipType between nodes 0 and 1. " +
                                  "The nodes have types [NodeLabel{name='A'}] and [NodeLabel{name='A'}]. " +
                                  "However, they need to be between [NodeLabel{name='B'}] and [NodeLabel{name='A'}]."
            );
    }

}

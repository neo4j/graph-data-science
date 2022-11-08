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

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.gds.ml.splitting.NegativeSampler.NEGATIVE;

@GdlExtension
class UserInputNegativeSamplerTest {

    @GdlGraph(orientation = Orientation.NATURAL, graphNamePrefix = "negative")
    static String gdlNegative = "(n1 :A)-[:T {foo: 5} ]->(n2 :A)-[:T {foo: 5} ]->(n3 :A)-[:T {foo: 5} ]->(n4 :A)-[:T {foo: 5} ]->(n5 :B)-[:T {foo: 5} ]->(n6 :A), (n1)-[:NEGATIVE]->(n3), (n3)-[:NEGATIVE]->(n5), (n5)-[:NEGATIVE]->(n7 :A)";

    @GdlGraph
    static String gdl =
        "(a1:A), " +
        "(a2:A), " +
        "(a3:A), " +
        "(a4:A), " +
        "(a5:A), " +
        "(a6:A), " +
        "(a7:A), " +
        "(a1)-->(a2), " +
        "(a1)-->(a3), " +
        "(a2)-->(a3), " +
        "(a3)-->(a4), " +
        "(a3)-->(a5), " +
        "(a4)-->(a5), " +
        "(a5)-->(a6), " +
        "(a5)-->(a7)";

    @Inject
    Graph graph;


    @Test
    void generateNegativeSamples() {
        int testSampleCount = 3;
        var sampler = new NegativeSampler.UserInputNegativeSampler(
            graph,
            testSampleCount
        );

        RelationshipsBuilder testBuilder = new RelationshipsBuilderBuilder().nodes(graph).addPropertyConfig(
            GraphFactory.PropertyConfig.of(Aggregation.SINGLE, DefaultValue.forDouble())
        ).build();
        RelationshipsBuilder trainBuilder = new RelationshipsBuilderBuilder().nodes(graph).addPropertyConfig(
            GraphFactory.PropertyConfig.of(Aggregation.SINGLE, DefaultValue.forDouble())
        ).build();

        sampler.produceNegativeSamples(testBuilder, trainBuilder);

        Relationships testSet = testBuilder.build();
        Relationships trainSet = trainBuilder.build();

        assertThat(testSet.topology().elementCount()).isEqualTo(testSampleCount);
        assertThat(trainSet.topology().elementCount()).isEqualTo(5);

        assertThat(testSet.properties()).isNotEmpty();
        graph.forEachNode(nodeId -> {
            try (var propertyCursor = testSet.properties().get().propertiesList().propertyCursor(nodeId)) {
                while (propertyCursor.hasNextLong()) {
                    assertThat(Double.longBitsToDouble(propertyCursor.nextLong())).isEqualTo(NEGATIVE);
                }
            }
            return true;
        });
    }

}

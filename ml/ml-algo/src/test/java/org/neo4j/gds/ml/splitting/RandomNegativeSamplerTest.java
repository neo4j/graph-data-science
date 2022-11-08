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

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.gds.ml.splitting.NegativeSampler.NEGATIVE;

@GdlExtension
class RandomNegativeSamplerTest {

    @GdlGraph
    static String gdl =
        "(a1:A), " +
        "(a2:A), " +
        "(a3:A), " +
        "(a4:A), " +
        "(a5:A), " +
        "(a6:A), " +
        "(a1)-->(a2), " +
        "(a2)-->(a3), " +
        "(a3)-->(a4), " +
        "(a4)-->(a5), " +
        "(a5)-->(a6)";

    @Inject
    Graph graph;

    @Test
    void generateNegativeSamples() {
        int testSampleCount = 3;
        int trainSampleCount = 5;
        var sampler = new NegativeSampler.RandomNegativeSampler(
            graph,
            testSampleCount,
            trainSampleCount,
            graph,
            graph,
            //Seed 42L is special - it gives a duplicate negative sample for train, and trainSet count becomes 4 != 5.
            Optional.of(41L)
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
        assertThat(trainSet.topology().elementCount()).isEqualTo(trainSampleCount);

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

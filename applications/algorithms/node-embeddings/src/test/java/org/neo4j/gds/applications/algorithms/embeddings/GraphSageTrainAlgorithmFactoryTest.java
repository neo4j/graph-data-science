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
package org.neo4j.gds.applications.algorithms.embeddings;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.api.User;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.embeddings.graphsage.algo.GraphSageTrainConfig;
import org.neo4j.gds.embeddings.graphsage.algo.MultiLabelGraphSageTrain;
import org.neo4j.gds.embeddings.graphsage.algo.SingleLabelGraphSageTrain;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class GraphSageTrainAlgorithmFactoryTest {
    @Test
    void shouldConstructMultiLabelAlgorithm() {
        var factory = new GraphSageTrainAlgorithmFactory();

        var rawInput = CypherMapWrapper.create(
            Map.of(
                "modelName", "graphSageModel",
                "featureProperties", List.of("a"),
                "projectedFeatureDimension", 42
            )
        );
        var configuration = GraphSageTrainConfig.of(
            User.DEFAULT.getUsername(),
            rawInput
        );
        var algorithm = factory.create(null, configuration, null, null);

        assertThat(algorithm).isInstanceOf(MultiLabelGraphSageTrain.class);
    }

    @Test
    void shouldConstructSingleLabelAlgorithm() {
        var factory = new GraphSageTrainAlgorithmFactory();

        var configuration = GraphSageTrainConfig.of(
            User.DEFAULT.getUsername(),
            CypherMapWrapper.create(Map.of(
                "modelName", "graphSageModel",
                "featureProperties", List.of("a")
            ))
        );
        var algorithm = factory.create(null, configuration, null, null);

        assertThat(algorithm).isInstanceOf(SingleLabelGraphSageTrain.class);
    }
}

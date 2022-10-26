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
package org.neo4j.gds.embeddings.hashgnn;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.core.utils.TerminationFlag;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class HashTaskTest {
    @Test
    void shouldHash() {
        int ITERATIONS = 10;
        int EMBEDDING_DENSITY = 10;
        int NEIGHBOR_HASH_REPEATS = 3;
        int NUMBER_OF_RELATIONSHIPS = 3;
        int EMBEDDING_DIMENSION = 10;

        var config = HashGNNConfigImpl
            .builder()
            .featureProperties(List.of("f1", "f2"))
            .iterations(ITERATIONS)
            .embeddingDensity(EMBEDDING_DENSITY)
            .build();

        var hashes = HashTask.compute(
            EMBEDDING_DIMENSION,
            NEIGHBOR_HASH_REPEATS,
            NUMBER_OF_RELATIONSHIPS,
            config,
            42,
            TerminationFlag.RUNNING_TRUE,
            ProgressTracker.NULL_TRACKER
        );

        assertThat(hashes.size()).isEqualTo(ITERATIONS * EMBEDDING_DENSITY);
        assertThat(hashes.get(0).neighborsAggregationHashes().length).isEqualTo(EMBEDDING_DIMENSION);
        assertThat(hashes.get(0).preAggregationHashes().size()).isEqualTo(NUMBER_OF_RELATIONSHIPS);
        assertThat(hashes.get(0).selfAggregationHashes().length).isEqualTo(EMBEDDING_DIMENSION);
    }
}

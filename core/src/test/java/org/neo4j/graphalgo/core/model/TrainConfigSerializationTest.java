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
package org.neo4j.graphalgo.core.model;

import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.model.catalog.TestTrainConfig;
import org.neo4j.graphalgo.utils.serialization.ObjectSerializer;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;

class TrainConfigSerializationTest {

    @Test
    void canSerializeAndDeserializeTrainConfig() throws IOException, ClassNotFoundException {
        var trainConfig = TestTrainConfig.of();

        var serializedTrainConfig = ObjectSerializer.toByteArray(trainConfig);
        assertThat(serializedTrainConfig)
            .isNotNull()
            .isNotEmpty();

        var deserializedTrainConfig = ObjectSerializer.fromByteArray(serializedTrainConfig, TestTrainConfig.class);

        assertThat(deserializedTrainConfig)
            .isNotNull()
            .usingRecursiveComparison()
            .withStrictTypeChecking()
            .isEqualTo(trainConfig);
    }
}

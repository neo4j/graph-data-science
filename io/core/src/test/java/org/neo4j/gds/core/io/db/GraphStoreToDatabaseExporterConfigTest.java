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
package org.neo4j.gds.core.io.db;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.compat.Neo4jProxy;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

class GraphStoreToDatabaseExporterConfigTest {

    @Test
    void testToBatchImporterConfig() {
        var config = ImmutableGraphStoreToDatabaseExporterConfig.builder()
            .dbName("test")
            .batchSize(1337)
            .pageCacheMemory(Optional.of(100_000L))
            .highIO(true)
            .writeConcurrency(42)
            .build();
        var pbiConfig = config.toBatchImporterConfig();

        assertThat(pbiConfig.batchSize()).isEqualTo(1337);
        assertThat(pbiConfig.highIO()).isTrue();
        assertThat(Neo4jProxy.writeConcurrency(pbiConfig)).isEqualTo(42);
        assertThat(pbiConfig.indexConfig().createLabelIndex()).isTrue();
        assertThat(pbiConfig.indexConfig().createRelationshipIndex()).isTrue();
    }

}

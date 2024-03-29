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
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.compat.Neo4jProxy;

import static org.assertj.core.api.Assertions.assertThat;

class GraphStoreToDatabaseExporterParametersTest {

    @Test
    void testToBatchImporterConfig() {
        var parameters = GraphStoreToDatabaseExporterParameters.create(
                "test",
                42,
                1337,
                false,
                RelationshipType.ALL_RELATIONSHIPS.name
            )
            .withPageCacheMemory(100_000L)
            .withHighIO(true);
        var pbiConfig = parameters.toBatchImporterConfig();

        assertThat(pbiConfig.batchSize()).isEqualTo(1337);
        assertThat(pbiConfig.highIO()).isTrue();
        assertThat(Neo4jProxy.writeConcurrency(pbiConfig)).isEqualTo(42);
        assertThat(pbiConfig.indexConfig().createLabelIndex()).isTrue();
        assertThat(pbiConfig.indexConfig().createRelationshipIndex()).isTrue();
    }

}

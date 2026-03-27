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
package org.neo4j.gds.core.io.json;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.neo4j.gds.core.io.json.Utils.deserialize;
import static org.neo4j.gds.core.io.json.Utils.formatWithoutWhitespace;
import static org.neo4j.gds.core.io.json.Utils.serialize;

class GraphStoreMetadataSerializerTest {

    @Test
    void serializeGraphStoreMetadata() throws JsonProcessingException {
        var databaseName = "neo";
        var databaseLocation = DatabaseInfo.DatabaseLocation.LOCAL;
        var databaseInfo = new DatabaseInfo(databaseName, databaseLocation, Optional.empty());
        var graphStoreMetadata = new GraphStoreMetadata(databaseInfo);

        var result = serialize(graphStoreMetadata);

        var expected = formatWithoutWhitespace(
            """
                {
                    "databaseInfo": {
                         "databaseName":"%s",
                         "databaseLocation":"%s",
                         "remoteDatabaseId":null
                    }
                }
                """, databaseName, databaseLocation
        );
        assertThat(result).isEqualTo(expected);
    }

    @Test
    void roundTripGraphStoreMetadata() throws JsonProcessingException {
        var databaseName = "neo";
        var databaseLocation = DatabaseInfo.DatabaseLocation.LOCAL;
        var databaseInfo = new DatabaseInfo(databaseName, databaseLocation, Optional.empty());
        var graphStoreMetadata = new GraphStoreMetadata(databaseInfo);

        var result = deserialize(serialize(graphStoreMetadata), GraphStoreMetadata.class);

        assertThat(result).isEqualTo(graphStoreMetadata);
    }
}
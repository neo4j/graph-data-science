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
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

class DatabaseInfoSerializerTest {

    @Test
    void serializeDatabaseInfo() throws JsonProcessingException {
        var databaseName = "neo";
        var databaseLocation = DatabaseInfo.DatabaseLocation.LOCAL;
        var databaseInfo = new DatabaseInfo(databaseName, databaseLocation, Optional.empty());

        var result = serialize(databaseInfo);

        var expected = formatWithoutWhitespace(
            """
                {
                     "databaseName":"%s",
                     "databaseLocation":"%s",
                     "remoteDatabaseId":null
                }
                """, databaseName, databaseLocation
        );
        assertThat(result).isEqualTo(expected);
    }

    @Test
    void serializeDatabaseInfoWithRemote() throws JsonProcessingException {
        var databaseName = "neo";
        var databaseLocation = DatabaseInfo.DatabaseLocation.REMOTE;
        var remoteDatabaseId = "foo";
        var databaseInfo = new DatabaseInfo(databaseName, databaseLocation, Optional.of(remoteDatabaseId));

        var result = serialize(databaseInfo);

        var expected = formatWithoutWhitespace(
            """
                {
                     "databaseName":"%s",
                     "databaseLocation":"%s",
                     "remoteDatabaseId":"%s"
                }
                """, databaseName, databaseLocation, remoteDatabaseId
        );
        assertThat(result).isEqualTo(expected);
    }

    @Test
    void roundTripDatabaseInfo() throws JsonProcessingException {
        var databaseName = "neo";
        var databaseLocation = DatabaseInfo.DatabaseLocation.LOCAL;
        var databaseInfo = new DatabaseInfo(databaseName, databaseLocation, Optional.empty());

        var result = deserialize(serialize(databaseInfo));

        assertThat(result).isEqualTo(databaseInfo);
    }

    private static DatabaseInfo deserialize(String jsonString) throws JsonProcessingException {
        var mapper = new ObjectMapper()
            .registerModule(new Jdk8Module());
        return mapper.readValue(jsonString, DatabaseInfo.class);
    }

    private static String serialize(DatabaseInfo databaseInfo) throws JsonProcessingException {
        var mapper = new ObjectMapper()
            .registerModule(new Jdk8Module());
        return mapper.writeValueAsString(databaseInfo);
    }

    private static String formatWithoutWhitespace(String base, Object... args) {
        return String
            .format(base, args)
            .replace(" ", "")
            .replace("\n", "");
    }
}

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

import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.api.DatabaseId;
import org.neo4j.gds.api.DatabaseInfo;
import org.neo4j.gds.api.GraphName;
import org.neo4j.gds.core.loading.Capabilities;
import org.neo4j.gds.core.loading.ImmutableStaticCapabilities;

import static org.assertj.core.api.Assertions.assertThat;

class RoundtripTest {

    @Test
    void nodeLabel() {
        roundtripTest(NodeLabel.of("Foobar"), new NodeLabelSerializer(), new NodeLabelDeserializer(), NodeLabel.class);
    }

    @Test
    void relationshipType() {
        roundtripTest(
            RelationshipType.of("Foobar"),
            new RelationshipTypeSerializer(),
            new RelationshipTypeDeserializer(),
            RelationshipType.class
        );
    }

    @Test
    void graphName() {
        roundtripTest(
            GraphName.parse("Foobar"),
            new GraphNameSerializer(),
            new GraphNameDeserializer(),
            GraphName.class
        );
    }

    @Test
    void databaseId() {
        roundtripTest(
            DatabaseId.of("Foobar"),
            new DatabaseIdSerializer(),
            new DatabaseIdDeserializer(),
            DatabaseId.class
        );
    }

    @Test
    void capabilities() {
        roundtripTest(
            ImmutableStaticCapabilities.of(Capabilities.WriteMode.REMOTE),
            new CapabilitiesSerializer(),
            new CapabilitiesDeserializer(),
            Capabilities.class
        );
    }

    @Test
    void databaseInfo() {
        var objectMapper = new ObjectMapper();
        objectMapper.registerModule(new SimpleModule()
            .addSerializer(DatabaseInfo.class, new DatabaseInfoSerializer())
            .addDeserializer(DatabaseInfo.class, new DatabaseInfoDeserializer())
            .addSerializer(DatabaseId.class, new DatabaseIdSerializer())
            .addDeserializer(DatabaseId.class, new DatabaseIdDeserializer()));

        roundtripTest(
            DatabaseInfo.of(DatabaseId.of("foobar"), DatabaseInfo.DatabaseLocation.LOCAL),
            objectMapper,
            DatabaseInfo.class
        );
    }

    private static <T> void roundtripTest(
        T instance,
        JsonSerializer<T> serializer,
        JsonDeserializer<T> deserializer,
        Class<T> clazz
    ) {
        var objectMapper = new ObjectMapper();
        objectMapper.registerModule(new SimpleModule()
            .addSerializer(clazz, serializer)
            .addDeserializer(clazz, deserializer)
        );

        roundtripTest(instance, objectMapper, clazz);
    }

    private static <T> void roundtripTest(
        T instance,
        ObjectMapper objectMapper,
        Class<T> clazz
    ) {
        var json = objectMapper.convertValue(instance, JsonNode.class);
        var actual = objectMapper.convertValue(json, clazz);

        assertThat(actual).isEqualTo(instance);
    }
}
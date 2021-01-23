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

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.api.schema.GraphSchema;
import org.neo4j.graphalgo.api.schema.SchemaDeserializer;
import org.neo4j.graphalgo.api.schema.SchemaSerializer;
import org.neo4j.graphalgo.core.model.proto.GraphSchemaProto;
import org.neo4j.graphalgo.core.model.proto.ModelProto;
import org.neo4j.graphalgo.embeddings.graphsage.GraphSageTestGraph;
import org.neo4j.graphalgo.gdl.GdlFactory;
import org.neo4j.graphalgo.model.catalog.TestTrainConfig;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

class ModelSerializationTest {

    private static final GraphSchema GRAPH_SCHEMA = GdlFactory
        .of(GraphSageTestGraph.GDL)
        .build()
        .graphStore()
        .schema();

    @Disabled
    @Test
    void shouldSerializeGraphSchema() throws IOException {
        var serializableGraphSchema = SchemaSerializer.serializableGraphSchema(GRAPH_SCHEMA);

        var output = new ByteArrayOutputStream();
        serializableGraphSchema.writeTo(output);

        var parsedGraphSchema = GraphSchemaProto.GraphSchema.parseFrom(output.toByteArray());
        assertThat(parsedGraphSchema).isNotNull();

        var deserializedGraphSchema = SchemaDeserializer.graphSchema(parsedGraphSchema);
        assertThat(deserializedGraphSchema)
            .isNotNull()
            .isEqualTo(GRAPH_SCHEMA);
    }

    @Test
    void shouldSerializeModel() throws IOException, ClassNotFoundException {
        var model = Model.of(
            "user1",
            "testModel",
            "testAlgo",
            GRAPH_SCHEMA,
            "testTrainData",
            TestTrainConfig.of()
        );

        ModelProto.Model protoModel = ModelSerializer.toSerializable(model);

        ByteArrayOutputStream output = new ByteArrayOutputStream();
        protoModel.writeTo(output);

        ModelProto.Model protoModelDeserialized = ModelProto.Model.parseFrom(output.toByteArray());

        assertEquals(model.algoType(), protoModelDeserialized.getAlgoType());
        assertEquals(model.username(), protoModelDeserialized.getUsername());
        assertEquals(model.name(), protoModelDeserialized.getName());
        assertEquals(model.creationTime(), ZonedDateTimeSerializer.fromSerializable(protoModelDeserialized.getCreationTime()));
    }

}

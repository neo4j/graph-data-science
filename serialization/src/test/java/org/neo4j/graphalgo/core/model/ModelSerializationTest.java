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
import org.neo4j.gds.embeddings.graphsage.ActivationFunction;
import org.neo4j.gds.embeddings.graphsage.Aggregator;
import org.neo4j.gds.embeddings.graphsage.algo.GraphSage;
import org.neo4j.gds.embeddings.graphsage.algo.ImmutableGraphSageTrainConfig;
import org.neo4j.graphalgo.api.DefaultValue;
import org.neo4j.graphalgo.api.schema.GraphSchema;
import org.neo4j.graphalgo.api.schema.SchemaDeserializer;
import org.neo4j.graphalgo.api.schema.SchemaSerializer;
import org.neo4j.graphalgo.core.model.proto.GraphSchemaProto;
import org.neo4j.graphalgo.embeddings.graphsage.GraphSageTestGraph;
import org.neo4j.graphalgo.gdl.GdlFactory;
import org.neo4j.graphalgo.model.catalog.TestTrainConfig;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ModelSerializationTest {

    private static final GraphSchema GRAPH_SCHEMA = GdlFactory
        .of(GraphSageTestGraph.GDL)
        .build()
        .graphStore()
        .schema();

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
    void shouldSerialize() throws IOException {
        var model = Model.of(
            "user",
            "testGS",
            GraphSage.MODEL_TYPE,
            GRAPH_SCHEMA,
            "blah, blah",
            ImmutableGraphSageTrainConfig.builder()
                .modelName("MODEL_NAME")
                .aggregator(Aggregator.AggregatorType.MEAN)
                .activationFunction(ActivationFunction.SIGMOID)
                .featureProperties(List.of("age", "birth_year", "death_year", "embedding"))
                .build()
        );

        var protoModelMetaData = ModelMetaDataSerializer.toSerializable(model);
        assertThat(protoModelMetaData).isNotNull();

        var deserializedModel =
            ModelMetaDataSerializer.fromSerializable(protoModelMetaData)
                .data("blah, blah")
                .customInfo(Model.Mappable.EMPTY)
                .build();

        assertThat(deserializedModel)
            .isNotNull()
            .usingRecursiveComparison()
            .ignoringFieldsOfTypes(DefaultValue.class)
            .ignoringFields("stored")
            .isEqualTo(model);

        assertThat(deserializedModel.stored()).isTrue();
    }

    @Test
    void shouldThrowUnsupportedModelType() throws IOException, ClassNotFoundException {
        var model = Model.of(
            "user1",
            "testModel",
            "notSupportedAlgoType",
            GRAPH_SCHEMA,
            "testTrainData",
            TestTrainConfig.of()
        );

        assertThatThrownBy(
            () -> ModelMetaDataSerializer.toSerializable(model),
            "Unsupported model type: %s",
            "notSupportedAlgoType"
        );
    }

}

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
package org.neo4j.gds.core.write.resultstore;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.api.EphemeralResultStore;
import org.neo4j.gds.api.ExportedRelationship;
import org.neo4j.gds.api.ResultStoreEntry;
import org.neo4j.gds.api.nodeproperties.ValueType;
import org.neo4j.gds.core.utils.progress.JobId;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

import java.util.List;
import java.util.function.LongUnaryOperator;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

class ResultStoreRelationshipStreamExporterTest {

    @Test
    void shouldWriteRelationshipStreamWithPropertiesToResultStore() {
        var jobId = new JobId("test");
        var resultStore = new EphemeralResultStore();
        var relationshipStream = Stream.of(
            new ExportedRelationship(0, 1, new Value[]{ Values.doubleValue(42.0), Values.doubleArray(new double[]{ 43.0, 44.0 })}),
            new ExportedRelationship(1, 2, new Value[]{ Values.doubleValue(45.0), Values.doubleArray(new double[]{ 46.0, 47.0 })})
        );
        LongUnaryOperator mappingOperator = l -> l + 42;
        new ResultStoreRelationshipStreamExporter(jobId, resultStore, relationshipStream, mappingOperator)
            .write(
                "REL",
                List.of("doubleProp", "doubleArrayProp"),
                List.of(ValueType.DOUBLE, ValueType.DOUBLE_ARRAY)
            );

        var entry = resultStore.get(jobId);
        assertThat(entry).isInstanceOf(ResultStoreEntry.RelationshipStream.class);

        var jobIdRelationshipStreamEntry = (ResultStoreEntry.RelationshipStream) entry;

        assertThat(jobIdRelationshipStreamEntry.relationshipType()).isEqualTo("REL");
        assertThat(jobIdRelationshipStreamEntry.propertyKeys()).containsExactly("doubleProp", "doubleArrayProp");
        assertThat(jobIdRelationshipStreamEntry.propertyTypes()).containsExactly(ValueType.DOUBLE, ValueType.DOUBLE_ARRAY);
        assertThat(jobIdRelationshipStreamEntry.relationshipStream()).isEqualTo(relationshipStream);
    }

    @Test
    void shouldWriteRelationshipStreamWithoutPropertiesToResultStore() {
        var jobId = new JobId("test");
        var resultStore = new EphemeralResultStore();
        var relationshipStream = Stream.of(
            new ExportedRelationship(0, 1, new Value[0]),
            new ExportedRelationship(1, 2, new Value[0])
        );
        LongUnaryOperator mappingOperator = l -> l + 42;
        new ResultStoreRelationshipStreamExporter(jobId, resultStore, relationshipStream, mappingOperator).write(
            "REL",
            List.of(),
            List.of()
        );

        var entry = resultStore.get(jobId);
        assertThat(entry).isInstanceOf(ResultStoreEntry.RelationshipStream.class);
        var jobIdRelationshipStreamEntry = (ResultStoreEntry.RelationshipStream) entry;
        assertThat(jobIdRelationshipStreamEntry.relationshipType()).isEqualTo("REL");
        assertThat(jobIdRelationshipStreamEntry.propertyKeys()).isEmpty();
        assertThat(jobIdRelationshipStreamEntry.propertyTypes()).isEmpty();
        assertThat(jobIdRelationshipStreamEntry.relationshipStream()).isEqualTo(relationshipStream);
    }
}

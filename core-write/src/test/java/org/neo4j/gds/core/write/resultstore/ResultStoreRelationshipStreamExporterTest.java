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
import org.neo4j.gds.api.ImmutableExportedRelationship;
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
            ImmutableExportedRelationship.of(0, 1, new Value[]{ Values.doubleValue(42.0), Values.doubleArray(new double[]{ 43.0, 44.0 })}),
            ImmutableExportedRelationship.of(1, 2, new Value[]{ Values.doubleValue(45.0), Values.doubleArray(new double[]{ 46.0, 47.0 })})
        );
        LongUnaryOperator mappingOperator = l -> l + 42;
        new ResultStoreRelationshipStreamExporter(jobId, resultStore, relationshipStream, mappingOperator)
            .write(
                "REL",
                List.of("doubleProp", "doubleArrayProp"),
                List.of(ValueType.DOUBLE, ValueType.DOUBLE_ARRAY)
            );
        var relationshipStreamEntry = resultStore.getRelationshipStream("REL", List.of("doubleProp", "doubleArrayProp"));
        assertThat(relationshipStreamEntry).isNotNull();
        assertRelationshipEntryWithProperties(
            relationshipStreamEntry.propertyTypes(),
            relationshipStreamEntry.relationshipStream(),
            relationshipStreamEntry.toOriginalId(),
            mappingOperator
        );

        var entry = resultStore.get(jobId);
        assertThat(entry).isInstanceOf(ResultStoreEntry.RelationshipStream.class);

        var jobIdRelationshipStreamEntry = (ResultStoreEntry.RelationshipStream) entry;
        assertThat(jobIdRelationshipStreamEntry.relationshipType()).isEqualTo("REL");
        assertThat(jobIdRelationshipStreamEntry.propertyKeys()).containsExactly("doubleProp", "doubleArrayProp");
        assertThat(jobIdRelationshipStreamEntry.propertyTypes()).isEqualTo(relationshipStreamEntry.propertyTypes());
        assertThat(jobIdRelationshipStreamEntry.relationshipStream()).isEqualTo(relationshipStreamEntry.relationshipStream());
    }

    private static void assertRelationshipEntryWithProperties(
        List<ValueType> propertyTypes,
        Stream<ExportedRelationship> relationshipStream,
        LongUnaryOperator actualOperator,
        LongUnaryOperator expectedOperator
    ) {
        assertThat(propertyTypes).isEqualTo(List.of(ValueType.DOUBLE, ValueType.DOUBLE_ARRAY));
        assertThat(actualOperator).isEqualTo(expectedOperator);

        var relationshipIterator = relationshipStream.iterator();

        assertThat(relationshipIterator).hasNext();
        var firstRelationship = relationshipIterator.next();
        assertThat(firstRelationship.sourceNode()).isEqualTo(0L);
        assertThat(firstRelationship.targetNode()).isEqualTo(1L);
        assertThat(firstRelationship.values()).containsExactly(Values.doubleValue(42.0), Values.doubleArray(new double[]{ 43.0, 44.0 }));

        assertThat(relationshipIterator).hasNext();
        var secondRelationship = relationshipIterator.next();
        assertThat(secondRelationship.sourceNode()).isEqualTo(1L);
        assertThat(secondRelationship.targetNode()).isEqualTo(2L);
        assertThat(secondRelationship.values()).containsExactly(Values.doubleValue(45.0), Values.doubleArray(new double[]{ 46.0, 47.0 }));

        assertThat(relationshipIterator).isExhausted();
    }

    @Test
    void shouldWriteRelationshipStreamWithoutPropertiesToResultStore() {
        var jobId = new JobId("test");
        var resultStore = new EphemeralResultStore();
        var relationshipStream = Stream.of(
            ImmutableExportedRelationship.of(0, 1, new Value[0]),
            ImmutableExportedRelationship.of(1, 2, new Value[0])
        );
        LongUnaryOperator mappingOperator = l -> l + 42;
        new ResultStoreRelationshipStreamExporter(jobId, resultStore, relationshipStream, mappingOperator).write(
            "REL",
            List.of(),
            List.of()
        );
        var relationshipStreamEntry = resultStore.getRelationshipStream("REL", List.of());
        assertThat(relationshipStreamEntry).isNotNull();
        assertRelationshipEntryWithoutProperties(
            relationshipStreamEntry.propertyTypes(),
            relationshipStreamEntry.relationshipStream(),
            relationshipStreamEntry.toOriginalId(),
            mappingOperator
        );

        var entry = resultStore.get(jobId);
        assertThat(entry).isInstanceOf(ResultStoreEntry.RelationshipStream.class);
        var jobIdRelationshipStreamEntry = (ResultStoreEntry.RelationshipStream) entry;
        assertThat(jobIdRelationshipStreamEntry.relationshipType()).isEqualTo("REL");
        assertThat(jobIdRelationshipStreamEntry.propertyKeys()).isEmpty();
        assertThat(jobIdRelationshipStreamEntry.propertyTypes()).isEqualTo(relationshipStreamEntry.propertyTypes());
        assertThat(jobIdRelationshipStreamEntry.relationshipStream()).isEqualTo(relationshipStreamEntry.relationshipStream());
    }

    private static void assertRelationshipEntryWithoutProperties(
        List<ValueType> propertyTypes,
        Stream<ExportedRelationship> relationshipStream,
        LongUnaryOperator actualOperator,
        LongUnaryOperator expectedOperator
    ) {
        assertThat(propertyTypes).isEqualTo(List.of());
        assertThat(actualOperator).isEqualTo(expectedOperator);

        var relationshipIterator = relationshipStream.iterator();

        assertThat(relationshipIterator).hasNext();
        var firstRelationship = relationshipIterator.next();
        assertThat(firstRelationship.sourceNode()).isEqualTo(0L);
        assertThat(firstRelationship.targetNode()).isEqualTo(1L);
        assertThat(firstRelationship.values()).isEmpty();

        assertThat(relationshipIterator).hasNext();
        var secondRelationship = relationshipIterator.next();
        assertThat(secondRelationship.sourceNode()).isEqualTo(1L);
        assertThat(secondRelationship.targetNode()).isEqualTo(2L);
        assertThat(secondRelationship.values()).isEmpty();

        assertThat(relationshipIterator).isExhausted();
    }
}

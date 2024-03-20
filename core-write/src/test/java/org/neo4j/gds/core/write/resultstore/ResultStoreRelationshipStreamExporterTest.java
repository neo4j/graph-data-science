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
import org.neo4j.gds.api.ImmutableExportedRelationship;
import org.neo4j.gds.api.nodeproperties.ValueType;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

import java.util.List;
import java.util.function.LongUnaryOperator;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

class ResultStoreRelationshipStreamExporterTest {

    @Test
    void shouldWriteRelationshipStreamWithPropertiesToResultStore() {
        var resultStore = new EphemeralResultStore();
        var relationshipStream = Stream.of(
            ImmutableExportedRelationship.of(0, 1, new Value[]{ Values.doubleValue(42.0), Values.doubleArray(new double[]{ 43.0, 44.0 })}),
            ImmutableExportedRelationship.of(1, 2, new Value[]{ Values.doubleValue(45.0), Values.doubleArray(new double[]{ 46.0, 47.0 })})
        );
        LongUnaryOperator mappingOperator = l -> l + 42;
        new ResultStoreRelationshipStreamExporter(resultStore, relationshipStream, mappingOperator)
            .write(
                "REL",
                List.of("doubleProp", "doubleArrayProp"),
                List.of(ValueType.DOUBLE, ValueType.DOUBLE_ARRAY)
            );
        var relationshipStreamEntry = resultStore.getRelationshipStream("REL", List.of("doubleProp", "doubleArrayProp"));
        assertThat(relationshipStreamEntry).isNotNull();
        assertThat(relationshipStreamEntry.propertyTypes()).isEqualTo(List.of(ValueType.DOUBLE, ValueType.DOUBLE_ARRAY));
        assertThat(relationshipStreamEntry.toOriginalId()).isEqualTo(mappingOperator);

        var relationshipIterator = relationshipStreamEntry.relationshipStream().iterator();

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
        var resultStore = new EphemeralResultStore();
        var relationshipStream = Stream.of(
            ImmutableExportedRelationship.of(0, 1, new Value[0]),
            ImmutableExportedRelationship.of(1, 2, new Value[0])
        );
        LongUnaryOperator mappingOperator = l -> l + 42;
        new ResultStoreRelationshipStreamExporter(resultStore, relationshipStream, mappingOperator).write(
            "REL",
            List.of(),
            List.of()
        );
        var relationshipStreamEntry = resultStore.getRelationshipStream("REL", List.of());
        assertThat(relationshipStreamEntry).isNotNull();
        assertThat(relationshipStreamEntry.propertyTypes()).isEqualTo(List.of());
        assertThat(relationshipStreamEntry.toOriginalId()).isEqualTo(mappingOperator);

        var relationshipIterator = relationshipStreamEntry.relationshipStream().iterator();

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

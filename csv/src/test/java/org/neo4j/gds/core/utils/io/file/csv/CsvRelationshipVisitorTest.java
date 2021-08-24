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
package org.neo4j.gds.core.utils.io.file.csv;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.api.nodeproperties.ValueType;
import org.neo4j.gds.api.schema.RelationshipSchema;

import java.util.Collections;
import java.util.List;
import java.util.Set;

import static org.neo4j.gds.core.utils.io.file.csv.CsvRelationshipVisitor.END_ID_COLUMN_NAME;
import static org.neo4j.gds.core.utils.io.file.csv.CsvRelationshipVisitor.START_ID_COLUMN_NAME;

class CsvRelationshipVisitorTest extends CsvVisitorTest {

    @Test
    void visitRelationshipsWithTypes() {
        var relationshipVisitor = new CsvRelationshipVisitor(tempDir, RelationshipSchema.builder().build());

        relationshipVisitor.startId(0L);
        relationshipVisitor.endId(1L);
        relationshipVisitor.type("Foo");
        relationshipVisitor.endOfEntity();

        relationshipVisitor.startId(1L);
        relationshipVisitor.endId(2L);
        relationshipVisitor.type("Bar");
        relationshipVisitor.endOfEntity();

        relationshipVisitor.startId(2L);
        relationshipVisitor.endId(0L);
        relationshipVisitor.type("Foo");
        relationshipVisitor.endOfEntity();
        
        relationshipVisitor.close();

        assertCsvFiles(List.of("relationships_Foo_0.csv", "relationships_Foo_header.csv", "relationships_Bar_0.csv", "relationships_Bar_header.csv"));
        assertHeaderFile("relationships_Foo_header.csv", Collections.emptyMap());
        assertDataContent(
            "relationships_Foo_0.csv",
            List.of(
                List.of("0","1"),
                List.of("2", "0")
            )
        );

        assertHeaderFile("relationships_Bar_header.csv", Collections.emptyMap());
        assertDataContent(
            "relationships_Bar_0.csv",
            List.of(
                List.of("1", "2")
            )
        );
    }
    
    @Test
    void visitNodesWithLabelsAndProperties() {
        var aType = RelationshipType.of("A");
        var bType = RelationshipType.of("B");

        var relationshipSchema = RelationshipSchema.builder()
            .addProperty(aType, "foo", ValueType.LONG)
            .addProperty(aType, "bar", ValueType.LONG)

            .addProperty(bType, "bar", ValueType.LONG)
            .addProperty(bType, "baz", ValueType.DOUBLE)

            .build();
        var relationshipVisitor= new CsvRelationshipVisitor(tempDir, relationshipSchema);

        // :A
        relationshipVisitor.startId(0L);
        relationshipVisitor.endId(1L);
        relationshipVisitor.type("A");
        relationshipVisitor.property("foo", 42L);
        relationshipVisitor.property("bar", 21L);
        relationshipVisitor.endOfEntity();

        // :B
        relationshipVisitor.startId(1L);
        relationshipVisitor.endId(2L);
        relationshipVisitor.type("B");
        relationshipVisitor.property("bar", 21L);
        relationshipVisitor.property("baz", 21.0);
        relationshipVisitor.endOfEntity();

        // :A
        relationshipVisitor.startId(2L);
        relationshipVisitor.endId(0L);
        relationshipVisitor.type("A");
        relationshipVisitor.endOfEntity();

        relationshipVisitor.close();

        assertCsvFiles(List.of(
            "relationships_A_0.csv", "relationships_A_header.csv",
            "relationships_B_0.csv", "relationships_B_header.csv"
        ));

        assertHeaderFile("relationships_A_header.csv", relationshipSchema.filter(Set.of(aType)).unionProperties());
        assertDataContent(
            "relationships_A_0.csv",
            List.of(  //src  tgt  bar   foo
                List.of("0", "1", "21", "42"),
                List.of("2", "0", "",   "")
            )
        );

        assertHeaderFile("relationships_B_header.csv", relationshipSchema.filter(Set.of(bType)).unionProperties());
        assertDataContent(
            "relationships_B_0.csv",
            List.of(  //src  tgt  bar   baz
                List.of("1", "2", "21", "21.0")
            )
        );

    }

    @Override
    protected List<String> defaultHeaderColumns() {
        return List.of(START_ID_COLUMN_NAME, END_ID_COLUMN_NAME);
    }
}

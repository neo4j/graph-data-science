/*
 * Copyright (c) 2017-2020 "Neo4j,"
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
package org.neo4j.graphalgo.core.utils.export.file.csv;

import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.RelationshipType;
import org.neo4j.graphalgo.api.nodeproperties.ValueType;
import org.neo4j.graphalgo.api.schema.GraphSchema;
import org.neo4j.graphalgo.api.schema.NodeSchema;
import org.neo4j.graphalgo.api.schema.RelationshipSchema;

import java.util.Collections;
import java.util.List;
import java.util.Set;

import static org.neo4j.graphalgo.core.utils.export.file.csv.CsvRelationshipVisitor.END_ID_COLUMN_NAME;
import static org.neo4j.graphalgo.core.utils.export.file.csv.CsvRelationshipVisitor.START_ID_COLUMN_NAME;

class CsvRelationshipVisitorTest extends CsvVisitorTest{

    @Test
    void visitRelationshipsWithTypes() {
        var relationshipVisitor = new CsvRelationshipVisitor(tempDir, GraphSchema.empty());

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

        assertCsvFiles(List.of("relationship_Foo.csv", "relationship_Foo_header.csv", "relationship_Bar.csv", "relationship_Bar_header.csv"));
        assertHeaderFile("relationship_Foo_header.csv", Collections.emptyMap());
        assertDataContent(
            "relationship_Foo.csv",
            List.of(
                List.of("0","1"),
                List.of("2", "0")
            )
        );

        assertHeaderFile("relationship_Bar_header.csv", Collections.emptyMap());
        assertDataContent(
            "relationship_Bar.csv",
            List.of(
                List.of("1", "2")
            )
        );
    }
    
    @Test
    void visitNodesWithLabelsAndProperties() {
        var aType = RelationshipType.of("A");
        var bType = RelationshipType.of("B");

        var graphSchema = GraphSchema.of(
            NodeSchema.builder().build(),
            RelationshipSchema.builder()
                .addProperty(aType, "foo", ValueType.LONG)
                .addProperty(aType, "bar", ValueType.LONG)

                .addProperty(bType, "bar", ValueType.LONG)
                .addProperty(bType, "baz", ValueType.DOUBLE)

                .build()
        );
        var relationshipVisitor= new CsvRelationshipVisitor(tempDir, graphSchema);

        // :A
        relationshipVisitor.startId(0L);
        relationshipVisitor.endId(1L);
        relationshipVisitor.type("A");
        relationshipVisitor.property("foo", 42);
        relationshipVisitor.property("bar", 21);
        relationshipVisitor.endOfEntity();

        // :B
        relationshipVisitor.startId(1L);
        relationshipVisitor.endId(2L);
        relationshipVisitor.type("B");
        relationshipVisitor.property("bar", 21);
        relationshipVisitor.property("baz", 21.0);
        relationshipVisitor.endOfEntity();

        // :A
        relationshipVisitor.startId(2L);
        relationshipVisitor.endId(0L);
        relationshipVisitor.type("A");
        relationshipVisitor.endOfEntity();

        relationshipVisitor.close();

        assertCsvFiles(List.of(
            "relationship_A.csv", "relationship_A_header.csv",
            "relationship_B.csv", "relationship_B_header.csv"
        ));

        assertHeaderFile("relationship_A_header.csv", graphSchema.relationshipSchema().filter(Set.of(aType)).unionProperties());
        assertDataContent(
            "relationship_A.csv",
            List.of(  //src  tgt  bar   foo
                List.of("0", "1", "21", "42"),
                List.of("2", "0", "",   "")
            )
        );

        assertHeaderFile("relationship_B_header.csv", graphSchema.relationshipSchema().filter(Set.of(bType)).unionProperties());
        assertDataContent(
            "relationship_B.csv",
            List.of(  //src  tgt  bar   baz
                List.of("1", "2", "21", "21.0")
            )
        );

    }

    @Override
    List<String> defaultHeaderColumns() {
        return List.of(START_ID_COLUMN_NAME, END_ID_COLUMN_NAME);
    }
}

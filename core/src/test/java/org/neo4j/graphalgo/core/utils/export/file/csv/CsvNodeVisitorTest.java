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
import org.neo4j.graphalgo.NodeLabel;
import org.neo4j.graphalgo.api.nodeproperties.ValueType;
import org.neo4j.graphalgo.api.schema.NodeSchema;

import java.util.Collections;
import java.util.List;
import java.util.Set;

import static org.neo4j.graphalgo.core.utils.export.file.csv.CsvNodeVisitor.ID_COLUMN_NAME;

class CsvNodeVisitorTest extends CsvVisitorTest{

    @Test
    void visitNodesWithoutLabelsAndProperties() {
        var nodeVisitor = new CsvNodeVisitor(tempDir, NodeSchema.builder().build());

        nodeVisitor.id(0L);
        nodeVisitor.endOfEntity();
        nodeVisitor.id(1L);
        nodeVisitor.endOfEntity();
        nodeVisitor.close();

        assertCsvFiles(List.of("nodes___ALL__.csv", "nodes___ALL___header.csv"));
        assertHeaderFile("nodes___ALL___header.csv", Collections.emptyMap());
        assertDataContent(
            "nodes___ALL__.csv",
            List.of(
                List.of("0"),
                List.of("1")
            )
        );
    }

    @Test
    void visitNodesWithLabels() {
        var nodeVisitor = new CsvNodeVisitor(tempDir, NodeSchema.builder().build());

        nodeVisitor.id(0L);
        nodeVisitor.labels(new String[]{"Foo", "Bar"});
        nodeVisitor.endOfEntity();
        
        nodeVisitor.id(1L);
        nodeVisitor.labels(new String[]{"Baz"});
        nodeVisitor.endOfEntity();

        nodeVisitor.id(2L);
        nodeVisitor.labels(new String[]{"Foo", "Bar"});
        nodeVisitor.endOfEntity();
        
        nodeVisitor.close();

        assertCsvFiles(List.of("nodes_Bar_Foo.csv", "nodes_Bar_Foo_header.csv", "nodes_Baz.csv", "nodes_Baz_header.csv"));
        assertHeaderFile("nodes_Bar_Foo_header.csv", Collections.emptyMap());
        assertDataContent(
            "nodes_Bar_Foo.csv",
            List.of(
                List.of("0"),
                List.of("2")
            )
        );

        assertHeaderFile("nodes_Baz_header.csv", Collections.emptyMap());
        assertDataContent(
            "nodes_Baz.csv",
            List.of(
                List.of("1")
            )
        );
    }

    @Test
    void visitNodesWithProperties() {
        var nodeSchema = NodeSchema.builder()
            .addProperty(NodeLabel.ALL_NODES, "foo", ValueType.LONG)
            .addProperty(NodeLabel.ALL_NODES, "bar", ValueType.LONG)
            .build();
        var nodeVisitor = new CsvNodeVisitor(tempDir, nodeSchema);

        nodeVisitor.id(0L);
        nodeVisitor.property("foo", 42.0);
        nodeVisitor.property("bar", 21.0);
        nodeVisitor.endOfEntity();

        nodeVisitor.id(1L);
        nodeVisitor.property("foo", 42.0);
        nodeVisitor.endOfEntity();

        nodeVisitor.id(2L);
        nodeVisitor.property("bar", 21.0);
        nodeVisitor.endOfEntity();

        nodeVisitor.close();

        assertCsvFiles(List.of("nodes___ALL__.csv", "nodes___ALL___header.csv"));
        assertHeaderFile("nodes___ALL___header.csv", nodeSchema.unionProperties());
        assertDataContent(
            "nodes___ALL__.csv",
            List.of(
                List.of("0", "21.0", "42.0"),
                List.of("1", "", "42.0"),
                List.of("2", "21.0", "")
            )
        );
    }

    @Test
    void visitNodesWithLabelsAndProperties() {
        var aLabel = NodeLabel.of("A");
        var bLabel = NodeLabel.of("B");
        var cLabel = NodeLabel.of("C");

        var nodeSchema = NodeSchema.builder()
            .addProperty(aLabel, "foo", ValueType.LONG)
            .addProperty(aLabel, "bar", ValueType.LONG)

            .addProperty(bLabel, "bar", ValueType.LONG)
            .addProperty(bLabel, "baz", ValueType.DOUBLE)

            .addProperty(cLabel, "isolated", ValueType.DOUBLE)

            .build();
        var nodeVisitor = new CsvNodeVisitor(tempDir, nodeSchema);

        // :A:B
        nodeVisitor.id(0L);
        nodeVisitor.labels(new String[]{"A", "B"});
        nodeVisitor.property("foo", 42);
        nodeVisitor.property("bar", 21);
        nodeVisitor.property("baz", 21.0);
        nodeVisitor.endOfEntity();

        // :A
        nodeVisitor.id(1L);
        nodeVisitor.labels(new String[]{"A"});
        nodeVisitor.property("foo", 42);
        nodeVisitor.property("bar", 21);
        nodeVisitor.endOfEntity();

        // :B
        nodeVisitor.id(2L);
        nodeVisitor.labels(new String[]{"B"});
        nodeVisitor.property("bar", 21);
        nodeVisitor.property("baz", 21.0);
        nodeVisitor.endOfEntity();

        // :C
        nodeVisitor.id(3L);
        nodeVisitor.labels(new String[]{"C"});
        nodeVisitor.property("isolated", 1337.0);
        nodeVisitor.endOfEntity();

        // :A:B
        nodeVisitor.id(4L);
        nodeVisitor.labels(new String[]{"A", "B"});
        nodeVisitor.property("bar", 21);
        nodeVisitor.property("baz", 21.0);
        nodeVisitor.endOfEntity();

        nodeVisitor.close();

        assertCsvFiles(List.of(
            "nodes_A_B.csv", "nodes_A_B_header.csv",
            "nodes_A.csv", "nodes_A_header.csv",
            "nodes_B.csv", "nodes_B_header.csv",
            "nodes_C.csv", "nodes_C_header.csv"
        ));

        assertHeaderFile("nodes_A_B_header.csv", nodeSchema.filter(Set.of(aLabel, bLabel)).unionProperties());
        assertDataContent(
            "nodes_A_B.csv",
            List.of(  //id   bar   baz     foo
                List.of("0", "21", "21.0", "42"),
                List.of("4", "21", "21.0", "")
            )
        );

        assertHeaderFile("nodes_A_header.csv", nodeSchema.filter(Set.of(aLabel)).unionProperties());
        assertDataContent(
            "nodes_A.csv",
            List.of(  //id   bar    foo
                List.of("1", "21", "42")
            )
        );

        assertHeaderFile("nodes_B_header.csv", nodeSchema.filter(Set.of(bLabel)).unionProperties());
        assertDataContent(
            "nodes_B.csv",
            List.of(  //id   bar    baz
                List.of("2", "21", "21.0")
            )
        );

        assertHeaderFile("nodes_C_header.csv", nodeSchema.filter(Set.of(cLabel)).unionProperties());
        assertDataContent(
            "nodes_C.csv",
            List.of(  //id   isolated
                List.of("3", "1337.0")
            )
        );

    }

    @Override
    List<String> defaultHeaderColumns() {
        return List.of(ID_COLUMN_NAME);
    }
}

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
package org.neo4j.gds.core.io.file.csv;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.api.nodeproperties.ValueType;
import org.neo4j.gds.api.schema.MutableNodeSchema;

import java.util.Collections;
import java.util.List;
import java.util.Set;

import static org.neo4j.gds.core.io.file.csv.CsvNodeVisitor.ID_COLUMN_NAME;

class CsvNodeVisitorTest extends CsvVisitorTest {

    @Test
    void visitNodesWithoutLabelsAndProperties() {
        var nodeVisitor = new CsvNodeVisitor(tempDir, MutableNodeSchema.empty());

        nodeVisitor.id(0L);
        nodeVisitor.endOfEntity();
        nodeVisitor.id(1L);
        nodeVisitor.endOfEntity();
        nodeVisitor.close();

        assertCsvFiles(List.of("nodes_0.csv", "nodes_header.csv"));
        assertHeaderFile("nodes_header.csv", Collections.emptyMap());
        assertDataContent(
            "nodes_0.csv",
            List.of(
                List.of("0"),
                List.of("1")
            )
        );
    }

    @Test
    void visitNodesWithLabels() {
        var nodeVisitor = new CsvNodeVisitor(tempDir, MutableNodeSchema.empty());

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

        assertCsvFiles(List.of("nodes_Bar_Foo_0.csv", "nodes_Bar_Foo_header.csv", "nodes_Baz_0.csv", "nodes_Baz_header.csv"));
        assertHeaderFile("nodes_Bar_Foo_header.csv", Collections.emptyMap());
        assertDataContent(
            "nodes_Bar_Foo_0.csv",
            List.of(
                List.of("0"),
                List.of("2")
            )
        );

        assertHeaderFile("nodes_Baz_header.csv", Collections.emptyMap());
        assertDataContent(
            "nodes_Baz_0.csv",
            List.of(
                List.of("1")
            )
        );
    }

    @Test
    void visitNodesWithProperties() {
        var nodeSchema = MutableNodeSchema.empty();
        nodeSchema.getOrCreateLabel(NodeLabel.ALL_NODES)
            .addProperty("foo", ValueType.DOUBLE)
            .addProperty("bar", ValueType.DOUBLE);
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

        assertCsvFiles(List.of("nodes_0.csv", "nodes_header.csv"));
        assertHeaderFile("nodes_header.csv", nodeSchema.unionProperties());
        assertDataContent(
            "nodes_0.csv",
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

        var nodeSchema = MutableNodeSchema.empty();
        nodeSchema.getOrCreateLabel(aLabel)
            .addProperty("foo", ValueType.LONG)
            .addProperty("bar", ValueType.LONG);

        nodeSchema.getOrCreateLabel(bLabel)
            .addProperty("bar", ValueType.LONG)
            .addProperty("baz", ValueType.DOUBLE);

        nodeSchema.getOrCreateLabel(cLabel)
            .addProperty("isolated", ValueType.DOUBLE)
            .addProperty("isolated_array", ValueType.LONG_ARRAY);

        var nodeVisitor = new CsvNodeVisitor(tempDir, nodeSchema);

        // :A:B
        nodeVisitor.id(0L);
        nodeVisitor.labels(new String[]{"A", "B"});
        nodeVisitor.property("foo", 42L);
        nodeVisitor.property("bar", 21L);
        nodeVisitor.property("baz", 21.0);
        nodeVisitor.endOfEntity();

        // :A
        nodeVisitor.id(1L);
        nodeVisitor.labels(new String[]{"A"});
        nodeVisitor.property("foo", 42L);
        nodeVisitor.property("bar", 21L);
        nodeVisitor.endOfEntity();

        // :B
        nodeVisitor.id(2L);
        nodeVisitor.labels(new String[]{"B"});
        nodeVisitor.property("bar", 21L);
        nodeVisitor.property("baz", 21.0);
        nodeVisitor.endOfEntity();

        // :C
        nodeVisitor.id(3L);
        nodeVisitor.labels(new String[]{"C"});
        nodeVisitor.property("isolated", 1337.0);
        nodeVisitor.property("isolated_array", new long[]{1L, 42L, 19L});
        nodeVisitor.endOfEntity();

        // :A:B
        nodeVisitor.id(4L);
        nodeVisitor.labels(new String[]{"A", "B"});
        nodeVisitor.property("bar", 21L);
        nodeVisitor.property("baz", 21.0);
        nodeVisitor.endOfEntity();

        nodeVisitor.close();

        assertCsvFiles(List.of(
            "nodes_A_B_0.csv", "nodes_A_B_header.csv",
            "nodes_A_0.csv", "nodes_A_header.csv",
            "nodes_B_0.csv", "nodes_B_header.csv",
            "nodes_C_0.csv", "nodes_C_header.csv"
        ));

        assertHeaderFile("nodes_A_B_header.csv", nodeSchema.filter(Set.of(aLabel, bLabel)).unionProperties());
        assertDataContent(
            "nodes_A_B_0.csv",
            List.of(  //id   bar   baz     foo
                List.of("0", "21", "21.0", "42"),
                List.of("4", "21", "21.0", "")
            )
        );

        assertHeaderFile("nodes_A_header.csv", nodeSchema.filter(Set.of(aLabel)).unionProperties());
        assertDataContent(
            "nodes_A_0.csv",
            List.of(  //id   bar    foo
                List.of("1", "21", "42")
            )
        );

        assertHeaderFile("nodes_B_header.csv", nodeSchema.filter(Set.of(bLabel)).unionProperties());
        assertDataContent(
            "nodes_B_0.csv",
            List.of(  //id   bar    baz
                List.of("2", "21", "21.0")
            )
        );

        assertHeaderFile("nodes_C_header.csv", nodeSchema.filter(Set.of(cLabel)).unionProperties());
        assertDataContent(
            "nodes_C_0.csv",
            List.of(  //id   isolated
                List.of("3", "1337.0", "1;42;19")
            )
        );

    }

    @Override
    protected List<String> defaultHeaderColumns() {
        return List.of(ID_COLUMN_NAME);
    }
}

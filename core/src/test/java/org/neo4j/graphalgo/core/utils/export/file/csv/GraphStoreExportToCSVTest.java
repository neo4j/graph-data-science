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
import org.neo4j.graphalgo.RelationshipType;
import org.neo4j.graphalgo.api.GraphStore;
import org.neo4j.graphalgo.core.utils.export.GraphStoreExportToCSV;
import org.neo4j.graphalgo.core.utils.export.ImmutableGraphStoreFileExportConfig;
import org.neo4j.graphalgo.core.utils.mem.AllocationTracker;
import org.neo4j.graphalgo.extension.GdlExtension;
import org.neo4j.graphalgo.extension.GdlGraph;
import org.neo4j.graphalgo.extension.Inject;

import java.util.List;
import java.util.Set;

import static org.neo4j.graphalgo.core.utils.export.file.csv.CsvNodeVisitor.ID_COLUMN_NAME;
import static org.neo4j.graphalgo.core.utils.export.file.csv.CsvRelationshipVisitor.END_ID_COLUMN_NAME;
import static org.neo4j.graphalgo.core.utils.export.file.csv.CsvRelationshipVisitor.START_ID_COLUMN_NAME;

@GdlExtension
class GraphStoreExportToCSVTest extends CsvTest {

    @GdlGraph
    private static final String GDL =  "CREATE" +
                  "  (a:A:B { prop1: 0, prop2: 42, prop3: [1L, 3L, 3L, 7L]})" +
                  ", (b:A:B { prop1: 1, prop2: 43})" +
                  ", (c:A:C { prop1: 2, prop2: 44, prop3: [1L, 9L, 8L, 4L] })" +
                  ", (d:B { prop1: 3 })" +
                  ", (a)-[:REL1 { prop1: 0, prop2: 42 }]->(a)" +
                  ", (a)-[:REL1 { prop1: 1, prop2: 43 }]->(b)" +
                  ", (b)-[:REL1 { prop1: 2, prop2: 44 }]->(a)" +
                  ", (b)-[:REL2 { prop3: 3, prop4: 45 }]->(c)" +
                  ", (c)-[:REL2 { prop3: 4, prop4: 46 }]->(d)" +
                  ", (d)-[:REL2 { prop3: 5, prop4: 47 }]->(a)";

    @Inject
    public GraphStore graphStore;

    @Test
    void exportTopology() {
        var config = ImmutableGraphStoreFileExportConfig
            .builder()
            .exportLocation(tempDir.toString())
            .writeConcurrency(1)
            .build();

        // export db
        var graphStoreExport = new GraphStoreExportToCSV(graphStore, config);
        graphStoreExport.run(AllocationTracker.empty());

        var aLabel = NodeLabel.of("A");
        var bLabel = NodeLabel.of("B");
        var cLabel = NodeLabel.of("C");
        var rel1Type = RelationshipType.of("REL1");
        var rel2Type = RelationshipType.of("REL2");

        var abSchema = graphStore.schema().nodeSchema().filter(Set.of(aLabel, bLabel)).unionProperties();
        var acSchema = graphStore.schema().nodeSchema().filter(Set.of(aLabel, cLabel)).unionProperties();
        var bSchema = graphStore.schema().nodeSchema().filter(Set.of(bLabel)).unionProperties();
        var rel1Schema = graphStore.schema().relationshipSchema().filter(Set.of(rel1Type)).unionProperties();
        var rel2Schema = graphStore.schema().relationshipSchema().filter(Set.of(rel2Type)).unionProperties();

        var nodeColumns = List.of(ID_COLUMN_NAME);
        var relationshipColumns = List.of(START_ID_COLUMN_NAME, END_ID_COLUMN_NAME);

        assertCsvFiles(List.of(
            "nodes_A_B.csv", "nodes_A_B_header.csv",
            "nodes_A_C.csv", "nodes_A_C_header.csv",
            "nodes_B.csv", "nodes_B_header.csv",
            "relationship_REL1.csv", "relationship_REL1_header.csv",
            "relationship_REL2.csv", "relationship_REL2_header.csv"
        ));

        // Assert nodes

        assertHeaderFile("nodes_A_B_header.csv", nodeColumns, abSchema);
        assertDataContent(
            "nodes_A_B.csv",
            List.of(
                List.of("0", "0", "42", "1;3;3;7"),
                List.of("1", "1", "43", "")
            )
        );

        assertHeaderFile("nodes_A_C_header.csv", nodeColumns, acSchema);
        assertDataContent(
            "nodes_A_C.csv",
            List.of(
                List.of("2", "2", "44", "1;9;8;4")
            )
        );

        assertHeaderFile("nodes_B_header.csv", nodeColumns, bSchema);
        assertDataContent(
            "nodes_B.csv",
            List.of(
                List.of("3", "3", "", "")
            )
        );

        // assert relationships

        assertHeaderFile("relationship_REL1_header.csv", relationshipColumns, rel1Schema);
        assertDataContent(
            "relationship_REL1.csv",
            List.of(
                List.of("0","0","42.0"),
                List.of("0","1","43.0"),
                List.of("1","0","44.0")
            )
        );

        assertHeaderFile("relationship_REL2_header.csv", relationshipColumns, rel2Schema);
        assertDataContent(
            "relationship_REL2.csv",
            List.of(
                List.of("1","2","45.0"),
                List.of("2","3","46.0"),
                List.of("3","0","47.0")
            )
        );

    }

}

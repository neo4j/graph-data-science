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

import org.apache.commons.lang3.mutable.MutableLong;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.api.PropertyState;
import org.neo4j.gds.api.nodeproperties.ValueType;
import org.neo4j.gds.api.properties.graph.LongGraphPropertyValues;
import org.neo4j.gds.core.Aggregation;
import org.neo4j.gds.core.io.file.ImmutableGraphStoreToFileExporterConfig;
import org.neo4j.gds.core.loading.Capabilities.WriteMode;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.IdFunction;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.gdl.GdlFactory;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import static java.util.function.Predicate.not;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.neo4j.gds.Orientation.NATURAL;
import static org.neo4j.gds.Orientation.REVERSE;
import static org.neo4j.gds.Orientation.UNDIRECTED;
import static org.neo4j.gds.core.io.file.csv.CsvGraphCapabilitiesWriter.GRAPH_CAPABILITIES_FILE_NAME;
import static org.neo4j.gds.core.io.file.csv.CsvGraphInfoVisitor.GRAPH_INFO_FILE_NAME;
import static org.neo4j.gds.core.io.file.csv.CsvGraphPropertySchemaVisitor.GRAPH_PROPERTY_SCHEMA_FILE_NAME;
import static org.neo4j.gds.core.io.file.csv.CsvNodeSchemaVisitor.NODE_SCHEMA_FILE_NAME;
import static org.neo4j.gds.core.io.file.csv.CsvNodeVisitor.ID_COLUMN_NAME;
import static org.neo4j.gds.core.io.file.csv.CsvRelationshipSchemaVisitor.RELATIONSHIP_SCHEMA_FILE_NAME;
import static org.neo4j.gds.core.io.file.csv.CsvRelationshipVisitor.END_ID_COLUMN_NAME;
import static org.neo4j.gds.core.io.file.csv.CsvRelationshipVisitor.START_ID_COLUMN_NAME;
import static org.neo4j.gds.core.io.file.csv.CsvSchemaConstants.GRAPH_PROPERTY_SCHEMA_COLUMNS;
import static org.neo4j.gds.core.io.file.csv.CsvSchemaConstants.NODE_SCHEMA_COLUMNS;
import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

@GdlExtension
class GraphStoreToCsvExporterTest extends CsvTest {

    @GdlGraph(orientation = UNDIRECTED, propertyState = PropertyState.PERSISTENT)
    private static final String GDL =
        "CREATE" +
        "  (a:A:B { prop1: 0, prop2: 42, prop3: [1L, 3L, 3L, 7L]})" +
        ", (b:A:B { prop1: 1, prop2: 43})" +
        ", (c:A:C { prop1: 2, prop2: 44, prop3: [1L, 9L, 8L, 4L] })" +
        ", (d:B { prop1: 3 })" +
        ", (a)-[:REL1 { averylongnamethatisgreaterthantwentyfour: 0, prop2: 42 }]->(a)" +
        ", (a)-[:REL1 { averylongnamethatisgreaterthantwentyfour: 1, prop2: 43 }]->(b)" +
        ", (b)-[:REL1 { averylongnamethatisgreaterthantwentyfour: 2, prop2: 44 }]->(a)" +
        ", (b)-[:REL2 { prop3: 3, prop4: 45 }]->(c)" +
        ", (c)-[:REL2 { prop3: 4, prop4: 46 }]->(d)" +
        ", (d)-[:REL2 { prop3: 5, prop4: 47 }]->(a)";

    @Inject
    public GraphStore graphStore;

    @Inject
    private IdFunction idFunction;

    @GdlGraph(graphNamePrefix = "concurrent", orientation = NATURAL, propertyState = PropertyState.PERSISTENT)
    private static final String GDL_FOR_CONCURRENCY =
        "CREATE" +
        "  (a)" +
        ", (b)" +
        ", (c)" +
        ", (d)" +
        ", (a)-[:REL1]->(a)" +
        ", (a)-[:REL1]->(b)" +
        ", (b)-[:REL1]->(a)" +
        ", (b)-[:REL1]->(c)" +
        ", (c)-[:REL1]->(d)" +
        ", (d)-[:REL1]->(a)";

    @Inject
    public GraphStore concurrentGraphStore;

    @GdlGraph(graphNamePrefix = "noProperties", orientation = REVERSE)
    private static final String GDL_WITHOUT_PROPERTIES =
        "CREATE" +
        "  (a:A)" +
        ", (b:A)" +
        ", (c:B)" +
        ", (d:C)" +
        ", (a)-[:REL1]->(a)" +
        ", (a)-[:REL1]->(b)" +
        ", (b)-[:REL2]->(a)" +
        ", (b)-[:REL2]->(c)" +
        ", (c)-[:REL3]->(d)" +
        ", (d)-[:REL4]->(a)";

    @Inject
    public GraphStore noPropertiesGraphStore;

    private static final List<String> NODE_COLUMNS = List.of(ID_COLUMN_NAME);
    private static final List<String> RELATIONSHIP_COLUMNS = List.of(START_ID_COLUMN_NAME, END_ID_COLUMN_NAME);

    @AfterEach
    void tearDown() {
        graphStore.graphPropertyKeys().forEach(graphStore::removeGraphProperty);
    }

    @Test
    void exportTopology() {
        var config = ImmutableGraphStoreToFileExporterConfig
            .builder()
            .exportName(tempDir.toString())
            .writeConcurrency(1)
            .includeMetaData(true)
            .build();

        // export db
        var exporter = GraphStoreToCsvExporter.create(graphStore, config, tempDir);
        exporter.run();

        var aLabel = NodeLabel.of("A");
        var bLabel = NodeLabel.of("B");
        var cLabel = NodeLabel.of("C");
        var rel1Type = RelationshipType.of("REL1");
        var rel2Type = RelationshipType.of("REL2");

        var nodeSchema = graphStore.schema().nodeSchema();
        var abSchema = nodeSchema.filter(Set.of(aLabel, bLabel)).unionProperties();
        var acSchema = nodeSchema.filter(Set.of(aLabel, cLabel)).unionProperties();
        var bSchema = nodeSchema.filter(Set.of(bLabel)).unionProperties();
        var rel1Schema = graphStore.schema().relationshipSchema().filter(Set.of(rel1Type)).unionProperties();
        var rel2Schema = graphStore.schema().relationshipSchema().filter(Set.of(rel2Type)).unionProperties();

        assertCsvFiles(List.of(
            "nodes_A_B_0.csv", "nodes_A_B_header.csv",
            "nodes_A_C_0.csv", "nodes_A_C_header.csv",
            "nodes_B_0.csv", "nodes_B_header.csv",
            "relationships_REL1_0.csv", "relationships_REL1_header.csv",
            "relationships_REL2_0.csv", "relationships_REL2_header.csv"
        ));

        // Assert nodes

        assertHeaderFile("nodes_A_B_header.csv", NODE_COLUMNS, abSchema);
        assertDataContent(
            "nodes_A_B_0.csv",
            List.of(
                List.of(stringIdOf("a"), "0", "42", "1;3;3;7"),
                List.of(stringIdOf("b"), "1", "43", "")
            )
        );

        assertHeaderFile("nodes_A_C_header.csv", NODE_COLUMNS, acSchema);
        assertDataContent(
            "nodes_A_C_0.csv",
            List.of(
                List.of(stringIdOf("c"), "2", "44", "1;9;8;4")
            )
        );

        assertHeaderFile("nodes_B_header.csv", NODE_COLUMNS, bSchema);
        assertDataContent(
            "nodes_B_0.csv",
            List.of(
                List.of(stringIdOf("d"), "3", "", "")
            )
        );

        // assert relationships

        assertHeaderFile("relationships_REL1_header.csv", RELATIONSHIP_COLUMNS, rel1Schema);
        assertDataContent(
            "relationships_REL1_0.csv",
            List.of(
                List.of(stringIdOf("a"), stringIdOf("a"), "0.0", "42.0"),
                List.of(stringIdOf("a"), stringIdOf("a"), "0.0", "42.0"),
                List.of(stringIdOf("a"), stringIdOf("b"), "1.0", "43.0"),
                List.of(stringIdOf("b"), stringIdOf("a"), "1.0", "43.0"),
                List.of(stringIdOf("b"), stringIdOf("a"), "2.0", "44.0"),
                List.of(stringIdOf("a"), stringIdOf("b"), "2.0", "44.0")

            )
        );

        assertHeaderFile("relationships_REL2_header.csv", RELATIONSHIP_COLUMNS, rel2Schema);
        assertDataContent(
            "relationships_REL2_0.csv",
            List.of(
                List.of(stringIdOf("b"), stringIdOf("c"), "3.0", "45.0"),
                List.of(stringIdOf("c"), stringIdOf("b"), "3.0", "45.0"),
                List.of(stringIdOf("c"), stringIdOf("d"), "4.0", "46.0"),
                List.of(stringIdOf("d"), stringIdOf("c"), "4.0", "46.0"),
                List.of(stringIdOf("d"), stringIdOf("a"), "5.0", "47.0"),
                List.of(stringIdOf("a"), stringIdOf("d"), "5.0", "47.0")
            )
        );

    }

    @Test
    void shouldExportGraphProperties() {
        var config = ImmutableGraphStoreToFileExporterConfig
            .builder()
            .exportName(tempDir.toString())
            .writeConcurrency(1)
            .includeMetaData(false)
            .build();

        var graphPropertyValues = new LongGraphPropertyValues() {
            @Override
            public long valueCount() {
                return 3;
            }

            @Override
            public LongStream longValues() {
                return LongStream.range(0, valueCount());
            }
        };

        graphStore.addGraphProperty("graphProp", graphPropertyValues);

        var exporter = GraphStoreToCsvExporter.create(graphStore, config, tempDir);
        exporter.run();

        assertCsvFiles(List.of("graph_property_graphProp_0.csv", "graph_property_graphProp_header.csv"));

        assertDataContent(
            "graph_property_graphProp_0.csv",
            List.of(
                List.of("0"),
                List.of("1"),
                List.of("2")
            )
        );

        assertDataContent(
            "graph_property_graphProp_header.csv",
            List.of(
                List.of("graphProp:long")
            )
        );
    }

    @Test
    void exportMultithreaded() {
        var config = ImmutableGraphStoreToFileExporterConfig
            .builder()
            .exportName(tempDir.toString())
            .writeConcurrency(2)
            .includeMetaData(true)
            .build();

        // export db
        var exporter = GraphStoreToCsvExporter.create(concurrentGraphStore, config, tempDir);
        exporter.run();

        // Assert headers
        var nodeSchema = concurrentGraphStore.schema().nodeSchema();
        assertHeaderFile("nodes_header.csv", NODE_COLUMNS, nodeSchema.unionProperties());
        assertHeaderFile("relationships_REL1_header.csv", RELATIONSHIP_COLUMNS, Collections.emptyMap());

        // Sometimes we end up with only one file, so we cannot make absolute assumptions about the files created
        var nodeContents = Arrays.stream(Objects.requireNonNull(tempDir
            .toFile()
            .listFiles((file, name) -> name.matches("nodes(_.+)?(_\\d+).csv"))))
            .map(File::toPath)
            .flatMap(path -> {
                try {
                    return Files.readAllLines(path).stream();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }).collect(Collectors.toList());
        assertThat(nodeContents).containsExactlyInAnyOrder(
            Long.toString(idFunction.of("a")),
            Long.toString(idFunction.of("b")),
            Long.toString(idFunction.of("c")),
            Long.toString(idFunction.of("d"))
        );

        var relationshipContents = Arrays.stream(Objects.requireNonNull(tempDir
            .toFile()
            .listFiles((file, name) -> name.matches("relationships(_.+)?(_\\d+).csv"))))
            .map(File::toPath)
            .flatMap(path -> {
                try {
                    return Files.readAllLines(path).stream();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }).collect(Collectors.toList());
        assertThat(relationshipContents).containsExactlyInAnyOrder(
            stringPair("a", "a"),
            stringPair("a", "b"),
            stringPair("b", "a"),
            stringPair("b", "c"),
            stringPair("c", "d"),
            stringPair("d", "a")
        );
    }

    @Test
    void exportGraphPropertiesMultithreaded() throws IOException {
        var config = ImmutableGraphStoreToFileExporterConfig
            .builder()
            .exportName(tempDir.toString())
            .writeConcurrency(4)
            .includeMetaData(false)
            .build();

        var graphPropertyValues = new LongGraphPropertyValues() {

            @Override
            public long valueCount() {
                return 1_000_000;
            }

            @Override
            public LongStream longValues() {
                return LongStream.range(0, valueCount());
            }
        };

        graphStore.addGraphProperty("graphProp", graphPropertyValues);

        var exporter = GraphStoreToCsvExporter.create(graphStore, config, tempDir);
        exporter.run();

        assertCsvFiles(
            LongStream
                .range(0, config.writeConcurrency())
                .mapToObj(i -> formatWithLocale(
                    CsvGraphPropertyVisitor.GRAPH_PROPERTY_DATA_FILE_NAME_TEMPLATE,
                    "graphProp",
                    i
                ))
                .collect(Collectors.toList())
        );

        var dataFiles = Files
            .list(tempDir)
            .filter(path -> path.getFileName().toString().startsWith("graph_property_graphProp"))
            .filter(not(path -> path.getFileName().toString().endsWith("header.csv")))
            .collect(Collectors.toList());

        assertThat(dataFiles).hasSize(4);

        var exportedValues = dataFiles
            .stream()
            .flatMap(path -> {
                try {
                    return Files.readAllLines(path).stream();
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            })
            .filter(not(String::isEmpty))
            .mapToLong(Long::valueOf)
            .sorted()
            .toArray();

        assertArrayEquals(LongStream.range(0, graphPropertyValues.valueCount()).toArray(), exportedValues);
    }

    @Test
    void exportSchemaAndDatabaseId() {
        var config = ImmutableGraphStoreToFileExporterConfig
            .builder()
            .exportName(tempDir.toString())
            .writeConcurrency(1)
            .includeMetaData(true)
            .build();

        graphStore.addGraphProperty("graphProp", new LongGraphPropertyValues() {
            @Override
            public LongStream longValues() {
                return LongStream.range(0, valueCount());
            }

            @Override
            public long valueCount() {
                return 3;
            }
        });

        var exporter = GraphStoreToCsvExporter.create(graphStore, config, tempDir);
        exporter.run();

        assertCsvFiles(List.of(NODE_SCHEMA_FILE_NAME, RELATIONSHIP_SCHEMA_FILE_NAME, GRAPH_PROPERTY_SCHEMA_FILE_NAME, GRAPH_INFO_FILE_NAME));

        assertDataContent(
            NODE_SCHEMA_FILE_NAME,
            List.of(
                NODE_SCHEMA_COLUMNS,
                List.of("A", "prop1", ValueType.LONG.csvName(), ValueType.LONG.fallbackValue().toString(), PropertyState.PERSISTENT.name()),
                List.of("A", "prop2", ValueType.LONG.csvName(), ValueType.LONG.fallbackValue().toString(), PropertyState.PERSISTENT.name()),
                List.of("A", "prop3", ValueType.LONG_ARRAY.csvName(), ValueType.LONG_ARRAY.fallbackValue().toString(), PropertyState.PERSISTENT.name()),
                List.of("B", "prop1", ValueType.LONG.csvName(), ValueType.LONG.fallbackValue().toString(), PropertyState.PERSISTENT.name()),
                List.of("B", "prop2", ValueType.LONG.csvName(), ValueType.LONG.fallbackValue().toString(), PropertyState.PERSISTENT.name()),
                List.of("B", "prop3", ValueType.LONG_ARRAY.csvName(), ValueType.LONG_ARRAY.fallbackValue().toString(), PropertyState.PERSISTENT.name()),
                List.of("C", "prop1", ValueType.LONG.csvName(), ValueType.LONG.fallbackValue().toString(), PropertyState.PERSISTENT.name()),
                List.of("C", "prop2", ValueType.LONG.csvName(), ValueType.LONG.fallbackValue().toString(), PropertyState.PERSISTENT.name()),
                List.of("C", "prop3", ValueType.LONG_ARRAY.csvName(), ValueType.LONG_ARRAY.fallbackValue().toString(), PropertyState.PERSISTENT.name())
            )
        );

        assertDataContent(
            RELATIONSHIP_SCHEMA_FILE_NAME,
            List.of(
                CsvRelationshipSchemaVisitorTest.RELATIONSHIP_SCHEMA_COLUMNS,
                List.of("REL1", "UNDIRECTED", "averylongnamethatisgreaterthantwentyfour", ValueType.DOUBLE.csvName(), ValueType.DOUBLE.fallbackValue().toString(), Aggregation.NONE.name(), PropertyState.PERSISTENT.name()),
                List.of("REL1", "UNDIRECTED", "prop2", ValueType.DOUBLE.csvName(), ValueType.DOUBLE.fallbackValue().toString(), Aggregation.NONE.name(), PropertyState.PERSISTENT.name()),
                List.of("REL2", "UNDIRECTED", "prop3", ValueType.DOUBLE.csvName(), ValueType.DOUBLE.fallbackValue().toString(), Aggregation.NONE.name(), PropertyState.PERSISTENT.name()),
                List.of("REL2", "UNDIRECTED", "prop4", ValueType.DOUBLE.csvName(), ValueType.DOUBLE.fallbackValue().toString(), Aggregation.NONE.name(), PropertyState.PERSISTENT.name())
            )
        );

        assertDataContent(
            GRAPH_PROPERTY_SCHEMA_FILE_NAME,
            List.of(
                GRAPH_PROPERTY_SCHEMA_COLUMNS,
                List.of("graphProp", ValueType.LONG.csvName(), ValueType.LONG.fallbackValue().toString(), PropertyState.PERSISTENT.name())
            )
        );

        assertDataContent(
            GRAPH_INFO_FILE_NAME,
            List.of(
                List.of(
                    CsvGraphInfoVisitor.DATABASE_NAME_COLUMN_NAME,
                    CsvGraphInfoVisitor.NODE_COUNT_COLUMN_NAME,
                    CsvGraphInfoVisitor.MAX_ORIGINAL_ID_COLUMN_NAME,
                    CsvGraphInfoVisitor.REL_TYPE_COUNTS_COLUMN_NAME,
                    CsvGraphInfoVisitor.INVERSE_INDEXED_REL_TYPES
                ),
                List.of(
                    graphStore.databaseId().databaseName(),
                    Long.toString(graphStore.nodeCount()),
                    Long.toString(graphStore.nodes().highestOriginalId()),
                    CsvMapUtil.relationshipCountsToString(Map.of(RelationshipType.of("REL2"), 6L, RelationshipType.of("REL1"), 6L)),
                    ""
                )
            )
        );
    }

    @Test
    void exportUsername() {
        var config = ImmutableGraphStoreToFileExporterConfig
            .builder()
            .exportName(tempDir.toString())
            .username("UserA")
            .writeConcurrency(1)
            .includeMetaData(true)
            .build();

        var exporter = GraphStoreToCsvExporter.create(graphStore, config, tempDir);
        exporter.run();

        assertThat(tempDir)
            .isDirectoryContaining("glob:**.userinfo");

        assertThat(tempDir.resolve(".userinfo"))
            .hasContent("UserA");
    }

    @Test
    void exportSchemaWithoutProperties() {
        var config = ImmutableGraphStoreToFileExporterConfig
            .builder()
            .exportName(tempDir.toString())
            .writeConcurrency(1)
            .includeMetaData(true)
            .build();

        var exporter = GraphStoreToCsvExporter.create(noPropertiesGraphStore, config, tempDir);
        exporter.run();

        assertCsvFiles(List.of(NODE_SCHEMA_FILE_NAME, RELATIONSHIP_SCHEMA_FILE_NAME, GRAPH_INFO_FILE_NAME));

        assertDataContent(
            NODE_SCHEMA_FILE_NAME,
            List.of(
                NODE_SCHEMA_COLUMNS,
                List.of("A"),
                List.of("B"),
                List.of("C")
            )
        );

        assertDataContent(
            RELATIONSHIP_SCHEMA_FILE_NAME,
            List.of(
                CsvRelationshipSchemaVisitorTest.RELATIONSHIP_SCHEMA_COLUMNS,
                List.of("REL1", "DIRECTED"),
                List.of("REL2", "DIRECTED"),
                List.of("REL3", "DIRECTED"),
                List.of("REL4", "DIRECTED")
            )
        );
    }

    @Test
    void shouldExportWithOffsetIds() {
        var counter = new MutableLong(42L);
        var graphStore = GdlFactory.builder()
            .gdlGraph("CREATE (a)-[:REL]->(b)")
            .nodeIdFunction(counter::getAndIncrement)
            .build()
            .build();

        var config = ImmutableGraphStoreToFileExporterConfig
            .builder()
            .exportName(tempDir.toString())
            .writeConcurrency(1)
            .includeMetaData(false)
            .build();

        // export db
        var exporter = GraphStoreToCsvExporter.create(graphStore, config, tempDir);
        exporter.run();

        assertCsvFiles(List.of(
            "nodes_0.csv", "nodes_header.csv",
            "relationships_REL_0.csv", "relationships_REL_header.csv"
        ));

        assertDataContent(
            "nodes_0.csv",
            List.of(
                List.of("42"),
                List.of("43")
            )
        );

        assertDataContent(
            "relationships_REL_0.csv",
            List.of(
                List.of("42", "43")
            )
        );
    }

    @Test
    void shouldExportGraphCapabilities() {
        var config = ImmutableGraphStoreToFileExporterConfig
            .builder()
            .exportName(tempDir.toString())
            .writeConcurrency(1)
            .includeMetaData(true)
            .build();

        var exporter = GraphStoreToCsvExporter.create(graphStore, config, tempDir);
        exporter.run();

        assertCsvFiles(List.of(GRAPH_CAPABILITIES_FILE_NAME));

        assertDataContent(
            GRAPH_CAPABILITIES_FILE_NAME,
            List.of(
                List.of("writeMode"),
                List.of(WriteMode.LOCAL.name())
            )
        );
    }

    private String stringIdOf(String variable) {
        return Long.toString(idFunction.of(variable));
    }

    private String stringPair(String sourceNode, String endNode) {
        return formatWithLocale("%s,%s", idFunction.of(sourceNode), idFunction.of(endNode));
    }
}

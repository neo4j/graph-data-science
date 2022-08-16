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

import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import org.apache.commons.lang3.tuple.Pair;
import org.jetbrains.annotations.NotNull;
import org.neo4j.gds.ElementIdentifier;
import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.api.schema.ElementSchema;
import org.neo4j.gds.api.nodeproperties.ValueType;
import org.neo4j.gds.api.schema.NodeSchema;
import org.neo4j.gds.api.schema.PropertySchema;
import org.neo4j.gds.api.schema.RelationshipPropertySchema;
import org.neo4j.gds.api.schema.RelationshipSchema;
import org.neo4j.gds.compat.CompatPropertySizeCalculator;
import org.neo4j.gds.core.io.file.FileHeader;
import org.neo4j.gds.core.io.file.FileInput;
import org.neo4j.gds.core.io.file.GraphInfo;
import org.neo4j.gds.core.io.file.HeaderProperty;
import org.neo4j.gds.core.io.file.MappedListIterator;
import org.neo4j.gds.core.io.file.NodeFileHeader;
import org.neo4j.gds.core.io.file.RelationshipFileHeader;
import org.neo4j.gds.core.loading.Capabilities;
import org.neo4j.internal.batchimport.InputIterable;
import org.neo4j.internal.batchimport.InputIterator;
import org.neo4j.internal.batchimport.input.Collector;
import org.neo4j.internal.batchimport.input.Groups;
import org.neo4j.internal.batchimport.input.IdType;
import org.neo4j.internal.batchimport.input.Input;
import org.neo4j.internal.batchimport.input.InputChunk;
import org.neo4j.internal.batchimport.input.InputEntityVisitor;
import org.neo4j.internal.batchimport.input.ReadableGroups;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

final class CsvFileInput implements FileInput {

    private static final char COLUMN_SEPARATOR = ',';
    private static final String ARRAY_ELEMENT_SEPARATOR = ";";
    private static final CsvMapper CSV_MAPPER = new CsvMapper();
    private static final ObjectReader LINE_READER = CSV_MAPPER
        .readerFor(String[].class)
        .with(CsvSchema
            .emptySchema()
            .withColumnSeparator(COLUMN_SEPARATOR)
        );
    private static final ObjectReader ARRAY_READER = CSV_MAPPER
        .readerForArrayOf(String.class)
        .with(CsvSchema
            .emptySchema()
            .withArrayElementSeparator(ARRAY_ELEMENT_SEPARATOR)
        );

    private final Path importPath;
    private final String userName;
    private final GraphInfo graphInfo;
    private final NodeSchema nodeSchema;
    private final RelationshipSchema relationshipSchema;
    private final Capabilities capabilities;

    CsvFileInput(Path importPath) {
        this.importPath = importPath;
        this.userName = new UserInfoLoader(importPath).load();
        this.graphInfo = new GraphInfoLoader(importPath, CSV_MAPPER).load();
        this.nodeSchema = new NodeSchemaLoader(importPath).load();
        this.relationshipSchema = new RelationshipSchemaLoader(importPath).load();
        this.capabilities = new GraphCapabilitiesLoader(importPath, CSV_MAPPER).load();
    }

    @Override
    public InputIterable nodes(Collector badCollector) {
        Map<Path, List<Path>> pathMapping = CsvImportFileUtil.nodeHeaderToFileMapping(importPath);
        Map<NodeFileHeader, List<Path>> headerToDataFilesMapping = pathMapping.entrySet().stream().collect(Collectors.toMap(
            entry -> CsvImportFileUtil.parseNodeHeader(entry.getKey()),
            Map.Entry::getValue
        ));

        return () -> new NodeImporter(headerToDataFilesMapping, nodeSchema);
    }

    @Override
    public InputIterable relationships(Collector badCollector) {
        Map<Path, List<Path>> pathMapping = CsvImportFileUtil.relationshipHeaderToFileMapping(importPath);
        Map<RelationshipFileHeader, List<Path>> headerToDataFilesMapping = pathMapping.entrySet().stream().collect(Collectors.toMap(
            entry -> CsvImportFileUtil.parseRelationshipHeader(entry.getKey()),
            Map.Entry::getValue
        ));

        return () -> new RelationshipImporter(headerToDataFilesMapping, relationshipSchema);
    }

    @Override
    public IdType idType() {
        return IdType.ACTUAL;
    }

    @Override
    public ReadableGroups groups() {
        return Groups.EMPTY;
    }

    @Override
    public Input.Estimates calculateEstimates(CompatPropertySizeCalculator propertySizeCalculator) {
        return null;
    }

    @Override
    public String userName() {
        return userName;
    }

    @Override
    public GraphInfo graphInfo() {
        return graphInfo;
    }

    @Override
    public NodeSchema nodeSchema() {
        return nodeSchema;
    }

    @Override
    public RelationshipSchema relationshipSchema() {
        return relationshipSchema;
    }

    public Capabilities capabilities() {
        return capabilities;
    }

    abstract static class FileImporter<
        HEADER extends FileHeader<SCHEMA, PROPERTY_SCHEMA>,
        SCHEMA extends ElementSchema<SCHEMA, IDENTIFIER, PROPERTY_SCHEMA>,
        IDENTIFIER extends ElementIdentifier,
        PROPERTY_SCHEMA extends PropertySchema> implements InputIterator {

        private final MappedListIterator<HEADER, Path> entryIterator;
        final SCHEMA elementSchema;

        FileImporter(
            Map<HEADER, List<Path>> headerToDataFilesMapping,
            SCHEMA elementSchema
        ) {
            this.entryIterator = new MappedListIterator<>(headerToDataFilesMapping);
            this.elementSchema = elementSchema;
        }

        @Override
        public synchronized boolean next(InputChunk chunk) throws IOException {
            if (entryIterator.hasNext()) {
                Pair<HEADER, Path> entry = entryIterator.next();

                assert chunk instanceof LineChunk;
                var header = entry.getKey();
                ((LineChunk<HEADER, SCHEMA, IDENTIFIER, PROPERTY_SCHEMA>) chunk).initialize(header, entry.getValue());
                return true;
            }
            return false;
        }

        @Override
        public void close() {
        }
    }

    static class NodeImporter extends FileImporter<NodeFileHeader, NodeSchema, NodeLabel, PropertySchema> {

        NodeImporter(
            Map<NodeFileHeader, List<Path>> headerToDataFilesMapping,
            NodeSchema nodeSchema
        ) {
            super(headerToDataFilesMapping, nodeSchema);
        }

        @Override
        public InputChunk newChunk() {
            return new NodeLineChunk(elementSchema);
        }
    }

    static class RelationshipImporter extends FileImporter<RelationshipFileHeader, RelationshipSchema, RelationshipType, RelationshipPropertySchema> {

        RelationshipImporter(
            Map<RelationshipFileHeader, List<Path>> headerToDataFilesMapping,
            RelationshipSchema relationshipSchema
        ) {
            super(headerToDataFilesMapping, relationshipSchema);
        }

        @Override
        public InputChunk newChunk() {
            return new RelationshipLineChunk(elementSchema);
        }
    }

    abstract static class LineChunk<
        HEADER extends FileHeader<SCHEMA, PROPERTY_SCHEMA>,
        SCHEMA extends ElementSchema<SCHEMA, IDENTIFIER, PROPERTY_SCHEMA>,
        IDENTIFIER extends ElementIdentifier,
        PROPERTY_SCHEMA extends PropertySchema> implements InputChunk {

        private final SCHEMA schema;

        HEADER header;
        Stream<String> lineStream;
        Iterator<String> lineIterator;
        Map<String, PROPERTY_SCHEMA> propertySchemas;

        LineChunk(SCHEMA schema) {
            this.schema = schema;
        }

        void initialize(HEADER header, Path path) throws IOException {
            this.header = header;
            this.propertySchemas = header.schemaForIdentifier(schema);
            this.lineStream = Files.lines(path);
            this.lineIterator = this.lineStream.iterator();
        }

        @Override
        public boolean next(InputEntityVisitor visitor) throws IOException {
            if (lineIterator.hasNext()) {
                String line = lineIterator.next();
                // Ignore empty lines
                if (!line.isBlank()) {
                    visitLine(line, header, visitor);
                }
                return true;
            }
            return false;
        }

        abstract void visitLine(String line, HEADER header, InputEntityVisitor visitor) throws IOException;

        @Override
        public void close() throws IOException {
            if (this.lineStream != null) {
                this.lineStream.close();
            }
        }
    }

    static class NodeLineChunk extends LineChunk<NodeFileHeader, NodeSchema, NodeLabel, PropertySchema> {

        NodeLineChunk(NodeSchema nodeSchema) {
            super(nodeSchema);
        }

        @Override
        void visitLine(String line, NodeFileHeader header, InputEntityVisitor visitor) throws IOException {
            var parsedLine = (String[]) LINE_READER.readValue(line);

            visitor.labels(header.nodeLabels());
            visitor.id((Long) CsvImportParsingUtil.parse(parsedLine[0], ValueType.LONG, ValueType.LONG.fallbackValue(), ARRAY_READER));

            visitProperties(header, propertySchemas, visitor, parsedLine);

            visitor.endOfEntity();
        }

        @Override
        public void close() throws IOException {

        }
    }

    static class RelationshipLineChunk extends LineChunk<RelationshipFileHeader, RelationshipSchema, RelationshipType, RelationshipPropertySchema> {

        RelationshipLineChunk(RelationshipSchema relationshipSchema) {
            super(relationshipSchema);
        }

        @Override
        void visitLine(String line, RelationshipFileHeader header, InputEntityVisitor visitor) throws IOException {
            var parsedLine = (String[]) LINE_READER.readValue(line);

            visitor.type(header.relationshipType());
            visitor.startId((Long) CsvImportParsingUtil.parse(parsedLine[0], ValueType.LONG, ValueType.LONG.fallbackValue(), ARRAY_READER));
            visitor.endId((Long) CsvImportParsingUtil.parse(parsedLine[1], ValueType.LONG, ValueType.LONG.fallbackValue(), ARRAY_READER));

            visitProperties(header, propertySchemas, visitor, parsedLine);

            visitor.endOfEntity();
        }

        @Override
        public void close() throws IOException {

        }
    }

    private static <PROPERTY_SCHEMA extends PropertySchema> void visitProperties(
        FileHeader<?, PROPERTY_SCHEMA> header,
        Map<String, PROPERTY_SCHEMA> propertySchemas,
        InputEntityVisitor visitor,
        String[] parsedLine
    ) throws IOException {
        for (HeaderProperty headerProperty : header.propertyMappings()) {
            var stringProperty = parsedLine[headerProperty.position()];
            var propertyKey = headerProperty.propertyKey();
            var defaultValue = propertySchemas.get(propertyKey).defaultValue();
            var value = CsvImportParsingUtil.parse(stringProperty, headerProperty.valueType(), defaultValue, ARRAY_READER);
            visitor.property(propertyKey, value);
        }
    }
}

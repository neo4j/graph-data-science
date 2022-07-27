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

import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import org.apache.commons.lang3.tuple.Pair;
import org.neo4j.gds.api.schema.NodeSchema;
import org.neo4j.gds.api.schema.PropertySchema;
import org.neo4j.gds.api.schema.RelationshipPropertySchema;
import org.neo4j.gds.api.schema.RelationshipSchema;
import org.neo4j.gds.compat.CompatPropertySizeCalculator;
import org.neo4j.gds.core.io.file.FileHeader;
import org.neo4j.gds.core.io.file.FileInput;
import org.neo4j.gds.core.io.file.GraphInfo;
import org.neo4j.gds.core.io.file.GraphPropertyFileHeader;
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
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public final class CsvFileInput implements FileInput {

    private static final char COLUMN_SEPARATOR = ',';
    private static final String ARRAY_ELEMENT_SEPARATOR = ";";
    private static final CsvMapper CSV_MAPPER = new CsvMapper();

    private final Path importPath;
    private final String userName;
    private final GraphInfo graphInfo;
    private final NodeSchema nodeSchema;
    private final RelationshipSchema relationshipSchema;
    private final Map<String, PropertySchema> graphPropertySchema;
    private final Capabilities capabilities;

    CsvFileInput(Path importPath) {
        this.importPath = importPath;
        this.userName = new UserInfoLoader(importPath).load();
        this.graphInfo = new GraphInfoLoader(importPath, CSV_MAPPER).load();
        this.nodeSchema = new NodeSchemaLoader(importPath).load();
        this.relationshipSchema = new RelationshipSchemaLoader(importPath).load();
        this.graphPropertySchema = new GraphPropertySchemaLoader(importPath).load();
        this.capabilities = new GraphCapabilitiesLoader(importPath, CSV_MAPPER).load();

        setupCsvMapper();
    }

    private static void setupCsvMapper() {
        var module = new SimpleModule()
            .addDeserializer(NodeDTO.class, new NodeDeserializer())
            .addDeserializer(RelationshipDTO.class, new RelationshipDeserializer());
        CSV_MAPPER.registerModule(module);
    }

    @Override
    public InputIterable nodes(Collector badCollector) {
        Map<Path, List<Path>> pathMapping = CsvImportUtil.nodeHeaderToFileMapping(importPath);
        Map<NodeFileHeader, List<Path>> headerToDataFilesMapping = pathMapping.entrySet().stream().collect(Collectors.toMap(
            entry -> CsvImportUtil.parseNodeHeader(entry.getKey()),
            Map.Entry::getValue
        ));

        var propertySchemas = nodeSchema.unionProperties();
        CSV_MAPPER.setInjectableValues(new InjectableValues.Std().addValue(
            NodeDeserializer.NODE_SCHEMA_INJECTION_NAME,
            propertySchemas
        ));

        var headerToObjectReaderMapping = new HashMap<NodeFileHeader, ObjectReader>();
        headerToDataFilesMapping.keySet().forEach(header -> {
            var csvSchema = CsvSchemaUtil
                .fromElementSchema(propertySchemas, header, CsvNodeVisitor.ID_COLUMN_NAME)
                .withColumnSeparator(COLUMN_SEPARATOR)
                .withArrayElementSeparator(ARRAY_ELEMENT_SEPARATOR);

            var objectReader = CSV_MAPPER.readerFor(NodeDTO.class).with(csvSchema);
            headerToObjectReaderMapping.put(header, objectReader);
        });

        return () -> new NodeImporter(headerToDataFilesMapping, nodeSchema, headerToObjectReaderMapping);
    }

    @Override
    public InputIterable relationships(Collector badCollector) {
        Map<Path, List<Path>> pathMapping = CsvImportUtil.relationshipHeaderToFileMapping(importPath);
        Map<RelationshipFileHeader, List<Path>> headerToDataFilesMapping = pathMapping.entrySet().stream().collect(Collectors.toMap(
            entry -> CsvImportUtil.parseRelationshipHeader(entry.getKey()),
            Map.Entry::getValue
        ));

        var propertySchemas = relationshipSchema.unionProperties();

        CSV_MAPPER.setInjectableValues(new InjectableValues.Std().addValue(
            RelationshipDeserializer.RELATIONSHIP_SCHEMA_INJECTION_NAME,
            propertySchemas
        ));

        var headerToObjectReaderMapping = new HashMap<RelationshipFileHeader, ObjectReader>();
        headerToDataFilesMapping.keySet().forEach(header -> {
            var csvSchema = CsvSchemaUtil
                .fromElementSchema(propertySchemas, header, CsvRelationshipVisitor.START_ID_COLUMN_NAME, CsvRelationshipVisitor.END_ID_COLUMN_NAME)
                .withColumnSeparator(COLUMN_SEPARATOR)
                .withArrayElementSeparator(ARRAY_ELEMENT_SEPARATOR);

            var objectReader = CSV_MAPPER.readerFor(RelationshipDTO.class).with(csvSchema);

            headerToObjectReaderMapping.put(header, objectReader);
        });

        return () -> new RelationshipImporter(headerToDataFilesMapping, relationshipSchema, headerToObjectReaderMapping);
    }

    @Override
    public InputIterable graphProperties() {
        var pathMapping = CsvImportUtil.graphPropertyHeaderToFileMapping(importPath);
        var headerToDataFilesMapping = pathMapping.entrySet().stream().collect(Collectors.toMap(
            entry -> CsvImportUtil.parseGraphPropertyHeader(entry.getKey()),
            Map.Entry::getValue
        ));

        var headerToObjectReaderMapping = new HashMap<GraphPropertyFileHeader, ObjectReader>();
        headerToDataFilesMapping.keySet().forEach(header -> {
            var csvSchema = CsvSchema.builder()
                    .addColumn(header.propertyMapping().propertyKey(), CsvSchemaUtil.csvTypeFromValueType(header.propertyMapping().valueType()))
                    .build();

            var objectReader = CSV_MAPPER.reader().with(csvSchema);
            headerToObjectReaderMapping.put(header, objectReader);
        });
        return () -> new GraphPropertyImporter(headerToDataFilesMapping, graphPropertySchema, headerToObjectReaderMapping);
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

    @Override
    public Map<String, PropertySchema> graphPropertySchema() {
        return graphPropertySchema;
    }

    @Override
    public Capabilities capabilities() {
        return capabilities;
    }

    abstract static class FileImporter<
        HEADER extends FileHeader<SCHEMA, PROPERTY_SCHEMA>,
        SCHEMA,
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
                var objectReader = objectReaderForHeader(header);
                ((LineChunk<HEADER, SCHEMA, PROPERTY_SCHEMA>) chunk).initialize(header, entry.getValue(), objectReader);
                return true;
            }
            return false;
        }

        abstract ObjectReader objectReaderForHeader(HEADER header);

        @Override
        public void close() {
        }
    }

    static class NodeImporter extends FileImporter<NodeFileHeader, NodeSchema, PropertySchema> {

        private final Map<NodeFileHeader, ObjectReader> headerToObjectReaderMapping;

        NodeImporter(
            Map<NodeFileHeader, List<Path>> headerToDataFilesMapping,
            NodeSchema nodeSchema,
            Map<NodeFileHeader, ObjectReader> headerToObjectReaderMapping
        ) {
            super(headerToDataFilesMapping, nodeSchema);
            this.headerToObjectReaderMapping = headerToObjectReaderMapping;
        }

        @Override
        ObjectReader objectReaderForHeader(NodeFileHeader header) {
            return headerToObjectReaderMapping.get(header);
        }

        @Override
        public InputChunk newChunk() {
            return new NodeLineChunk(elementSchema);
        }
    }

    static class RelationshipImporter extends FileImporter<RelationshipFileHeader, RelationshipSchema, RelationshipPropertySchema> {

        private final Map<RelationshipFileHeader, ObjectReader> headerToObjectReaderMapping;

        RelationshipImporter(
            Map<RelationshipFileHeader, List<Path>> headerToDataFilesMapping,

            RelationshipSchema relationshipSchema,
            Map<RelationshipFileHeader, ObjectReader> headerToObjectReaderMapping
        ) {
            super(headerToDataFilesMapping, relationshipSchema);
            this.headerToObjectReaderMapping = headerToObjectReaderMapping;
        }

        @Override
        ObjectReader objectReaderForHeader(RelationshipFileHeader header) {
            return headerToObjectReaderMapping.get(header);
        }

        @Override
        public InputChunk newChunk() {
            return new RelationshipLineChunk(elementSchema);
        }
    }

    static class GraphPropertyImporter extends FileImporter<GraphPropertyFileHeader, Map<String, PropertySchema>, PropertySchema> {

        private final Map<GraphPropertyFileHeader, ObjectReader> headerToObjectReaderMapping;

        GraphPropertyImporter(
            Map<GraphPropertyFileHeader, List<Path>> headerToDataFilesMapping,
            Map<String, PropertySchema> graphPropertySchema,
            Map<GraphPropertyFileHeader, ObjectReader> headerToObjectReaderMapping
        ) {
            super(headerToDataFilesMapping, graphPropertySchema);
            this.headerToObjectReaderMapping = headerToObjectReaderMapping;
        }

        @Override
        ObjectReader objectReaderForHeader(GraphPropertyFileHeader header) {
            return headerToObjectReaderMapping.get(header);
        }

        @Override
        public InputChunk newChunk() {
            return new GraphPropertyLineChunk(elementSchema);
        }
    }

    abstract static class LineChunk<
        HEADER extends FileHeader<SCHEMA, PROPERTY_SCHEMA>,
        SCHEMA,
        PROPERTY_SCHEMA extends PropertySchema> implements InputChunk {

        private final SCHEMA schema;

        HEADER header;
        Stream<String> lineStream;
        Iterator<String> lineIterator;
        Map<String, PROPERTY_SCHEMA> propertySchemas;
        ObjectReader objectReader;

        LineChunk(SCHEMA schema) {
            this.schema = schema;
        }

        void initialize(
            HEADER header,
            Path path,
            ObjectReader objectReader
        ) throws IOException {
            this.header = header;
            this.propertySchemas = header.schemaForIdentifier(schema);
            this.lineStream = Files.lines(path);
            this.lineIterator = this.lineStream.iterator();
            this.objectReader = objectReader;
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

    static class NodeLineChunk extends LineChunk<NodeFileHeader, NodeSchema, PropertySchema> {

        NodeLineChunk(NodeSchema nodeSchema) {
            super(nodeSchema);
        }

        @Override
        void visitLine(String line, NodeFileHeader header, InputEntityVisitor visitor) throws IOException {
            NodeDTO node = objectReader.readValue(line);

            visitor.labels(header.nodeLabels());
            visitor.id(node.id);
            node.properties.forEach(visitor::property);

            visitor.endOfEntity();
        }

        @Override
        public void close() throws IOException {

        }
    }

    static class RelationshipLineChunk extends LineChunk<RelationshipFileHeader, RelationshipSchema, RelationshipPropertySchema> {

        public RelationshipLineChunk(RelationshipSchema relationshipSchema) {
            super(relationshipSchema);
        }

        @Override
        void visitLine(String line, RelationshipFileHeader header, InputEntityVisitor visitor) throws IOException {
            RelationshipDTO relationship = objectReader.readValue(line);

            visitor.type(header.relationshipType());
            visitor.startId(relationship.sourceId);
            visitor.endId(relationship.targetId);

            relationship.properties.forEach(visitor::property);

            visitor.endOfEntity();
        }

        @Override
        public void close() throws IOException {

        }
    }

    static class GraphPropertyLineChunk extends LineChunk<GraphPropertyFileHeader, Map<String, PropertySchema>, PropertySchema> {

        GraphPropertyLineChunk(Map<String, PropertySchema> stringPropertySchemaMap) {
            super(stringPropertySchemaMap);
        }

        @Override
        void visitLine(
            String line, GraphPropertyFileHeader header, InputEntityVisitor visitor
        ) throws IOException {
            var node = objectReader.readTree(line);
            var propertyMapping = header.propertyMapping();
            var propertyKey = propertyMapping.propertyKey();
            var defaultValue = propertySchemas.get(propertyKey).defaultValue();
            visitor.property(propertyKey, propertyMapping.valueType().fromCsvValue(defaultValue, node.get(propertyKey)));
            visitor.endOfEntity();
        }
    }
}

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
package org.neo4j.graphalgo.core.utils.export.file;

import org.apache.commons.lang3.tuple.Pair;
import org.neo4j.graphalgo.api.schema.NodeSchema;
import org.neo4j.graphalgo.compat.CompatInput;
import org.neo4j.graphalgo.compat.CompatPropertySizeCalculator;
import org.neo4j.graphalgo.core.utils.export.file.csv.CsvImportUtil;
import org.neo4j.internal.batchimport.InputIterable;
import org.neo4j.internal.batchimport.InputIterator;
import org.neo4j.internal.batchimport.input.Collector;
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

public final class FileInput implements CompatInput {

    private final Path importPath;
    private final GraphInfo graphInfo;
    private final NodeSchema nodeSchema;

    FileInput(Path importPath) {
        this.importPath = importPath;
        this.graphInfo = new GraphInfoLoader(importPath).load();
        this.nodeSchema = new NodeSchemaLoader(importPath).load();
    }

    @Override
    public InputIterable nodes(Collector badCollector) {
        Map<Path, List<Path>> pathMapping = CsvImportUtil.headerToFileMapping(importPath);
        Map<NodeFileHeader, List<Path>> headerToDataFilesMapping = pathMapping.entrySet().stream().collect(Collectors.toMap(
            entry -> CsvImportUtil.parseHeader(entry.getKey()),
            Map.Entry::getValue
        ));
        return () -> new NodeImporter(headerToDataFilesMapping);
    }

    @Override
    public InputIterable relationships(Collector badCollector) {
        return null;
    }

    @Override
    public IdType idType() {
        return null;
    }

    @Override
    public ReadableGroups groups() {
        return null;
    }

    @Override
    public Input.Estimates calculateEstimates(CompatPropertySizeCalculator propertySizeCalculator) throws IOException {
        return null;
    }

    public GraphInfo graphInfo() {
        return graphInfo;
    }

    public NodeSchema nodeSchema() {
        return nodeSchema;
    }

    abstract static class FileImporter implements InputIterator {

        private final MappedListIterator<NodeFileHeader, Path> entryIterator;

        FileImporter(Map<NodeFileHeader, List<Path>> headerToDataFilesMapping) {
            this.entryIterator = new MappedListIterator<>(headerToDataFilesMapping);
        }

        @Override
        public synchronized boolean next(InputChunk chunk) throws IOException {
            if (entryIterator.hasNext()) {
                Pair<NodeFileHeader, Path> entry = entryIterator.next();
                ((LineChunk) chunk).initialize(entry.getKey(), entry.getValue());
                return true;
            }
            return false;
        }
    }

    static class NodeImporter extends FileImporter {

        NodeImporter(Map<NodeFileHeader, List<Path>> headerToDataFilesMapping) {
            super(headerToDataFilesMapping);
        }

        @Override
        public InputChunk newChunk() {
            return new NodeLineChunk();
        }

        @Override
        public void close() throws IOException {

        }
    }

    abstract static class LineChunk implements InputChunk {
        NodeFileHeader header;
        Iterator<String> lineIterator;

        void initialize(NodeFileHeader header, Path path) throws IOException {
            this.header = header;
            this.lineIterator = Files.lines(path).iterator();
        }

        @Override
        public boolean next(InputEntityVisitor visitor) throws IOException {
            if (lineIterator.hasNext()) {
                String line = lineIterator.next();
                visitLine(line, header, visitor);
                return true;
            }
            return false;
        }

        abstract void visitLine(String line, NodeFileHeader header, InputEntityVisitor visitor) throws IOException;
    }

    static class NodeLineChunk extends LineChunk {

        @Override
        void visitLine(String line, NodeFileHeader header, InputEntityVisitor visitor) throws IOException {
            var lineValues = line.split(",");

            visitor.labels(header.nodeLabels());
            visitor.id(Long.parseLong(lineValues[0]));

            header
                .propertyMappings()
                .forEach(property -> visitor.property(property.propertyKey(), lineValues[property.position()]));

            visitor.endOfEntity();
        }

        @Override
        public void close() throws IOException {

        }
    }

    static class RelationshipLineChunk extends LineChunk {

        RelationshipLineChunk() {
        }

        @Override
        void visitLine(String line, NodeFileHeader header, InputEntityVisitor visitor) {

        }

        @Override
        public void close() throws IOException {

        }
    }
}

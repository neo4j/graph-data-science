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
package org.neo4j.graphalgo.core.utils.export;

import de.siegmar.fastcsv.writer.CsvAppender;
import de.siegmar.fastcsv.writer.CsvWriter;
import org.neo4j.graphalgo.NodeLabel;
import org.neo4j.graphalgo.api.GraphStore;
import org.neo4j.internal.batchimport.input.Collector;
import org.neo4j.internal.batchimport.input.InputEntityVisitor;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.neo4j.graphalgo.utils.StringFormatting.formatWithLocale;

public class CsvExporter {
    private final GraphStoreInput graphStoreInput;
    private final Path fileLocation;
    private final GraphStore graphStore;

    public CsvExporter(GraphStoreInput graphStoreInput, Path fileLocation, GraphStore graphStore) {
        this.graphStoreInput = graphStoreInput;
        this.fileLocation = fileLocation;
        this.graphStore = graphStore;
    }

    void doExport() {
        exportNodes();
        exportRelationships();
    }


    void exportNodes() {
        var nodeInput = graphStoreInput.nodes(Collector.EMPTY);
        var nodeInputIterator = nodeInput.iterator();
        var nodeVisitor = new NodeVisitor(fileLocation, graphStore);

        try (var chunk = nodeInputIterator.newChunk()) {
            while (nodeInputIterator.next(chunk)) {
                while(chunk.next(nodeVisitor)) {

                }
            }
        } catch ( IOException e ) {
            throw new RuntimeException( e );
        }

        nodeVisitor.close();
    }

    void exportRelationships() {}


    private static class NodeVisitor extends InputEntityVisitor.Adapter {

        private long currentId;
        private String[] currentLabels;
        private final Path fileLocation;
        private final GraphStore graphStore;
        private final Object[] currentProperties;
        private final Map<String, CsvAppender> csvAppenders;
        private final CsvWriter csvWriter;
        private final Map<String, List<String>> propertyKeys;
        private final Map<String, Integer> propertyKeyPositions;

        NodeVisitor(Path fileLocation, GraphStore graphStore) {
            this.fileLocation = fileLocation;
            this.graphStore = graphStore;
            this.propertyKeys = new HashMap<>();
            this.propertyKeyPositions = new HashMap<>();
            var allProperties = graphStore
                .schema()
                .nodeSchema()
                .allProperties();
            var i = 0;
            for (String propertyKey : allProperties) {
                propertyKeyPositions.put(propertyKey, i++);
            }

            this.currentProperties = new Object[propertyKeyPositions.size()];
            this.csvAppenders = new HashMap<>();
            this.csvWriter = new CsvWriter();
        }

        @Override
        public boolean id(long id) {
            currentId = id;
            return true;
        }

        @Override
        public boolean labels(String[] labels) {
            Arrays.sort(labels);
            currentLabels = labels;
            return true;
        }

        @Override
        public boolean property(String key, Object value) {
            var propertyPosition = propertyKeyPositions.get(key);
            currentProperties[propertyPosition] = value;
            return true;
        }

        @Override
        public void endOfEntity() {
            // do the import
            var csvAppender = getAppender();
            try {
                // write Id
                csvAppender.appendField(Long.toString(currentId));
                // write properties
                for (String propertyKey : propertyKeys.get(labelString())) {
                    var propertyPosition = propertyKeyPositions.get(propertyKey);
                    var propertyValue = currentProperties[propertyPosition];
                    csvAppender.appendField(propertyValue == null ? "" : propertyValue.toString());
                }
                csvAppender.endLine();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

            // reset
            Arrays.fill(currentProperties, null);
        }

        @Override
        public void close() {
            csvAppenders.values().forEach(csvAppender -> {
                try {
                    csvAppender.flush();
                    csvAppender.close();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });
        }

        private CsvAppender getAppender() {
            var labelsString = labelString();

            return csvAppenders.computeIfAbsent(labelsString, (ignore) -> {
                var fileName = formatWithLocale("nodes_%s.csv", labelsString);

                // Calculate the property schema for this label combination
                var nodeLabelList = Arrays.stream(currentLabels).map(NodeLabel::of).collect(Collectors.toList());
                var properties = graphStore.nodePropertyKeys(nodeLabelList);
                propertyKeys.put(labelsString, properties.stream().sorted().collect(Collectors.toList()));

                try {
                    return csvWriter.append(fileLocation.resolve(fileName), StandardCharsets.UTF_8);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });
        }

        private String labelString() {
            return String.join("_", currentLabels);
        }
    }
}

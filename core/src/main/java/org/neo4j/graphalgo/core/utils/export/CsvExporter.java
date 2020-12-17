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
import org.neo4j.graphalgo.api.nodeproperties.ValueType;
import org.neo4j.graphalgo.api.schema.PropertySchema;
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
import java.util.stream.IntStream;

import static org.neo4j.graphalgo.api.DefaultValue.INTEGER_DEFAULT_FALLBACK;
import static org.neo4j.graphalgo.api.DefaultValue.LONG_DEFAULT_FALLBACK;
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
                while (chunk.next(nodeVisitor)) {

                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
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
        private final Map<String, List<Map.Entry<String, PropertySchema>>> propertyKeys;
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
                for (Map.Entry<String, PropertySchema> propertyEntry : propertyKeys.get(labelString())) {
                    var propertyPosition = propertyKeyPositions.get(propertyEntry.getKey());
                    var propertyValue = currentProperties[propertyPosition];
                    var propertyString = formatValue(propertyValue, propertyEntry.getValue().valueType());
                    csvAppender.appendField(propertyString);
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
                var fileName = formatWithLocale("nodes_%s", labelsString);
                var headerFileName = formatWithLocale("%s_header.csv", fileName);
                var dataFileName = formatWithLocale("%s.csv", fileName);

                calculateLabelSchema(labelsString);
                writeHeaderFile(labelsString, headerFileName);

                try {
                    return csvWriter.append(fileLocation.resolve(dataFileName), StandardCharsets.UTF_8);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });
        }

        private String labelString() {
            return String.join("_", currentLabels);
        }

        private void calculateLabelSchema(String labelsString) {
            var nodeLabelList = Arrays.stream(currentLabels).map(NodeLabel::of).collect(Collectors.toSet());
            var propertySchemaForLabels = graphStore.schema().nodeSchema().filter(nodeLabelList);
            var unionProperties = propertySchemaForLabels.unionProperties();
            var sortedPropertyEntries = unionProperties
                .entrySet()
                .stream()
                .sorted(Map.Entry.comparingByKey())
                .collect(Collectors.toList());
            propertyKeys.put(labelsString, sortedPropertyEntries);
        }

        private void writeHeaderFile(String labelString, String headerFileName) {
            try (var headerAppender = csvWriter.append(fileLocation.resolve(headerFileName), StandardCharsets.UTF_8)) {
                headerAppender.appendField("ID");

                var propertyEntries = propertyKeys.get(labelString);
                for (Map.Entry<String, PropertySchema> propertyEntry : propertyEntries) {
                    var propertyHeader = formatWithLocale(
                        "%s:%s",
                        propertyEntry.getKey(),
                        propertyEntry.getValue().valueType().cypherName()
                    );
                    headerAppender.appendField(propertyHeader);
                }
                headerAppender.endLine();
            } catch (IOException e) {
                throw new RuntimeException("Could not write header file", e);
            }
        }

        private String formatValue(Object value, ValueType valueType) {
            if (value == null) {
                return "";
            }
            switch (valueType) {
                case LONG:
                    var longValue = (long) value;
                    if (longValue == LONG_DEFAULT_FALLBACK || longValue == INTEGER_DEFAULT_FALLBACK) {
                        return "";
                    }
                    return Long.toString(longValue);

                case DOUBLE:
                    var doubleValue = (double) value;
                    if (Double.isNaN(doubleValue) || Float.isNaN((float) doubleValue)) {
                        return "";
                    }
                    return Double.toString(doubleValue);

                case DOUBLE_ARRAY:
                    var doubleArray = (double[]) value;
                    return Arrays.stream(doubleArray).mapToObj(Double::toString).collect(Collectors.joining(";"));

                case FLOAT_ARRAY:
                    var floatArray = (float[]) value;
                    return IntStream
                        .range(0, floatArray.length)
                        .mapToDouble(i -> floatArray[i])
                        .mapToObj(Double::toString)
                        .collect(Collectors.joining(";"));

                case LONG_ARRAY:
                    var longArray = (long[]) value;
                    return Arrays.stream(longArray).mapToObj(Long::toString).collect(Collectors.joining(";"));

                default:
                    return value.toString();
            }
        }
    }
}

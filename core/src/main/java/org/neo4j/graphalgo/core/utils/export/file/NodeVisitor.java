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
package org.neo4j.graphalgo.core.utils.export.file;

import org.neo4j.graphalgo.NodeLabel;
import org.neo4j.graphalgo.api.GraphStore;
import org.neo4j.graphalgo.api.nodeproperties.ValueType;
import org.neo4j.graphalgo.api.schema.PropertySchema;
import org.neo4j.internal.batchimport.input.InputEntityVisitor;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public abstract class NodeVisitor extends InputEntityVisitor.Adapter {

    private long currentId;
    private List<String> currentLabels;
    private final GraphStore graphStore;
    private final Object[] currentProperties;

    // TODO use String instead of List<String>?
    private final Map<List<String>, List<Map.Entry<String, PropertySchema>>> propertyKeys;

    private final Map<String, Integer> propertyKeyPositions;

    protected NodeVisitor(GraphStore graphStore) {
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
    }

    protected abstract void importNode();

    public long getId() {
        return currentId;
    }

    public List<String> labels() {
        return currentLabels;
    }

    public void forEachProperty(PropertyConsumer propertyConsumer) {
        for (Map.Entry<String, PropertySchema> propertyEntry : propertyKeys.get(currentLabels)) {
            var propertyPosition = propertyKeyPositions.get(propertyEntry.getKey());
            var propertyValue = currentProperties[propertyPosition];
            propertyConsumer.accept(propertyEntry.getKey(), propertyValue, propertyEntry.getValue().valueType());
        }
    }

    @Override
    public boolean id(long id) {
        currentId = id;
        return true;
    }

    @Override
    public boolean labels(String[] labels) {
        Arrays.sort(labels);
        currentLabels = Arrays.asList(labels);
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
        if (!propertyKeys.containsKey(currentLabels)) {
            calculateLabelSchema();
        }

        // do the import
        importNode();

        // reset
        Arrays.fill(currentProperties, null);
    }



    private void calculateLabelSchema() {
        var nodeLabelList = currentLabels.stream().map(NodeLabel::of).collect(Collectors.toSet());
        var propertySchemaForLabels = graphStore.schema().nodeSchema().filter(nodeLabelList);
        var unionProperties = propertySchemaForLabels.unionProperties();
        var sortedPropertyEntries = unionProperties
            .entrySet()
            .stream()
            .sorted(Map.Entry.comparingByKey())
            .collect(Collectors.toList());
        propertyKeys.put(currentLabels, sortedPropertyEntries);
    }

    @FunctionalInterface
    public interface PropertyConsumer {
        void accept(String key, Object value, ValueType type);
    }
}

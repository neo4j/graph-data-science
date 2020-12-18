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

import org.neo4j.graphalgo.RelationshipType;
import org.neo4j.graphalgo.api.schema.GraphSchema;
import org.neo4j.graphalgo.api.schema.RelationshipPropertySchema;
import org.neo4j.internal.batchimport.input.InputEntityVisitor;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public abstract class RelationshipVisitor extends InputEntityVisitor.Adapter {


    private long currentStartNode;
    private long currentEndNode;
    private String relationshipType;
    private final GraphSchema graphSchema;
    private final Object[] currentProperties;

    private final Map<String, List<Map.Entry<String, RelationshipPropertySchema>>> propertyKeys;
    private final Map<String, Integer> propertyKeyPositions;

    protected RelationshipVisitor(GraphSchema graphSchema) {
        currentStartNode = -1;
        relationshipType = RelationshipType.ALL_RELATIONSHIPS.name;

        this.graphSchema = graphSchema;
        this.propertyKeys = new HashMap<>();
        this.propertyKeyPositions = new HashMap<>();
        var allProperties = graphSchema
            .relationshipSchema()
            .allProperties();
        var i = 0;
        for (String propertyKey : allProperties) {
            propertyKeyPositions.put(propertyKey, i++);
        }

        this.currentProperties = new Object[propertyKeyPositions.size()];
    }

    protected abstract void importRelationship();

    public long startNode() {
        return currentStartNode;
    }

    public long endNode() {
        return currentEndNode;
    }

    public String relationshipType() {
        return relationshipType;
    }

    public void forEachProperty(PropertyConsumer propertyConsumer) {
        for (Map.Entry<String, RelationshipPropertySchema> propertyEntry : propertyKeys.get(relationshipType)) {
            var propertyPosition = propertyKeyPositions.get(propertyEntry.getKey());
            var propertyValue = currentProperties[propertyPosition];
            propertyConsumer.accept(propertyEntry.getKey(), propertyValue, propertyEntry.getValue().valueType());
        }
    }

    @Override
    public boolean startId(long id) {
        currentStartNode = id;
        return true;
    }

    @Override
    public boolean endId(long id) {
        currentEndNode = id;
        return true;
    }


    @Override
    public boolean type(String type) {
        relationshipType = type;
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
        // Check if we encounter a new label combination
        if (!propertyKeys.containsKey(relationshipType)) {
            calculateTypeSchema();
        }

        // do the import
        importRelationship();

        // reset
        currentStartNode = -1;
        currentEndNode = -1;
        relationshipType = RelationshipType.ALL_RELATIONSHIPS.name;
        Arrays.fill(currentProperties, null);
    }

    private void calculateTypeSchema() {
        var relationshipType = RelationshipType.of(this.relationshipType);
        var propertySchemaForLabels = graphSchema.relationshipSchema().filter(Set.of(relationshipType));
        var unionProperties = propertySchemaForLabels.unionProperties();
        var sortedPropertyEntries = unionProperties
            .entrySet()
            .stream()
            .sorted(Map.Entry.comparingByKey())
            .collect(Collectors.toList());
        propertyKeys.put(this.relationshipType, sortedPropertyEntries);
    }
}

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
package org.neo4j.gds;

import io.soabase.recordbuilder.core.RecordBuilder;
import org.jetbrains.annotations.Nullable;
import org.neo4j.gds.core.ConfigKeyValidation;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static java.util.Collections.emptyMap;
import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

@RecordBuilder.Options(
    useImmutableCollections = true,
    addSingleItemCollectionBuilders = true,
    prefixEnclosingClassNames = false,
    addStaticBuilder = false,
    addConcreteSettersForOptional = true,
    interpretNotNulls = true
)
@RecordBuilder
public record RelationshipProjection(
    String type,
    Orientation orientation,
    Aggregation aggregation,
    boolean indexInverse,
    PropertyMappings properties
) implements ElementProjection, RelationshipProjectionBuilder.With {

    public RelationshipProjection {
        if (orientation == Orientation.UNDIRECTED && indexInverse) {
            throw new IllegalArgumentException(
                "Relationship projection `" + type + "` cannot be UNDIRECTED and inverse indexed. " +
                    "Indexing the inverse orientation is only allowed for NATURAL and REVERSE."
            );
        }
    }

    public RelationshipProjection(String type) {
        this(type, DEFAULT_ORIENTATION, DEFAULT_AGGREGATION, DEFAULT_INDEX_INVERSE, DEFAULT_PROPERTIES);
    }

    public RelationshipProjection(String type, Aggregation aggregation) {
        this(type, DEFAULT_ORIENTATION, aggregation, DEFAULT_INDEX_INVERSE, DEFAULT_PROPERTIES);
    }

    public RelationshipProjection(String type, PropertyMappings properties) {
        this(type, DEFAULT_ORIENTATION, DEFAULT_AGGREGATION, DEFAULT_INDEX_INVERSE, properties);
    }

    public RelationshipProjection(String type, Orientation orientation) {
        this(type, orientation, DEFAULT_AGGREGATION, DEFAULT_INDEX_INVERSE, DEFAULT_PROPERTIES);
    }

    public RelationshipProjection(String type, boolean indexInverse) {
        this(type, DEFAULT_ORIENTATION, DEFAULT_AGGREGATION, indexInverse, DEFAULT_PROPERTIES);
    }

    public RelationshipProjection(String type, Orientation orientation, Aggregation aggregation) {
        this(type, orientation, aggregation, DEFAULT_INDEX_INVERSE, DEFAULT_PROPERTIES);
    }

    public RelationshipProjection(String type, Orientation orientation, boolean indexInverse) {
        this(type, orientation, DEFAULT_AGGREGATION, indexInverse, DEFAULT_PROPERTIES);
    }

    public RelationshipProjection(String type, Orientation orientation, PropertyMappings properties) {
        this(type, orientation, DEFAULT_AGGREGATION, DEFAULT_INDEX_INVERSE, properties);
    }

    public RelationshipProjection(String type, Aggregation aggregation, PropertyMappings properties) {
        this(type, DEFAULT_ORIENTATION, aggregation, DEFAULT_INDEX_INVERSE, properties);
    }

    public RelationshipProjection(String type, boolean indexInverse, PropertyMappings properties) {
        this(type, DEFAULT_ORIENTATION, DEFAULT_AGGREGATION, indexInverse, properties);
    }

    public RelationshipProjection(String type, Orientation orientation, boolean indexInverse, PropertyMappings properties) {
        this(type, orientation, DEFAULT_AGGREGATION, indexInverse, properties);
    }

    public RelationshipProjection(String type, Orientation orientation, Aggregation aggregation, boolean indexInverse) {
        this(type, orientation, aggregation, indexInverse, DEFAULT_PROPERTIES);
    }

    public RelationshipProjection(String type, Orientation orientation, Aggregation aggregation, PropertyMappings properties) {
        this(type, orientation, aggregation, DEFAULT_INDEX_INVERSE, properties);
    }

    public RelationshipProjection inverse() {
        return new RelationshipProjection(type, orientation.inverse(), aggregation, indexInverse, properties);
    }

    private static final Orientation DEFAULT_ORIENTATION = Orientation.NATURAL;
    private static final Aggregation DEFAULT_AGGREGATION = Aggregation.DEFAULT;
    private static final boolean DEFAULT_INDEX_INVERSE = false;
    private static final PropertyMappings DEFAULT_PROPERTIES = PropertyMappings.of();

    public static final RelationshipProjection ALL = new RelationshipProjection(PROJECT_ALL, Orientation.NATURAL);

    /**
     * Checks if the projection defines a global aggregation that requires at least one property mapping, such as `MIN`.
     * This check is independent of the annotated check method, since we normalize projections after they have been created.
     * (see org.neo4j.gds.config.GraphProjectFromStoreConfig#withNormalizedPropertyMappings()),
     */
    public void checkAggregation() {
        if (properties().isEmpty()) {
            switch (aggregation()) {
                case DEFAULT, NONE, SINGLE -> {}
                case COUNT, SUM, MIN, MAX -> throw new IllegalArgumentException("Setting a global `" + aggregation() + "` aggregation requires at least one property mapping.");
            }
        }
    }

    public static final String TYPE_KEY = "type";
    public static final String ORIENTATION_KEY = "orientation";
    public static final String AGGREGATION_KEY = "aggregation";
    public static final String INDEX_INVERSE_KEY = "indexInverse";

    public static RelationshipProjection fromMap(Map<String, Object> map, RelationshipType relationshipType) {
        validateConfigKeys(map);
        var type = String.valueOf(map.getOrDefault(TYPE_KEY, relationshipType.name));
        var orientation = map.containsKey(ORIENTATION_KEY)
            ? Orientation.parse(nonEmptyString(map, ORIENTATION_KEY))
            : DEFAULT_ORIENTATION;
        var aggregation = map.containsKey(AGGREGATION_KEY)
            ? Aggregation.parse(nonEmptyString(map, AGGREGATION_KEY))
            : DEFAULT_AGGREGATION;
        var indexInverse = map.containsKey(INDEX_INVERSE_KEY)
            ? (boolean) map.get(INDEX_INVERSE_KEY)
            : DEFAULT_INDEX_INVERSE;
        var inputProperties = map.getOrDefault(PROPERTIES_KEY, emptyMap());
        var properties = PropertyMappings.fromObject(inputProperties, aggregation);
        return new RelationshipProjection(type, orientation, aggregation, indexInverse, properties);
    }

    private static String nonEmptyString(Map<String, Object> config, String key) {
        @Nullable Object value = config.get(key);
        if (!(value instanceof String) || ((String) value).isEmpty()) {
            throw new IllegalArgumentException(formatWithLocale(
                "'%s' is not a valid value for the key '%s'",
                value, key
            ));
        }
        return (String) value;
    }

    public static RelationshipProjection fromObject(Object object, RelationshipType relationshipType) {
        if (object == null) {
            return ALL;
        }
        if (object instanceof String) {
            return new RelationshipProjection((String) object);
        }
        if (object instanceof Map) {
            var caseInsensitiveMap = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
            //noinspection unchecked
            caseInsensitiveMap.putAll((Map<String, Object>) object);
            return fromMap(caseInsensitiveMap, relationshipType);
        }
        throw new IllegalArgumentException(formatWithLocale(
            "Cannot construct a relationship filter out of a %s",
            object.getClass().getName()
        ));
    }

    public boolean isMultiGraph() {
        boolean somePropertyIsNotAggregated = properties()
            .mappings()
            .stream()
            .anyMatch(m -> Aggregation.equivalentToNone(m.aggregation()));
        return Aggregation.equivalentToNone(aggregation()) && (properties().isEmpty() || somePropertyIsNotAggregated);
    }

    public Map<String, Object> toObject() {
        Map<String, Object> value = new LinkedHashMap<>();
        value.put(TYPE_KEY, type);
        value.put(ORIENTATION_KEY, orientation.name());
        value.put(AGGREGATION_KEY, aggregation.name());
        value.put(INDEX_INVERSE_KEY, indexInverse);
        value.put(PROPERTIES_KEY, properties().toObject(true));
        return value;
    }

    RelationshipProjection withAdditionalPropertyMappings(PropertyMappings mappings) {
        var withSameAggregation = PropertyMappings.fromObject(mappings, aggregation);
        var newMappings = properties.mergeWith(withSameAggregation);

        if (newMappings == properties) {
            return this;
        }
        return new RelationshipProjection(type, orientation, aggregation, indexInverse, newMappings);
    }

    private static void validateConfigKeys(Map<String, Object> map) {
        ConfigKeyValidation.requireOnlyKeysFrom(List.of(
            TYPE_KEY,
            ORIENTATION_KEY,
            AGGREGATION_KEY,
            PROPERTIES_KEY,
            INDEX_INVERSE_KEY
        ), map.keySet());
    }
}

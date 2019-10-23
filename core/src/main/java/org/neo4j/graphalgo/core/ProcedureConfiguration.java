/*
 * Copyright (c) 2017-2019 "Neo4j,"
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
package org.neo4j.graphalgo.core;

import org.neo4j.graphalgo.PropertyMappings;
import org.neo4j.graphalgo.api.GraphFactory;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraph;
import org.neo4j.graphalgo.core.huge.HugeGraph;
import org.neo4j.graphalgo.core.loading.CypherGraphFactory;
import org.neo4j.graphalgo.core.loading.HugeGraphFactory;
import org.neo4j.graphalgo.core.lightweight.LightGraph;
import org.neo4j.graphalgo.core.loading.GraphLoadFactory;
import org.neo4j.graphalgo.core.utils.Directions;
import org.neo4j.graphalgo.core.utils.ParallelUtil;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.RelationshipType;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static java.util.Arrays.asList;
import static org.neo4j.graphalgo.core.ProcedureConstants.NODE_PROPERTIES_KEY;
import static org.neo4j.graphalgo.core.ProcedureConstants.RELATIONSHIP_PROPERTIES_KEY;

/**
 * Wrapper around configuration options map
 */
public class ProcedureConfiguration {

    private final Map<String, Object> config;

    public ProcedureConfiguration(Map<String, Object> config) {
        this.config = new HashMap<>(config);
    }

    /**
     * Checks if the given key exists in the configuration.
     *
     * @param key key to look for
     * @return true, iff the key exists
     */
    public boolean containsKey(String key) {
        return this.config.containsKey(key);
    }

    /**
     * Sets the nodeOrLabelQuery parameter.
     *
     * If the parameters is already set, it's overriden.
     *
     * @param nodeLabelOrQuery the query or identifier
     * @return this configuration
     */
    public ProcedureConfiguration setNodeLabelOrQuery(String nodeLabelOrQuery) {
        config.put(ProcedureConstants.NODE_LABEL_QUERY_KEY, nodeLabelOrQuery);
        return this;
    }

    /**
     * Sets the relationshipOrQuery parameter.
     *
     * If the parameters is already set, it's overriden.
     *
     * @param relationshipTypeOrQuery the relationshipQuery or Identifier
     * @return this configuration
     */
    public ProcedureConfiguration setRelationshipTypeOrQuery(String relationshipTypeOrQuery) {
        config.put(ProcedureConstants.RELATIONSHIP_QUERY_KEY, relationshipTypeOrQuery);
        return this;
    }

    /**
     * Sets the direction parameter.
     *
     * If the parameters is already set, it's overriden.
     *
     * @return this configuration
     */
    public ProcedureConfiguration setDirection(String direction) {
        config.put(ProcedureConstants.DIRECTION_KEY, direction);
        return this;
    }

    /**
     * Sets the direction parameter.
     *
     * If the parameters is already set, it's overriden.
     *
     * @return this configuration
     */
    public ProcedureConfiguration setDirection(Direction direction) {
        config.put(ProcedureConstants.DIRECTION_KEY, direction.name());
        return this;
    }

    /**
     * return either the Label or the cypher query for node request
     *
     * @return the label or query
     */
    public String getNodeLabelOrQuery() {
        return getString(ProcedureConstants.NODE_LABEL_QUERY_KEY, null);
    }

    /**
     * return either the Label or the cypher query for node request
     *
     * @param defaultValue default value if {@link ProcedureConstants#NODE_LABEL_QUERY_KEY}
     *                     is not set
     * @return the label or query
     */
    public String getNodeLabelOrQuery(String defaultValue) {
        return getString(ProcedureConstants.NODE_LABEL_QUERY_KEY, defaultValue);
    }

    public String getRelationshipOrQuery() {
        return getString(ProcedureConstants.RELATIONSHIP_QUERY_KEY, null);
    }

    /**
     * return the name of the property to write to
     *
     * @return property name
     */
    public String getWriteProperty() {
        return getWriteProperty(ProcedureConstants.WRITE_PROPERTY_DEFAULT);
    }

    /**
     * return either the name of the property to write to if given or defaultValue
     *
     * @param defaultValue a default value
     * @return the property name
     */
    public String getWriteProperty(String defaultValue) {
        return getString(ProcedureConstants.WRITE_PROPERTY_KEY, defaultValue);
    }

    /**
     * return either the relationship name or a cypher query for requesting the relationships
     * TODO: @mh pls. validate
     *
     * @param defaultValue a default value
     * @return the relationship name or query
     */
    public String getRelationshipOrQuery(String defaultValue) {
        return getString(ProcedureConstants.RELATIONSHIP_QUERY_KEY, defaultValue);
    }

    /**
     * return whether the write-back option has been set
     *
     * @return true if write is activated, false otherwise
     */
    public boolean isWriteFlag() {
        return isWriteFlag(true);
    }

    /**
     * flag for requesting additional result stats
     *
     * @return true if stat flag is activated, false otherwise
     */
    public boolean isStatsFlag() {
        return isStatsFlag(false);
    }

    /**
     * return whether the write-back option has been set
     *
     * @param defaultValue a default value
     * @return true if write is activated, false otherwise
     */
    public boolean isWriteFlag(boolean defaultValue) {
        return get(ProcedureConstants.WRITE_FLAG_KEY, defaultValue);
    }

    public boolean isStatsFlag(boolean defaultValue) {
        return get(ProcedureConstants.STATS_FLAG_KEY, defaultValue);
    }

    public boolean hasWeightProperty() {
        return containsKey(ProcedureConstants.DEPRECATED_RELATIONSHIP_PROPERTY_KEY);
    }

    public String getWeightProperty() {
        return getString(ProcedureConstants.DEPRECATED_RELATIONSHIP_PROPERTY_KEY, null);
    }

    public PropertyMappings getNodeProperties() {
        return getPropertyMappings(NODE_PROPERTIES_KEY);
    }

    public PropertyMappings getRelationshipProperties() {
        return getPropertyMappings(RELATIONSHIP_PROPERTIES_KEY);
    }

    private PropertyMappings getPropertyMappings(String paramKey) {
        Object propertyMappings = get(paramKey, null);
        if (propertyMappings != null) {
            return PropertyMappings.fromObject(propertyMappings);
        }
        return PropertyMappings.EMPTY;
    }

    public double getWeightPropertyDefaultValue(double defaultValue) {
        return getNumber(ProcedureConstants.DEFAULT_VALUE_KEY, defaultValue).doubleValue();
    }

    /**
     * return the number of iterations a algorithm has to compute
     *
     * @param defaultValue a default value
     * @return
     */
    public int getIterations(int defaultValue) {
        return getNumber(ProcedureConstants.ITERATIONS_KEY, defaultValue).intValue();
    }

    /**
     * get the batchSize for parallel evaluation
     *
     * @return batch size
     */
    public int getBatchSize() {
        return getNumber(ProcedureConstants.BATCH_SIZE_KEY, ParallelUtil.DEFAULT_BATCH_SIZE).intValue();
    }

    public int getConcurrency() {
        return getConcurrency(Pools.DEFAULT_CONCURRENCY);
    }

    public int getConcurrency(int defaultValue) {
        int requestedConcurrency = getNumber(ProcedureConstants.CONCURRENCY_KEY, defaultValue).intValue();
        return Pools.allowedConcurrency(requestedConcurrency);
    }

    public int getReadConcurrency() {
        return getReadConcurrency(Pools.DEFAULT_CONCURRENCY);
    }

    public int getReadConcurrency(int defaultValue) {
        Number readConcurrency = getNumber(
                ProcedureConstants.READ_CONCURRENCY_KEY,
                ProcedureConstants.CONCURRENCY_KEY,
                defaultValue);
        int requestedConcurrency = readConcurrency.intValue();
        return Pools.allowedConcurrency(requestedConcurrency);
    }

    public int getWriteConcurrency() {
        return getWriteConcurrency(Pools.DEFAULT_CONCURRENCY);
    }

    public int getWriteConcurrency(int defaultValue) {
        Number writeConcurrency = getNumber(
                ProcedureConstants.WRITE_CONCURRENCY_KEY,
                ProcedureConstants.CONCURRENCY_KEY,
                defaultValue);
        int requestedConcurrency = writeConcurrency.intValue();
        return Pools.allowedConcurrency(requestedConcurrency);
    }

    public String getDirectionName(String defaultDirection) {
        return get(ProcedureConstants.DIRECTION_KEY, defaultDirection);
    }

    public Direction getDirection(Direction defaultDirection) {
        return Directions.fromString(getDirectionName(defaultDirection.name()));
    }

    public RelationshipType getRelationship() {
        return getRelationshipOrQuery() == null ? null : RelationshipType.withName(getRelationshipOrQuery());
    }

    public String getGraphName(String defaultValue) {
        return getString(ProcedureConstants.GRAPH_IMPL_KEY, defaultValue);
    }

    public Class<? extends GraphFactory> getGraphImpl() {
        return getGraphImpl(ProcedureConstants.GRAPH_IMPL_DEFAULT);
    }

    /**
     * @return the Graph-Implementation Factory class
     */
    public Class<? extends GraphFactory> getGraphImpl(String defaultGraphImpl) {
        final String graphImpl = getGraphName(defaultGraphImpl);
        switch (graphImpl.toLowerCase(Locale.ROOT)) {
            case CypherGraphFactory.TYPE:
                return CypherGraphFactory.class;
            case LightGraph.TYPE:
            case HeavyGraph.TYPE:
            case HugeGraph.TYPE:
                return HugeGraphFactory.class;
            default:
                if (validCustomName(graphImpl) && GraphLoadFactory.exists(graphImpl)) {
                    return GraphLoadFactory.class;
                }
                throw new IllegalArgumentException("Unknown impl: " + graphImpl);
        }
    }

    private static final Set<String> RESERVED = new HashSet<>(asList(
            CypherGraphFactory.TYPE,
            LightGraph.TYPE,
            HugeGraph.TYPE,
            HeavyGraph.TYPE));

    public static boolean validCustomName(String name) {
        return name != null && !name.trim().isEmpty() && !RESERVED.contains(name.trim().toLowerCase());
    }

    public final Class<? extends GraphFactory> getGraphImpl(
            String defaultImpl,
            String... alloweds) {
        String graphName = getGraphName(defaultImpl);
        List<String> allowedNames = asList(alloweds);
        if (allowedNames.contains(graphName) || allowedNames.contains(GraphLoadFactory.getType(graphName))) {
            return getGraphImpl(defaultImpl);
        }
        throw new IllegalArgumentException("The graph algorithm only supports these graph types; " + allowedNames);
    }

    /**
     * specialized getter for String which either returns the value
     * if found, the defaultValue if the key is not found or null if
     * the key is found but its value is empty.
     *
     * @param key          configuration key
     * @param defaultValue the default value if key is not found
     * @return the configuration value
     */
    public String getString(String key, String defaultValue) {
        String value = (String) config.getOrDefault(key, defaultValue);
        return (null == value || "".equals(value)) ? defaultValue : value;
    }

    public String getString(String key, String oldKey, String defaultValue) {
        return getChecked(key, oldKey, defaultValue, String.class);
    }

    public Optional<String> getString(String key) {
        if (config.containsKey(key)) {
            // Optional.of will throw an NPE if key does not exist because of the default value
            //  which we want have as a kind of sanity check - the default value *should* not be used
            return Optional.of(getChecked(key, null, String.class));
        }
        return Optional.empty();
    }

    public Optional<String> getStringWithFallback(String key, String oldKey) {
        Optional<String> value = getString(key);
        // #migration-note: On Java9+ there is a #or method on Optional that we should use instead
        //  https://docs.oracle.com/en/java/javase/11/docs/api/java.base/java/util/Optional.html#or(java.util.function.Supplier)
        if (!value.isPresent()) {
            value = getString(oldKey);
        }
        return value;
    }

    public Object get(String key) {
        return config.get(key);
    }

    public Boolean getBool(String key, boolean defaultValue) {
        return getChecked(key, defaultValue, Boolean.class);
    }

    public Number getNumber(String key, Number defaultValue) {
        return getChecked(key, defaultValue, Number.class);
    }

    public Number getNumber(String key, String oldKey, Number defaultValue) {
        Object value = get(key, oldKey, (Object) defaultValue);
        if (null == value) {
            return defaultValue;
        }
        if (!(value instanceof Number)) {
            throw new IllegalArgumentException("The value of " + key + " must be a Number type");
        }
        return (Number) value;
    }

    public int getInt(String key, int defaultValue) {
        Number value = (Number) config.get(key);
        if (null == value) {
            return defaultValue;
        }
        return value.intValue();
    }

    @SuppressWarnings("unchecked")
    public <V> V get(String key, V defaultValue) {
        Object value = config.get(key);
        if (null == value) {
            return defaultValue;
        }
        return (V) value;
    }

    public Double getSkipValue(Double defaultValue) {
        String key = ProcedureConstants.SKIP_VALUE_KEY;
        if (!config.containsKey(key)) {
            return defaultValue;
        }
        Object value = config.get(key);

        if(value == null) {
            return null;
        }

        return typedValue(key, Number.class, value).doubleValue();
    }

    /**
     * Get and convert the value under the given key to the given type.
     *
     * @return the found value under the key - if it is of the provided type,
     *         or the provided default value if no entry for the key is found (or it's mapped to null).
     * @throws IllegalArgumentException if a value was found, but it is not of the expected type.
     */
    public <V> V getChecked(String key, V defaultValue, Class<V> expectedType) {
        Object value = config.get(key);
        return checkValue(key, defaultValue, expectedType, value);
    }

    @SuppressWarnings("unchecked")
    public <V> V get(String newKey, String oldKey, V defaultValue) {
        Object value = config.get(newKey);
        if (null == value) {
            value = config.get(oldKey);
        }
        return null == value ? defaultValue : (V) value;
    }

    public <V> V getChecked(String key, String oldKey, V defaultValue, Class<V> expectedType) {
        Object value = get(key, oldKey, null);
        return checkValue(key, defaultValue, expectedType, value);
    }

    private <V> V checkValue(final String key, final V defaultValue, final Class<V> expectedType, final Object value) {
        if (null == value) {
            return defaultValue;
        }
        return typedValue(key, expectedType, value);
    }

    private <V> V typedValue(String key, Class<V> expectedType, Object value) {
        if (!expectedType.isInstance(value)) {
            String template = "The value of %s must be a %s.";
            String message = String.format(template, key, expectedType.getSimpleName());
            throw new IllegalArgumentException(message);
        }
        return expectedType.cast(value);
    }

    public static ProcedureConfiguration create(Map<String, Object> config) {
        return new ProcedureConfiguration(config);
    }

    public static ProcedureConfiguration empty() {
        return new ProcedureConfiguration(Collections.emptyMap());
    }

    public Map<String, Object> getParams() {
        return (Map<String, Object>) config.getOrDefault("params", Collections.emptyMap());
    }

    public DeduplicationStrategy getDeduplicationStrategy() {
        String strategy = get("duplicateRelationships", null);
        return strategy != null ? DeduplicationStrategy.lookup(strategy.toUpperCase()) : DeduplicationStrategy.DEFAULT;
    }
}

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

import org.neo4j.graphalgo.api.GraphFactory;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.heavyweight.HeavyCypherGraphFactory;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraph;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraphFactory;
import org.neo4j.graphalgo.core.huge.HugeGraphImpl;
import org.neo4j.graphalgo.core.huge.loader.HugeGraphFactory;
import org.neo4j.graphalgo.core.lightweight.LightGraph;
import org.neo4j.graphalgo.core.loading.LoadGraphFactory;
import org.neo4j.graphalgo.core.neo4jview.GraphView;
import org.neo4j.graphalgo.core.neo4jview.GraphViewFactory;
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

/**
 * Wrapper around configuration options map
 *
 * @author mknblch
 */
public class ProcedureConfiguration {

    private final Map<String, Object> config;

    public ProcedureConfiguration(Map<String, Object> config) {
        this.config = new HashMap<>(config);
    }

    /**
     * check if all given keys exist
     *
     * @param keys one or many keys
     * @return true if key is known, false otherwise
     */
    public boolean containsKeys(String... keys) {
        for (String key : keys) {
            if (!config.containsKey(key)) {
                return false;
            }
        }
        return true;
    }

    /**
     * override the nodeOrLabelQuery param
     *
     * @param nodeLabelOrQuery the query or identifier
     * @return self
     */
    public ProcedureConfiguration overrideNodeLabelOrQuery(String nodeLabelOrQuery) {
        config.put(ProcedureConstants.NODE_LABEL_QUERY_PARAM, nodeLabelOrQuery);
        return this;
    }

    /**
     * override relationshipOrQuery param
     *
     * @param relationshipTypeOrQuery the relationshipQuery or Identifier
     * @return self
     */
    public ProcedureConfiguration overrideRelationshipTypeOrQuery(String relationshipTypeOrQuery) {
        config.put(ProcedureConstants.RELATIONSHIP_QUERY_PARAM, relationshipTypeOrQuery);
        return this;
    }

    /**
     *
     */
    public ProcedureConfiguration overrideDirection(String direction) {
        config.put(ProcedureConstants.DIRECTION, direction);
        return this;
    }

    public ProcedureConfiguration overrideDirection(Direction direction) {
        config.put(ProcedureConstants.DIRECTION, direction.name());
        return this;
    }

    /**
     * override property param
     *
     * @return self
     */
    public ProcedureConfiguration overrideWeightProperty(String weightProperty) {
        config.put(ProcedureConstants.PROPERTY_PARAM, weightProperty);
        return this;
    }

    /**
     * return either the Label or the cypher query for node request
     *
     * @return the label or query
     */
    public String getNodeLabelOrQuery() {
        return getString(ProcedureConstants.NODE_LABEL_QUERY_PARAM, null);
    }

    /**
     * return either the Label or the cypher query for node request
     *
     * @param defaultValue default value if {@link ProcedureConstants#NODE_LABEL_QUERY_PARAM}
     *                     is not set
     * @return the label or query
     */
    public String getNodeLabelOrQuery(String defaultValue) {
        return getString(ProcedureConstants.NODE_LABEL_QUERY_PARAM, defaultValue);
    }

    public String getRelationshipOrQuery() {
        return getString(ProcedureConstants.RELATIONSHIP_QUERY_PARAM, null);
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
        return getString(ProcedureConstants.WRITE_PROPERTY, defaultValue);
    }

    /**
     * return either the relationship name or a cypher query for requesting the relationships
     * TODO: @mh pls. validate
     *
     * @param defaultValue a default value
     * @return the relationship name or query
     */
    public String getRelationshipOrQuery(String defaultValue) {
        return getString(ProcedureConstants.RELATIONSHIP_QUERY_PARAM, defaultValue);
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
     * TODO
     *
     * @return
     */
    public boolean isCypherFlag() {
        return isCypherFlag(false);
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
        return get(ProcedureConstants.WRITE_FLAG, defaultValue);
    }

    public boolean isCypherFlag(boolean defaultValue) {
        return (boolean) config.getOrDefault(ProcedureConstants.CYPHER_QUERY, defaultValue);
    }

    public boolean isStatsFlag(boolean defaultValue) {
        return get(ProcedureConstants.STATS_FLAG, defaultValue);
    }

    public boolean hasWeightProperty() {
        return containsKeys(ProcedureConstants.PROPERTY_PARAM);
    }

    public String getWeightProperty() {
        return getString(ProcedureConstants.PROPERTY_PARAM, null);
    }

    public double getWeightPropertyDefaultValue(double defaultValue) {
        return getNumber(ProcedureConstants.DEFAULT_PROPERTY_VALUE_PARAM, defaultValue).doubleValue();
    }

    /**
     * return the number of iterations a algorithm has to compute
     *
     * @param defaultValue a default value
     * @return
     */
    public int getIterations(int defaultValue) {
        return getNumber(ProcedureConstants.ITERATIONS_PARAM, defaultValue).intValue();
    }

    /**
     * get the batchSize for parallel evaluation
     *
     * @return batch size
     */
    public int getBatchSize() {
        return getNumber(ProcedureConstants.BATCH_SIZE_PARAM, ParallelUtil.DEFAULT_BATCH_SIZE).intValue();
    }

    public int getBatchSize(int defaultValue) {
        return getNumber(ProcedureConstants.BATCH_SIZE_PARAM, defaultValue).intValue();
    }

    public int getConcurrency() {
        return getConcurrency(Pools.DEFAULT_CONCURRENCY);
    }

    public int getConcurrency(int defaultValue) {
        int requestedConcurrency = getNumber(ProcedureConstants.CONCURRENCY, defaultValue).intValue();
        return Pools.allowedConcurrency(requestedConcurrency);
    }

    public String getDirectionName() {
        return getDirectionName(ProcedureConstants.DIRECTION_DEFAULT);
    }

    public String getDirectionName(String defaultDirection) {
        return get(ProcedureConstants.DIRECTION, defaultDirection);
    }

    public Direction getDirection(Direction defaultDirection) {
        return Directions.fromString(getDirectionName(defaultDirection.name()));
    }

    public RelationshipType getRelationship() {
        return getRelationshipOrQuery() == null ? null : RelationshipType.withName(getRelationshipOrQuery());
    }

    public String getGraphName(String defaultValue) {
        return getString(ProcedureConstants.GRAPH_IMPL_PARAM, defaultValue);
    }

    public Class<? extends GraphFactory> getGraphImpl() {
        return getGraphImpl(ProcedureConstants.DEFAULT_GRAPH_IMPL);
    }

    /**
     * @return the Graph-Implementation Factory class
     */
    public Class<? extends GraphFactory> getGraphImpl(String defaultGraphImpl) {
        final String graphImpl = getGraphName(defaultGraphImpl);
        switch (graphImpl.toLowerCase(Locale.ROOT)) {
            case HeavyGraph.TYPE:
                return HeavyGraphFactory.class;
            case HeavyCypherGraphFactory.TYPE:
                return HeavyCypherGraphFactory.class;
            case LightGraph.TYPE:
                return HeavyGraphFactory.class;
            case GraphView.TYPE:
                return GraphViewFactory.class;
            case HugeGraphImpl.TYPE:
                return HugeGraphFactory.class;
            default:
                if (validCustomName(graphImpl) && LoadGraphFactory.check(graphImpl)) {
                    return LoadGraphFactory.class;
                }
                throw new IllegalArgumentException("Unknown impl: " + graphImpl);
        }
    }

    private static Set<String> RESERVED = new HashSet<>(asList(HeavyGraph.TYPE, HeavyCypherGraphFactory.TYPE,
            LightGraph.TYPE, GraphView.TYPE, HeavyGraph.TYPE));

    public static boolean validCustomName(String name) {
        return name != null && !name.trim().isEmpty() && !RESERVED.contains(name.trim().toLowerCase());
    }

    public final Class<? extends GraphFactory> getGraphImpl(
            String defaultImpl,
            String... alloweds) {
        String graphName = getGraphName(defaultImpl);
        List<String> allowedNames = asList(alloweds);
        if (allowedNames.contains(graphName) || allowedNames.contains(LoadGraphFactory.getType(graphName))) {
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

    public Optional<String> getString(String key) {
        if (config.containsKey(key)) {
            return Optional.of((String) get(key));
        }
        return Optional.empty();
    }

    public Object get(String key) {
        return config.get(key);
    }

    @SuppressWarnings("unchecked")
    public Number getNumber(String key, Number defaultValue) {
        Object value = config.get(key);
        if (null == value) {
            return defaultValue;
        }
        if (!(value instanceof Number)) {
            throw new IllegalArgumentException("The value of " + key + " must Number type");
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

    public <V> V get(String newKey, String oldKey, V defaultValue) {
        Object value = config.get(newKey);
        if (null == value) {
            value = config.get(oldKey);
            if (null == value) {
                return defaultValue;
            }
        }
        return (V) value;
    }

    public static ProcedureConfiguration create(Map<String, Object> config) {
        return new ProcedureConfiguration(config);
    }

    private static String reverseGraphLookup(Class<? extends GraphFactory> cls) {
        if (HeavyGraphFactory.class.isAssignableFrom(cls)) {
            return "heavy";
        }
        if (HeavyCypherGraphFactory.class.isAssignableFrom(cls)) {
            return "cypher";
        }
        if (GraphViewFactory.class.isAssignableFrom(cls)) {
            return "kernel";
        }
        if (HugeGraphFactory.class.isAssignableFrom(cls)) {
            return "huge";
        }
        throw new IllegalArgumentException("Unknown impl: " + cls);
    }

    public Map<String, Object> getParams() {
        return (Map<String, Object>) config.getOrDefault("params", Collections.emptyMap());
    }

    public DuplicateRelationshipsStrategy getDuplicateRelationshipsStrategy() {
        String strategy = get("duplicateRelationships", null);
        return strategy != null ? DuplicateRelationshipsStrategy.valueOf(strategy.toUpperCase()) : DuplicateRelationshipsStrategy.NONE;
    }


}

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

import org.neo4j.graphalgo.PropertyMapping;
import org.neo4j.graphalgo.PropertyMappings;
import org.neo4j.graphalgo.api.GraphFactory;
import org.neo4j.graphalgo.api.GraphSetup;
import org.neo4j.graphalgo.core.huge.HugeGraph;
import org.neo4j.graphalgo.core.loading.CypherGraphFactory;
import org.neo4j.graphalgo.core.loading.GraphCatalog;
import org.neo4j.graphalgo.core.loading.HugeGraphFactory;
import org.neo4j.graphalgo.core.utils.Directions;
import org.neo4j.graphalgo.core.utils.ParallelUtil;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphalgo.core.utils.mem.MemoryEstimation;
import org.neo4j.graphalgo.newapi.BaseAlgoConfig;
import org.neo4j.graphalgo.newapi.GraphCreateConfig;
import org.neo4j.graphalgo.newapi.WriteConfig;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.internal.kernel.api.security.AuthSubject;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static java.util.Arrays.asList;
import static org.neo4j.graphalgo.core.ProcedureConstants.DEFAULT_VALUE_DEFAULT;
import static org.neo4j.graphalgo.core.ProcedureConstants.NODECOUNT_KEY;
import static org.neo4j.graphalgo.core.ProcedureConstants.NODE_PROPERTIES_KEY;
import static org.neo4j.graphalgo.core.ProcedureConstants.RELATIONSHIP_PROPERTIES_KEY;
import static org.neo4j.graphalgo.core.ProcedureConstants.RELCOUNT_KEY;

/**
 * Wrapper around configuration options map
 */
public class ProcedureConfiguration implements BaseAlgoConfig, WriteConfig {

    public static final String HEAVY_GRAPH_TYPE = "heavy";
    public static final String LIGHT_GRAPH_TYPE = "light";
    public static final String ALGO_SPECIFIC_DEFAULT_WEIGHT = "algoSpecificDefaultWeight";

    private final CypherMapWrapper configurationMap;

    private final String username;
    private final boolean computeHistogram;
    private final boolean computeCommunityCount;

    protected ProcedureConfiguration(
        CypherMapWrapper configurationMap,
        String username,
        boolean computeHistogram,
        boolean computeCommunityCount
    ) {
        this.configurationMap = configurationMap;
        this.username = username;
        this.computeHistogram = computeHistogram;
        this.computeCommunityCount = computeCommunityCount;
    }

    public GraphLoader configureLoader(GraphLoader loader) {
        if (hasWeightProperty()) {
            loader.withRelationshipProperties(PropertyMapping.of(
                getWeightProperty(),
                getWeightPropertyDefaultValue(getAlgoSpecificDefaultWeightValue())
            ));
        }

        String label = getNodeLabelOrQuery();
        String relationship = getRelationshipOrQuery();
        return loader.withUsername(getUsername())
            .withName(getGraphName(null))
            .withOptionalLabel(label)
            .withOptionalRelationshipType(relationship)
            .withConcurrency(getReadConcurrency())
            .withBatchSize(getBatchSize())
            .withDeduplicationStrategy(getDeduplicationStrategy())
            .withParams(getParams())
            .withLoadedGraph(getGraphImpl() == GraphCatalog.class);
    }

    private double getAlgoSpecificDefaultWeightValue() {
        return configurationMap.getDouble(ALGO_SPECIFIC_DEFAULT_WEIGHT, DEFAULT_VALUE_DEFAULT);
    }

    public ProcedureConfiguration setAlgoSpecificDefaultWeight(double defaultWeightProperty) {
        return new ProcedureConfiguration(configurationMap.withDouble(
            ALGO_SPECIFIC_DEFAULT_WEIGHT,
            defaultWeightProperty
        ), username, computeHistogram, computeCommunityCount);
    }

    public MemoryEstimation estimate(GraphSetup setup, GraphFactory factory) {
        MemoryEstimation estimation;

        if (containsKey(NODECOUNT_KEY)) {
            GraphDimensions dimensions = factory.dimensions();
            long nodeCount = get(NODECOUNT_KEY, 0L);
            long relCount = get(RELCOUNT_KEY, 0L);
            dimensions.nodeCount(nodeCount);
            dimensions.maxRelCount(relCount);
            estimation = HugeGraphFactory
                .getMemoryEstimation(setup, dimensions, true);
        } else {
            estimation = factory.memoryEstimation();
        }
        return estimation;
    }

    // Below methods are delegators that will be removed
    // START DELEGATION

    public boolean containsKey(String key) {
        return configurationMap.containsKey(key);
    }

    public Optional<String> getStringWithFallback(String key, String oldKey) {
        return configurationMap.getStringWithFallback(key, oldKey);
    }

    public <V> V get(String key, V defaultValue) {
        return configurationMap.get(key, defaultValue);
    }

    public String getString(String key, String defaultValue) {
        return configurationMap.getString(key, defaultValue);
    }

    public String getString(String key, String oldKey, String defaultValue) {
        return configurationMap.getString(key, oldKey, defaultValue);
    }

    public Optional<String> getString(String key) {
        return configurationMap.getString(key);
    }

    public Boolean getBool(String key, boolean defaultValue) {
        return configurationMap.getBool(key, defaultValue);
    }

    public Number getNumber(String key, Number defaultValue) {
        return configurationMap.getNumber(key, defaultValue);
    }

    public Number getNumber(String key, String oldKey, Number defaultValue) {
        return configurationMap.getNumber(key, oldKey, defaultValue);
    }

    public int getInt(String key, int defaultValue) {
        return configurationMap.getInt(key, defaultValue);
    }

    public <V> V getChecked(String key, V defaultValue, Class<V> expectedType) {
        return configurationMap.getChecked(key, defaultValue, expectedType);
    }

    public <V> V get(String newKey, String oldKey, V defaultValue) {
        return configurationMap.get(newKey, oldKey, defaultValue);
    }

    // END DELEGATION

    public String getUsername() {
        return username();
    }

    @Override
    public String username() {
        return username;
    }

    /**
     * Sets the nodeOrLabelQuery parameter.
     *
     * If the parameters is already set, it's overriden.
     *
     * @param nodeLabelOrQuery the query or identifier
     * @return an updated configuration
     */
    public ProcedureConfiguration setNodeLabelOrQuery(String nodeLabelOrQuery) {
        return new ProcedureConfiguration(
            configurationMap.withString(ProcedureConstants.NODE_LABEL_QUERY_KEY, nodeLabelOrQuery),
            username,
            computeHistogram,
            computeCommunityCount
        );
    }

    /**
     * Sets the relationshipOrQuery parameter.
     *
     * If the parameters is already set, it's overriden.
     *
     * @param relationshipTypeOrQuery the relationshipQuery or Identifier
     * @return an updated configuration
     */
    public ProcedureConfiguration setRelationshipTypeOrQuery(String relationshipTypeOrQuery) {
        return new ProcedureConfiguration(
            configurationMap.withString(ProcedureConstants.RELATIONSHIP_QUERY_KEY, relationshipTypeOrQuery),
            username,
            computeHistogram,
            computeCommunityCount
        );
    }

    /**
     * Sets the direction parameter.
     *
     * If the parameters is already set, it's overriden.
     *
     * @return this configuration
     */
    public ProcedureConfiguration setDirection(String direction) {
        return new ProcedureConfiguration(
            configurationMap.withString(ProcedureConstants.DIRECTION_KEY, direction),
            username,
            computeHistogram,
            computeCommunityCount
        );
    }

    public ProcedureConfiguration setComputeHistogram(boolean computeHistogram) {
        return new ProcedureConfiguration(configurationMap, username, computeHistogram, computeCommunityCount);
    }

    public ProcedureConfiguration setComputeCommunityCount(boolean computeCommunityCount) {
        return new ProcedureConfiguration(configurationMap, username, computeHistogram, computeCommunityCount);
    }

    /**
     * True iff the procedure caller yields histogram fields (p01, p25, etc.).
     */
    public boolean computeHistogram() {
        return computeHistogram;
    }

    /**
     * True iff the procedure caller yields community counts (communityCount, setCount).
     */
    public boolean computeCommunityCount() {
        return computeCommunityCount;
    }

    /**
     * return either the Label or the cypher query for node request
     *
     * @return the label or query
     */
    public String getNodeLabelOrQuery() {
        return configurationMap.getString(ProcedureConstants.NODE_LABEL_QUERY_KEY, null);
    }

    /**
     * return either the Label or the cypher query for node request
     *
     * @param defaultValue default value if {@link ProcedureConstants#NODE_LABEL_QUERY_KEY}
     *                     is not set
     * @return the label or query
     */
    public String getNodeLabelOrQuery(String defaultValue) {
        return configurationMap.getString(ProcedureConstants.NODE_LABEL_QUERY_KEY, defaultValue);
    }

    public String getRelationshipOrQuery() {
        return configurationMap.getString(ProcedureConstants.RELATIONSHIP_QUERY_KEY, null);
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
        return configurationMap.getString(ProcedureConstants.WRITE_PROPERTY_KEY, defaultValue);
    }

    /**
     * return either the relationship name or a cypher query for requesting the relationships
     * TODO: @mh pls. validate
     *
     * @param defaultValue a default value
     * @return the relationship name or query
     */
    public String getRelationshipOrQuery(String defaultValue) {
        return configurationMap.getString(ProcedureConstants.RELATIONSHIP_QUERY_KEY, defaultValue);
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
        return configurationMap.get(ProcedureConstants.STATS_FLAG_KEY, false);
    }

    /**
     * return whether the write-back option has been set
     *
     * @param defaultValue a default value
     * @return true if write is activated, false otherwise
     */
    public boolean isWriteFlag(boolean defaultValue) {
        return configurationMap.get(ProcedureConstants.WRITE_FLAG_KEY, defaultValue);
    }

    public boolean hasWeightProperty() {
        return containsKey(ProcedureConstants.DEPRECATED_RELATIONSHIP_PROPERTY_KEY);
    }

    public String getWeightProperty() {
        return configurationMap.getString(ProcedureConstants.DEPRECATED_RELATIONSHIP_PROPERTY_KEY, null);
    }

    public PropertyMappings getNodeProperties() {
        return getPropertyMappings(NODE_PROPERTIES_KEY);
    }

    public PropertyMappings getRelationshipProperties() {
        return getPropertyMappings(RELATIONSHIP_PROPERTIES_KEY);
    }

    private PropertyMappings getPropertyMappings(String paramKey) {
        Object propertyMappings = configurationMap.get(paramKey, null);
        if (propertyMappings != null) {
            return PropertyMappings.fromObject(propertyMappings);
        }
        return PropertyMappings.of();
    }

    public double getWeightPropertyDefaultValue(double defaultValue) {
        return configurationMap.getNumber(ProcedureConstants.DEFAULT_VALUE_KEY, defaultValue).doubleValue();
    }

    /**
     * return the number of iterations a algorithm has to compute
     *
     * @param defaultValue a default value
     * @return
     */
    public int getIterations(int defaultValue) {
        return configurationMap.getNumber(ProcedureConstants.ITERATIONS_KEY, defaultValue).intValue();
    }

    /**
     * get the batchSize for parallel evaluation
     *
     * @return batch size
     */
    public int getBatchSize() {
        return configurationMap.getNumber(ProcedureConstants.BATCH_SIZE_KEY, ParallelUtil.DEFAULT_BATCH_SIZE).intValue();
    }

    @Override
    public int concurrency() {
        return concurrency(Pools.DEFAULT_CONCURRENCY);
    }

    @Override
    public Optional<String> graphName() {
        return Optional.ofNullable(getGraphName(null));
    }

    @Override
    public Optional<GraphCreateConfig> implicitCreateConfig() {
        return Optional.empty();
    }

    public int concurrency(int defaultValue) {
        int requestedConcurrency = configurationMap.getNumber(ProcedureConstants.CONCURRENCY_KEY, defaultValue).intValue();
        return Pools.allowedConcurrency(requestedConcurrency);
    }

    public int getReadConcurrency() {
        return getReadConcurrency(Pools.DEFAULT_CONCURRENCY);
    }

    public int getReadConcurrency(int defaultValue) {
        Number readConcurrency = configurationMap.getNumber(
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
        Number writeConcurrency = configurationMap.getNumber(
                ProcedureConstants.WRITE_CONCURRENCY_KEY,
                ProcedureConstants.CONCURRENCY_KEY,
                defaultValue);
        int requestedConcurrency = writeConcurrency.intValue();
        return Pools.allowedConcurrency(requestedConcurrency);
    }

    private String getDirectionName(String defaultDirection) {
        return configurationMap.get(ProcedureConstants.DIRECTION_KEY, defaultDirection);
    }

    public Direction getDirection(Direction defaultDirection) {
        return Directions.fromString(getDirectionName(defaultDirection.name()));
    }

    public RelationshipType getRelationship() {
        return getRelationshipOrQuery() == null ? null : RelationshipType.withName(getRelationshipOrQuery());
    }

    public String getGraphName(String defaultValue) {
        return configurationMap.getString(ProcedureConstants.GRAPH_IMPL_KEY, defaultValue);
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
            case LIGHT_GRAPH_TYPE:
            case HEAVY_GRAPH_TYPE:
            case HugeGraph.TYPE:
                return HugeGraphFactory.class;
            default:
                if (validCustomName(graphImpl) && GraphCatalog.exists(getUsername(), graphImpl)) {
                    return GraphCatalog.class;
                }
                throw new IllegalArgumentException("Unknown impl: " + graphImpl);
        }
    }

    private static final Set<String> RESERVED = new HashSet<>(asList(
        CypherGraphFactory.TYPE,
        HugeGraph.TYPE,
        LIGHT_GRAPH_TYPE,
        HEAVY_GRAPH_TYPE
    ));

    private static boolean validCustomName(String name) {
        return name != null && !name.trim().isEmpty() && !RESERVED.contains(name.trim().toLowerCase());
    }

    public final Class<? extends GraphFactory> getGraphImpl(
            String defaultImpl,
            String... alloweds) {
        String graphName = getGraphName(defaultImpl);
        List<String> allowedNames = asList(alloweds);
        if (allowedNames.contains(graphName) || allowedNames.contains(GraphCatalog.getType(getUsername(), graphName))) {
            return getGraphImpl(defaultImpl);
        }
        throw new IllegalArgumentException("The graph algorithm only supports these graph types; " + allowedNames);
    }

    public Double getSkipValue(Double defaultValue) {
        String key = ProcedureConstants.SKIP_VALUE_KEY;
        if (!configurationMap.containsKey(key)) {
            return defaultValue;
        }
        Object value = configurationMap.get(key, null);

        if (value == null) {
            return null;
        }

        return CypherMapWrapper.typedValue(key, Number.class, value).doubleValue();
    }

    public static ProcedureConfiguration create(Map<String, Object> config, String username) {
        return create(CypherMapWrapper.create(config), username);
    }

    public static ProcedureConfiguration create(CypherMapWrapper map, String username) {
        return new ProcedureConfiguration(map, username, false, false);
    }

    public static ProcedureConfiguration create(String username) {
        return create(CypherMapWrapper.empty(), username);
    }

    public static ProcedureConfiguration empty() {
        return create(CypherMapWrapper.empty(), AuthSubject.ANONYMOUS.username());
    }

    public Map<String, Object> getParams() {
        return configurationMap.getChecked("params", Collections.emptyMap(), Map.class);
    }

    public DeduplicationStrategy getDeduplicationStrategy() {
        String strategy = configurationMap.get("duplicateRelationships", null);
        return strategy != null ? DeduplicationStrategy.lookup(strategy.toUpperCase()) : DeduplicationStrategy.DEFAULT;
    }

    @Override
    public String writeProperty() {
        return getWriteProperty();
    }

    @Override
    public int writeConcurrency() {
        return getWriteConcurrency(Pools.DEFAULT_CONCURRENCY);
    }
}

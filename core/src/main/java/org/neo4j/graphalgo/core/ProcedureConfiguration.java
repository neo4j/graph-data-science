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
package org.neo4j.graphalgo.core;

import org.neo4j.graphalgo.api.GraphFactory;
import org.neo4j.graphalgo.api.GraphSetup;
import org.neo4j.graphalgo.core.utils.Directions;
import org.neo4j.graphalgo.core.utils.ParallelUtil;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphalgo.core.utils.ProjectionParser;
import org.neo4j.graphalgo.core.utils.mem.MemoryEstimation;
import org.neo4j.graphalgo.newapi.AlgoBaseConfig;
import org.neo4j.graphalgo.newapi.GraphCreateConfig;
import org.neo4j.graphalgo.newapi.WriteConfig;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.internal.kernel.api.security.AuthSubject;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.neo4j.graphalgo.core.ProcedureConstants.NODECOUNT_KEY;
import static org.neo4j.graphalgo.core.ProcedureConstants.RELCOUNT_KEY;

public class ProcedureConfiguration implements AlgoBaseConfig, WriteConfig {

    private final CypherMapWrapper configurationMap;

    private final String username;

    public static ProcedureConfiguration empty() {
        return create(CypherMapWrapper.empty(), AuthSubject.ANONYMOUS.username());
    }

    public static ProcedureConfiguration create(Map<String, Object> config, String username) {
        return create(CypherMapWrapper.create(config), username);
    }

    public static ProcedureConfiguration create(CypherMapWrapper map, String username) {
        return new ProcedureConfiguration(map, username);
    }

    protected ProcedureConfiguration(CypherMapWrapper configurationMap, String username) {
        this.configurationMap = configurationMap;
        this.username = username;
    }

    public MemoryEstimation estimate(GraphSetup setup, GraphFactory factory) {
        MemoryEstimation estimation;

        if (containsKey(NODECOUNT_KEY)) {
            GraphDimensions dimensions = factory.dimensions();
            long nodeCount = get(NODECOUNT_KEY, 0L);
            long relCount = get(RELCOUNT_KEY, 0L);

            GraphDimensions estimateDimensions = ImmutableGraphDimensions.builder()
                .from(dimensions)
                .nodeCount(nodeCount)
                .highestNeoId(nodeCount)
                .maxRelCount(relCount)
                .build();

            estimation = factory.memoryEstimation(setup, estimateDimensions);
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

    @Override
    public String username() {
        return username;
    }

    @Override
    public Optional<String> graphName() {
        String graphName = configurationMap.getString(ProcedureConstants.GRAPH_IMPL_KEY, null);
        return Optional.ofNullable(graphName);
    }

    @Override
    public Optional<GraphCreateConfig> implicitCreateConfig() {
        return Optional.empty();
    }

    @Override
    public List<String> relationshipTypes() {
        String relationshipTypes = configurationMap.getString(ProcedureConstants.RELATIONSHIP_TYPES).orElse("");
        Set<String> parsedRelationshipTypes = ProjectionParser.parse(relationshipTypes);
        return new ArrayList<>(parsedRelationshipTypes);
    }

    @Override
    public String writeProperty() {
        return configurationMap.getString(ProcedureConstants.WRITE_PROPERTY_KEY,
            ProcedureConstants.WRITE_PROPERTY_DEFAULT
        );
    }

    @Override
    public int concurrency() {
        int requestedConcurrency = configurationMap
            .getNumber(ProcedureConstants.CONCURRENCY_KEY, Pools.DEFAULT_CONCURRENCY)
            .intValue();
        return Pools.allowedConcurrency(requestedConcurrency);
    }

    @Override
    public int writeConcurrency() {
        Number writeConcurrency = configurationMap.getNumber(
            ProcedureConstants.WRITE_CONCURRENCY_KEY,
            ProcedureConstants.CONCURRENCY_KEY,
            Pools.DEFAULT_CONCURRENCY
        );
        int requestedConcurrency = writeConcurrency.intValue();
        return Pools.allowedConcurrency(requestedConcurrency);
    }

    public int getBatchSize() {
        return configurationMap.getNumber(ProcedureConstants.BATCH_SIZE_KEY, ParallelUtil.DEFAULT_BATCH_SIZE).intValue();
    }

    @Override
    public Collection<String> configKeys() {
        // ProcedureConfig takes ownership of every key
        return configurationMap.toMap().keySet();
    }

    // TODO: get rid of usage in LinkPrediction
    public Direction getDirection(Direction defaultDirection) {
        String direction = configurationMap.get(ProcedureConstants.DIRECTION_KEY, defaultDirection.name());
        return Directions.fromString(direction);
    }

    // TODO: get rid of usage in LinkPrediction
    public RelationshipType getRelationship() {
        return configurationMap.getString(ProcedureConstants.RELATIONSHIP_QUERY_KEY, null) == null
            ? null
            : RelationshipType.withName(configurationMap.getString(ProcedureConstants.RELATIONSHIP_QUERY_KEY, null));
    }
}

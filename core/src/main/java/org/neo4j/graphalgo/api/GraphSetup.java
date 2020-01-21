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
package org.neo4j.graphalgo.api;

import org.jetbrains.annotations.NotNull;
import org.neo4j.graphalgo.PropertyMappings;
import org.neo4j.graphalgo.core.DeduplicationStrategy;
import org.neo4j.graphalgo.core.utils.TerminationFlag;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphdb.Direction;
import org.neo4j.logging.Log;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;

/**
 * DTO to ease the use of the GraphFactory-CTor.
 * Contains setup options for loading the graph from Neo4j.
 */
public interface GraphSetup {

    String username();

    String name();

    int concurrency();

    @NotNull String nodeLabel();

    @NotNull String relationshipType();

    Optional<String> nodeQuery();

    Optional<String> relationshipQuery();

    /**
     * Temporary flag to indicate that the graph needs to be loaded with legacy behaviour.
     */
    @Deprecated
    boolean legacyMode();

    /**
     * @deprecated There is no global direction anymore
     */
    @Deprecated
    Direction direction();

    /**
     * @deprecated There is no global direction anymore
     */
    @Deprecated
    boolean loadIncoming();

    /**
     * @deprecated There is no global direction anymore
     */
    @Deprecated
    boolean loadOutgoing();

    /**
     * @deprecated There is no global direction anymore
     */
    @Deprecated
    boolean loadAsUndirected();

    /**
     * @deprecated There is no global relationship property anymore
     */
    @Deprecated
    Optional<Double> relationshipDefaultPropertyValue();

    /**
     * @deprecated There is no global node property configuration anymore
     */
    @Deprecated
    PropertyMappings nodePropertyMappings();

    /**
     * @deprecated There is no global relationship property configuration anymore
     */
    @Deprecated
    PropertyMappings relationshipPropertyMappings();

    /**
     * @deprecated There is no global relationship deduplication strategy anymore
     */
    @Deprecated
    DeduplicationStrategy deduplicationStrategy();

    Map<String, Object> params();

    Log log();

    long logMillis();

    AllocationTracker tracker();

    TerminationFlag terminationFlag();

    ExecutorService executor();
}

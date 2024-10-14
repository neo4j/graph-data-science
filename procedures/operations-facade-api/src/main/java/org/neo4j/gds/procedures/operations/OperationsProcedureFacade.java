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
package org.neo4j.gds.procedures.operations;

import org.neo4j.gds.core.utils.warnings.UserLogEntry;

import java.util.stream.Stream;

public interface OperationsProcedureFacade {
    void enableAdjacencyCompressionMemoryTracking(boolean value);

    void enableArrowDatabaseImport(boolean value);

    Stream<ProgressResult> listProgress(String jobIdAsString);

    Stream<UserLogEntry> queryUserLog(String jobId);

    Stream<FeatureStringValue> resetAdjacencyPackingStrategy();

    Stream<FeatureState> resetEnableAdjacencyCompressionMemoryTracking();

    Stream<FeatureState> resetEnableArrowDatabaseImport();

    Stream<FeatureLongValue> resetPagesPerThread();

    Stream<FeatureState> resetUseMixedAdjacencyList();

    Stream<FeatureState> resetUsePackedAdjacencyList();

    Stream<FeatureState> resetUseReorderedAdjacencyList();

    Stream<FeatureState> resetUseUncompressedAdjacencyList();

    void setAdjacencyPackingStrategy(String strategyIdentifier);

    void setPagesPerThread(long value);

    void setUseMixedAdjacencyList(boolean value);

    void setUsePackedAdjacencyList(boolean value);

    void setUseReorderedAdjacencyList(boolean value);

    void setUseUncompressedAdjacencyList(boolean value);

}

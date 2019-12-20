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

package org.neo4j.graphalgo.impl.similarity.modern;

import org.immutables.value.Value;
import org.jetbrains.annotations.Nullable;
import org.neo4j.graphalgo.annotation.Configuration;
import org.neo4j.graphalgo.annotation.ValueClass;
import org.neo4j.graphalgo.newapi.AlgoBaseConfig;
import org.neo4j.graphalgo.newapi.WriteConfig;

import java.util.Collections;
import java.util.List;
import java.util.Map;

@ValueClass
@Configuration("ModernCosineConfigImpl")
public interface ModernCosineConfig extends AlgoBaseConfig, WriteConfig {

    int TOP_K_DEFAULT = 0;

    int TOP_N_DEFAULT = 0;

    @Value.Default
    default @Nullable Double skipValue() {
        return Double.NaN;
    }

    @Value.Default
    default String graph() {
        return "dense";
    }

    @Value.Default
    default Object data() {
        return Collections.emptyList();
    }

    @Value.Default
    default Map<String, Object> params() {
        return Collections.emptyMap();
    }

    @Value.Default
    default long degreeCutoff() {
        return 0;
    }

    @Value.Default
    default double similarityCutoff() {
        return -1D;
    }

    @Value.Default
    default int sparseVectorRepeatCutoff() {
        return 3;
    }

    @Value.Default
    default List<Long> sourceIds() {
        return Collections.emptyList();
    }

    @Value.Default
    default List<Long> targetIds() {
        return Collections.emptyList();
    }

    @Value.Default
    default int top() {
        return TOP_N_DEFAULT;
    }

    @Value.Default
    default int topK() {
        return TOP_K_DEFAULT;
    }

    @Value.Default
    default boolean showComputations() {
        return false;
    }

    @Value.Default
    default String writeRelationshipType() {
        return "SIMILAR";
    }

    @Value.Default
    default String writeProperty() {
        return "score";
    }

    @Value.Default
    default long writeBatchSize() {
        return 10_000L;
    }

    @Value.Default
    default boolean write() {
        return false;
    }

}

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
package org.neo4j.graphalgo.core.model;

import org.immutables.value.Value;
import org.neo4j.gds.annotation.ValueClass;
import org.neo4j.graphalgo.api.schema.GraphSchema;
import org.neo4j.graphalgo.config.BaseConfig;
import org.neo4j.graphalgo.config.ModelConfig;
import org.neo4j.graphalgo.core.utils.TimeUtil;

import java.time.ZonedDateTime;
import java.util.List;
import java.util.Map;

@ValueClass
public interface Model<DATA, CONFIG extends ModelConfig & BaseConfig> {

    String ALL_USERS = "*";
    String PUBLIC_MODEL_SUFFIX = "_public";

    String creator();

    List<String> sharedWith();

    String name();

    String algoType();

    GraphSchema graphSchema();

    DATA data();

    CONFIG trainConfig();

    ZonedDateTime creationTime();

    Mappable customInfo();

    @Value.Default
    @Value.Parameter(false)
    default boolean loaded() {
        return true;
    }

    @Value.Default
    @Value.Parameter(false)
    default boolean stored() {
        return false;
    }

    @Value.Default
    @Value.Derived
    default void load() {

    }

    @Value.Default
    @Value.Derived
    default void unload() {
        throw new RuntimeException("Only stored models can be unloaded.");
    }

    default Model<DATA, CONFIG> publish() {
        return ImmutableModel.<DATA, CONFIG>builder()
            .from(this)
            .sharedWith(List.of(ALL_USERS))
            .name(name() + PUBLIC_MODEL_SUFFIX)
            .build();
    }

    static <D, C extends ModelConfig & BaseConfig> Model<D, C> of(
        String creator,
        String name,
        String algoType,
        GraphSchema graphSchema,
        D modelData,
        C trainConfig
    ) {
        return Model.of(
            creator,
            name,
            algoType,
            graphSchema,
            modelData,
            trainConfig,
            Mappable.EMPTY
        );
    }

    static <D, C extends ModelConfig & BaseConfig> Model<D, C> of(
        String creator,
        String name,
        String algoType,
        GraphSchema graphSchema,
        D modelData,
        C trainConfig,
        Mappable customInfo
    ) {
        return ImmutableModel.of(
            creator,
            List.of(),
            name,
            algoType,
            graphSchema,
            modelData,
            trainConfig,
            TimeUtil.now(),
            customInfo
        );
    }

    interface Mappable {
        Map<String, Object> toMap();

        Mappable EMPTY = Map::of;
    }

    class SerializableMappable implements Mappable {
        private final Map<String, Object> map;

        public SerializableMappable(Map<String, Object> map) {this.map = map;}

        @Override
        public Map<String, Object> toMap() {
            return map;
        }
    }
}

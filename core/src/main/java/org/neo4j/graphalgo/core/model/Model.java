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

import org.neo4j.graphalgo.annotation.ValueClass;
import org.neo4j.graphalgo.api.schema.GraphSchema;
import org.neo4j.graphalgo.config.BaseConfig;
import org.neo4j.graphalgo.config.ModelConfig;
import org.neo4j.graphalgo.core.utils.TimeUtil;

import java.time.ZonedDateTime;
import java.util.Map;

@ValueClass
public interface Model<DATA, CONFIG extends ModelConfig & BaseConfig, I extends Model.Mappable> {

    String username();

    String name();

    String algoType();

    GraphSchema graphSchema();

    DATA data();

    CONFIG trainConfig();

    ZonedDateTime creationTime();

    I customInfo();

    static <D, C extends ModelConfig & BaseConfig> Model<D, C, Mappable> of(
        String username,
        String name,
        String algoType,
        GraphSchema graphSchema,
        D modelData,
        C trainConfig
    ) {
        return Model.of(
            username,
            name,
            algoType,
            graphSchema,
            modelData,
            trainConfig,
            Mappable.EMPTY
        );
    }

    static <D, C extends ModelConfig & BaseConfig, I extends Mappable> Model<D, C, I> of(
        String username,
        String name,
        String algoType,
        GraphSchema graphSchema,
        D modelData,
        C trainConfig,
        I customInfo
    ) {
        return ImmutableModel.of(
            username,
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
}

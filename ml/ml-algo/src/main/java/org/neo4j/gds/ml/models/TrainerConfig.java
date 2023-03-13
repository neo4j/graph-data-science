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
package org.neo4j.gds.ml.models;

import org.immutables.value.Value;
import org.neo4j.gds.annotation.Configuration;
import org.neo4j.gds.config.ToMapConvertible;
import org.neo4j.gds.ml.api.TrainingMethod;
import org.neo4j.gds.ml.models.automl.TunableTrainerConfig;

import java.util.HashMap;
import java.util.Map;

public interface TrainerConfig extends ToMapConvertible {

    @Configuration.Ignore
    TrainingMethod method();

    @Value.Derived
    @Configuration.Ignore
    default TunableTrainerConfig toTunableConfig() {
        return TunableTrainerConfig.of(toMap(), method());
    }

    @Configuration.Ignore
    default Map<String, Object> toMapWithTrainerMethod() {
        var mapWithTrainerMethod = new HashMap<>(toMap());
        mapWithTrainerMethod.put("methodName", method().toString());

        return mapWithTrainerMethod;
    }
}

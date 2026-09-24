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
package org.neo4j.gds.pyruntime.graphsage;

import org.neo4j.gds.annotation.Configuration;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

public interface GraphSageSupervisedTrainBaseConfig extends GraphSageBaseTrainConfig {

    String targetLabel();

    String targetProperty();

    @Configuration.IntegerRange(min = 1)
    Optional<Integer> batchSize();

    @Configuration.IntegerRange(min = 0)
    default int epochsPerVal() {
        return 0;
    }

    default Map<String, Double> splitRatios() {
        // insertion-ordered so that the rendered default is stable across JVMs (Map.of order is salted)
        var splitRatios = new LinkedHashMap<String, Double>();
        splitRatios.put("TRAIN", 0.6);
        splitRatios.put("TEST", 0.2);
        splitRatios.put("VALID", 0.2);
        return splitRatios;
    }

    default boolean classWeights() {
        return false;
    }
}

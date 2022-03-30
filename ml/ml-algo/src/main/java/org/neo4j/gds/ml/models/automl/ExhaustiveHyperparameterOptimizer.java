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
package org.neo4j.gds.ml.models.automl;

import org.neo4j.gds.ml.models.TrainerConfig;
import org.neo4j.gds.ml.models.TrainingMethod;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

// this is temporary until we have a better optimizer
public class ExhaustiveHyperparameterOptimizer implements HyperParameterOptimizer {
    private final Iterator<TrainerConfig> parameterSpaceIterator;

    public ExhaustiveHyperparameterOptimizer(Map<TrainingMethod, List<TunableTrainerConfig>> parameterSpace) {
        this.parameterSpaceIterator = parameterSpace
            .values()
            .stream()
            .flatMap(List::stream)
            .map(tunableConfig ->
                tunableConfig.trainingMethod().createConfig(tunableConfig.value)
            )
            .collect(Collectors.toList())
            .iterator();
    }

    @Override
    public boolean hasNext() {
        return parameterSpaceIterator.hasNext();
    }

    @Override
    public TrainerConfig next() {
        return parameterSpaceIterator.next();
    }
}

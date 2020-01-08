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
package positive;

import org.eclipse.collections.api.tuple.Pair;
import org.eclipse.collections.impl.tuple.Tuples;
import org.jetbrains.annotations.NotNull;
import org.neo4j.graphalgo.core.CypherMapWrapper;

import javax.annotation.processing.Generated;

@Generated("org.neo4j.graphalgo.proc.ConfigurationProcessor")
public final class ParametersConfig implements Parameters {

    private final int keyFromParameter;

    private final long keyFromMap;

    private final int parametersAreAddedFirst;

    public ParametersConfig(int keyFromParameter, int parametersAreAddedFirst, @NotNull CypherMapWrapper config) {
        this.keyFromParameter = keyFromParameter;
        this.keyFromMap = config.requireLong("keyFromMap");
        this.parametersAreAddedFirst = parametersAreAddedFirst;
    }

    public static Pair<Parameters, CypherMapWrapper> of(
        int keyFromParameter,
        int parametersAreAddedFirst,
        @NotNull CypherMapWrapper config
    ) {
        Parameters instance = new ParametersConfig(keyFromParameter, parametersAreAddedFirst, config);
        CypherMapWrapper newConfig = config.withoutAny("keyFromMap");
        return Tuples.pair(instance, newConfig);
    }

    @Override
    public int keyFromParameter() {
        return this.keyFromParameter;
    }

    @Override
    public long keyFromMap() {
        return this.keyFromMap;
    }

    @Override
    public int parametersAreAddedFirst() {
        return this.parametersAreAddedFirst;
    }
}

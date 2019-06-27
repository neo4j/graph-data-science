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
package org.neo4j.graphalgo.impl;

import org.neo4j.graphalgo.Algorithm;

import java.util.Objects;

public final class AlgoWithConfig<A extends Algorithm<A>, Conf> {
    private final A algo;
    private final Conf conf;

    private AlgoWithConfig(final A algo, final Conf conf) {
        this.algo = Objects.requireNonNull(algo);
        this.conf = Objects.requireNonNull(conf);
    }

    public static <Conf, A extends Algorithm<A>> AlgoWithConfig<A, Conf> of(final A algo, final Conf conf) {
        return new AlgoWithConfig<>(algo, conf);
    }

    public A algo() {
        return algo;
    }

    public Conf conf() {
        return conf;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final AlgoWithConfig<?, ?> that = (AlgoWithConfig<?, ?>) o;
        return algo.equals(that.algo) &&
                conf.equals(that.conf);
    }

    @Override
    public int hashCode() {
        return Objects.hash(algo, conf);
    }

    @Override
    public String toString() {
        return "AlgoWithConfig{" +
                "algo=" + algo +
                ", conf=" + conf +
                '}';
    }
}

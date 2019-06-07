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

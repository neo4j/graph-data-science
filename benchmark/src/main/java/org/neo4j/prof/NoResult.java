package org.neo4j.prof;

import org.openjdk.jmh.results.AggregationPolicy;
import org.openjdk.jmh.results.Aggregator;
import org.openjdk.jmh.results.Result;
import org.openjdk.jmh.results.ResultRole;
import org.openjdk.jmh.util.SingletonStatistics;

import java.util.Collection;
import java.util.stream.Collectors;

final class NoResult extends Result<NoResult> {

    private final String label;
    private final String output;

    NoResult(final String label, final String output) {
        super(
                ResultRole.SECONDARY,
                label,
                new SingletonStatistics(Double.NaN),
                "N/A",
                AggregationPolicy.SUM);
        this.label = label;
        this.output = output;
    }

    String output() {
        return output;
    }

    @Override
    protected Aggregator<NoResult> getThreadAggregator() {
        return new NoResultAggregator();
    }

    @Override
    protected Aggregator<NoResult> getIterationAggregator() {
        return new NoResultAggregator();
    }

    @Override
    public String extendedInfo() {
        return output;
    }

    private static final class NoResultAggregator implements Aggregator<NoResult> {
        @Override
        public NoResult aggregate(final Collection<NoResult> results) {
            return new NoResult(
                    results.iterator().next().label,
                    results.stream()
                            .map(NoResult::output)
                            .collect(Collectors.joining(System.lineSeparator()))
            );
        }
    }
}

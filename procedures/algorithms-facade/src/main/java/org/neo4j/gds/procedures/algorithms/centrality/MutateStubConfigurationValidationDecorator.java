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
package org.neo4j.gds.procedures.algorithms.centrality;

import org.neo4j.gds.applications.algorithms.machinery.MemoryEstimateResult;
import org.neo4j.gds.mem.MemoryEstimation;
import org.neo4j.gds.procedures.algorithms.stubs.MutateStub;

import java.util.Map;
import java.util.stream.Stream;

/**
 * A decorator that validates a property before delegating
 */
class MutateStubConfigurationValidationDecorator<CONFIGURATION, RESULT> implements MutateStub<CONFIGURATION, RESULT> {
    private final MutateStub<CONFIGURATION, RESULT> delegate;
    private final String key;

    MutateStubConfigurationValidationDecorator(MutateStub<CONFIGURATION, RESULT> delegate, String key) {
        this.delegate = delegate;
        this.key = key;
    }

    @Override
    public CONFIGURATION parseConfiguration(Map<String, Object> configuration) {
        validate(configuration);

        return delegate.parseConfiguration(configuration);
    }

    @Override
    public MemoryEstimation getMemoryEstimation(String username, Map<String, Object> configuration) {
        validate(configuration);

        return delegate.getMemoryEstimation(username, configuration);
    }

    @Override
    public Stream<MemoryEstimateResult> estimate(Object graphName, Map<String, Object> configuration) {
        validate(configuration);

        return delegate.estimate(graphName, configuration);
    }

    @Override
    public Stream<RESULT> execute(String graphName, Map<String, Object> configuration) {
        validate(configuration);

        return delegate.execute(graphName, configuration);
    }

    private void validate(Map<String, Object> configuration) {
        if (configuration.containsKey(key)) {
            throw new IllegalArgumentException("Unexpected configuration key: " + key);
        }
    }
}

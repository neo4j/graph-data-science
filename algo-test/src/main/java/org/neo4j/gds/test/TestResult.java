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
package org.neo4j.gds.test;

import org.neo4j.gds.result.AbstractResultBuilder;

import java.util.Map;

public final class TestResult {

    public long preProcessingMillis;
    public long computeMillis;
    public long relationshipCount;
    public Map<String, Object> configuration;

    TestResult(long preProcessingMillis, long computeMillis, long relationshipCount, Map<String, Object> configuration) {
        this.preProcessingMillis = preProcessingMillis;
        this.computeMillis = computeMillis;
        this.relationshipCount = relationshipCount;
        this.configuration = configuration;
    }

    public static class TestResultBuilder extends AbstractResultBuilder<TestResult> {

        long relationshipCount = 0;

        @Override
        public TestResult build() {
            return new TestResult(
                preProcessingMillis,
                computeMillis,
                relationshipCount,
                config.toMap()
            );
        }

        public TestResultBuilder withRelationshipCount(long relationshipCount) {
            this.relationshipCount = relationshipCount;
            return this;
        }
    }
}

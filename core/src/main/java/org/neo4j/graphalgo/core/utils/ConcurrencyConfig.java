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
package org.neo4j.graphalgo.core.utils;

final class ConcurrencyConfig {

    private static final String PROCESSORS_OVERRIDE_PROPERTY = "neo4j.graphalgo.processors";
    private static final int MAX_CE_CONCURRENCY = 4;

    final int maxConcurrency;
    final int defaultConcurrency;

    static ConcurrencyConfig of() {
        Integer definedProcessors = null;
        try {
            definedProcessors = Integer.getInteger(PROCESSORS_OVERRIDE_PROPERTY);
        } catch (SecurityException ignored) {
        }
        if (definedProcessors == null) {
            definedProcessors = Runtime.getRuntime().availableProcessors();
        }
        boolean isOnEnterprise = Package.getPackage("org.neo4j.kernel.impl.enterprise") != null;
        return new ConcurrencyConfig(definedProcessors, isOnEnterprise);
    }

    /* test-private */ ConcurrencyConfig(int availableProcessors, boolean isOnEnterprise) {
        if (isOnEnterprise) {
            maxConcurrency = Integer.MAX_VALUE;
            defaultConcurrency = availableProcessors;
        } else {
            maxConcurrency = MAX_CE_CONCURRENCY;
            defaultConcurrency = Math.min(availableProcessors, MAX_CE_CONCURRENCY);
        }
    }
}

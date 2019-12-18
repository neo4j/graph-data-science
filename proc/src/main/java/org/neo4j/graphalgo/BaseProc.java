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
package org.neo4j.graphalgo;

import org.neo4j.graphalgo.core.ImmutableModernGraphLoader;
import org.neo4j.graphalgo.core.ModernGraphLoader;
import org.neo4j.graphalgo.core.utils.TerminationFlag;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.newapi.GraphCreateConfig;
import org.neo4j.internal.kernel.api.procs.ProcedureCallContext;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;

import java.util.Collection;
import java.util.function.Supplier;
import java.util.regex.Pattern;

public abstract class BaseProc {

    @Context
    public GraphDatabaseAPI api;

    @Context
    public Log log;

    @Context
    public KernelTransaction transaction;

    @Context
    public ProcedureCallContext callContext;

    protected String getUsername() {
        return transaction.subjectOrAnonymous().username();
    }


    protected final ModernGraphLoader newLoader(
        AllocationTracker tracker,
        GraphCreateConfig config
    ) {
        return ImmutableModernGraphLoader
            .builder()
            .api(api)
            .log(log)
            .username(getUsername())
            .tracker(tracker)
            .terminationFlag(TerminationFlag.wrap(transaction))
            .createConfig(config)
            .build();
    }

    protected void runWithExceptionLogging(String message, Runnable runnable) {
        try {
            runnable.run();
        } catch (Exception e) {
            log.debug(message, e);
            throw e;
        }
    }

    protected <R> R runWithExceptionLogging(String message, Supplier<R> supplier) {
        try {
            return supplier.get();
        } catch (Exception e) {
            log.debug(message, e);
            throw e;
        }
    }

    static final class OutputFieldParser {
        private static final Pattern PERCENTILE_FIELD_REGEXP = Pattern.compile("^p\\d{1,3}$");
        private static final Pattern COMMUNITY_COUNT_REGEXP = Pattern.compile("^(community|set)Count$");

        private OutputFieldParser() {}

        static boolean computeHistogram(Collection<String> returnItems) {
            return returnItems.stream().anyMatch(PERCENTILE_FIELD_REGEXP.asPredicate());
        }

        static boolean computeCommunityCount(Collection<String> returnItems) {
            return computeHistogram(returnItems) || returnItems.stream().anyMatch(COMMUNITY_COUNT_REGEXP.asPredicate());
        }
    }

}

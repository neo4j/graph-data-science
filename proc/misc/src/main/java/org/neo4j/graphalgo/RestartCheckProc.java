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
package org.neo4j.graphalgo;

import org.neo4j.graphalgo.core.loading.GraphStoreCatalog;
import org.neo4j.graphalgo.core.model.ModelCatalog;
import org.neo4j.graphalgo.core.utils.progress.ProgressEventStore;
import org.neo4j.procedure.Admin;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Internal;
import org.neo4j.procedure.Procedure;

import java.util.stream.Stream;

public class RestartCheckProc {

    @Context
    public ProgressEventStore progress;

    @Admin
    @Internal
    @Procedure("gds.internal.safeToRestart")
    public Stream<SafeToRestartResult> safeToRestart() {
        return Stream.of(new SafeToRestartResult(compute()));
    }

    private boolean compute() {
        return GraphStoreCatalog.isEmpty() && ModelCatalog.isEmpty() && progress.isEmpty();
    }

    public static final class SafeToRestartResult {
        public final boolean safeToRestart;

        SafeToRestartResult(boolean safeToRestart) {
            this.safeToRestart = safeToRestart;
        }
    }
}

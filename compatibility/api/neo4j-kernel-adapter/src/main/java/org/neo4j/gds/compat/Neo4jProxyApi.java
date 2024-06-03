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
package org.neo4j.gds.compat;

import org.neo4j.configuration.Config;
import org.neo4j.internal.batchimport.AdditionalInitialIds;
import org.neo4j.internal.batchimport.BatchImporter;
import org.neo4j.internal.batchimport.Configuration;
import org.neo4j.internal.batchimport.input.Collector;
import org.neo4j.internal.kernel.api.Read;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.api.procedure.GlobalProcedures;
import org.neo4j.logging.internal.LogService;
import org.neo4j.scheduler.JobScheduler;

public interface Neo4jProxyApi {

    @CompatSince(Neo4jVersion.V_5_17)
    long estimateNodeCount(Read read, int label);

    @CompatSince(Neo4jVersion.V_5_17)
    long estimateRelationshipCount(Read read, int sourceLabel, int targetLabel, int type);

    @CompatSince(Neo4jVersion.V_5_18)
    default BatchImporter instantiateBlockBatchImporter(
        DatabaseLayout directoryStructure,
        FileSystemAbstraction fileSystem,
        PageCacheTracer pageCacheTracer,
        Configuration configuration,
        CompatMonitor compatMonitor,
        LogService logService,
        AdditionalInitialIds additionalInitialIds,
        Config dbConfig,
        JobScheduler jobScheduler,
        Collector badCollector
    ) {
        throw new UnsupportedOperationException("GDS does not support block store format batch importer on this Neo4j version. Requires >= Neo4j 5.18.");
    }

    GlobalProcedureRegistry globalProcedureRegistry(GlobalProcedures globalProcedures);
}

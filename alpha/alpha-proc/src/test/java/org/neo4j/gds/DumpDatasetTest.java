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
package org.neo4j.gds;

import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.neo4j.cli.AdminTool;
import org.neo4j.cli.ExecutionContext;
import org.neo4j.dbms.api.DatabaseManagementServiceBuilder;
import org.neo4j.gds.compat.GdsGraphDatabaseAPI;
import org.neo4j.gds.compat.Neo4jProxy;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.GraphStore;
import org.neo4j.graphalgo.compat.GraphDatabaseApiProxy;
import org.neo4j.graphalgo.core.Settings;
import org.neo4j.graphalgo.core.loading.GraphStoreCatalog;
import org.neo4j.graphalgo.datasets.CommunityDbCreator;
import org.neo4j.graphdb.ResultTransformer;
import org.neo4j.kernel.api.procedure.GlobalProcedures;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

abstract class DumpDatasetTest {
    abstract String dumpPath();
    abstract List<Class> procedureAndFunctionClasses();

    protected Path dbPath;
    protected GdsGraphDatabaseAPI api;

    protected GraphStore graphStore(String graphName) {
        return GraphStoreCatalog.get("", api.databaseId(), graphName).graphStore();
    }

    protected Graph graph(String graphName) {
        return graphStore(graphName).getUnion();
    }

    protected void runQuery(String query) {
        api.executeTransactionally(query, Map.of(), r-> {return true;});
    }

    protected void runQuery(String query, ResultTransformer<Object> transformer) {
        api.executeTransactionally(query, Map.of(), transformer);
    }

    protected void runQuery(String query, Map<String, Object> parameters, ResultTransformer<Object> transformer) {
        api.executeTransactionally(query, parameters, transformer);
    }

    @BeforeEach
    void setup() {
        api = open(dumpPath());
    }

    @AfterEach
    void tearDown() {
        api.shutdown();
        try {
            FileUtils.deleteDirectory(dbPath.toFile());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private GdsGraphDatabaseAPI open(String dumpPath) {
        var creator = CommunityDbCreator.getInstance();
        createEmptyDb(creator);
        runLoadCommand(dumpPath);
        return makeDbmsAndApi();
    }

    private GdsGraphDatabaseAPI makeDbmsAndApi() {
        var dbms = new DatabaseManagementServiceBuilder(dbPath.toFile())
            .setConfig(Settings.procedureUnrestricted(), List.of("gds.*"))
            .setConfig(Settings.udc(), false)
            .setConfig(Settings.boltEnabled(), false)
            .setConfig(Settings.httpEnabled(), false)
            .setConfig(Settings.httpsEnabled(), false)
            .setConfigRaw(Map.of(
                "dbms.allow_upgrade", "true"
            ))
            .build();
        api = Neo4jProxy.newDb(dbms);
        GlobalProcedures proceduresService = GraphDatabaseApiProxy.resolveDependency(api, GlobalProcedures.class);

        registerProcedures(proceduresService);
        registerFunctions(proceduresService);
        return api;
    }

    private void registerProcedures(GlobalProcedures proceduresService) {
        procedureAndFunctionClasses()
            .forEach(procedureClass -> {
                try {
                    proceduresService.registerProcedure(procedureClass);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
    }

    private void registerFunctions(GlobalProcedures proceduresService) {
        procedureAndFunctionClasses()
            .forEach(functionClass -> {
                try {
                    proceduresService.registerFunction(functionClass);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
    }

    private void runLoadCommand(String dumpPath) {
        final var ctx = new ExecutionContext( dbPath, dbPath.resolve("conf") );
        try {
            var confDir = dbPath.resolve("conf");
            confDir.toFile().mkdirs();
            confDir.resolve("neo4j.conf").toFile().createNewFile();
        } catch (IOException e) {
            e.printStackTrace();
        }
        var execute = AdminTool.execute(ctx, new String[]{"load", "--from=" + dumpPath, "--database=neo4j", "--force"});
        if (execute != 0) {
            throw new RuntimeException("Load command failed");
        }
    }

    private void createEmptyDb(CommunityDbCreator creator) {
        Path datasetDir = null;
        try {
            dbPath = Files.createTempDirectory("testDumpDb");
            datasetDir = dbPath.toAbsolutePath();
            Files.createDirectories(datasetDir);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        var db = creator.createEmbeddedDatabase(datasetDir);
        db.shutdown();
    }

}

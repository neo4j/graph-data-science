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
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.neo4j.cli.AdminTool;
import org.neo4j.cli.ExecutionContext;
import org.neo4j.dbms.api.DatabaseManagementServiceBuilder;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.GraphStore;
import org.neo4j.graphalgo.catalog.GraphCreateProc;
import org.neo4j.graphalgo.compat.GdsGraphDatabaseAPI;
import org.neo4j.graphalgo.compat.Neo4jProxy;
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

class DumpDatasetTest {
    private static final String DUMP_PATH = "INSERT_PATH_HERE";
    // Fill in the classes you need
    private static final List<Class> PROC_CLASSES = List.of(GraphCreateProc.class);

    private static Path dbPath;
    private static GdsGraphDatabaseAPI api;

    void test() {
        String createQuery = "CALL gds.graph.create(" +
                             "'all_person', " +
                             "    {" +
                             "      Person: {label: 'Person', properties: ['title_int', 'age', 'gender_int', 'death_year']}" +
                             "    }," +
                             "    {" +
                             "      relType: {type: '*', orientation: 'UNDIRECTED', properties: ['weight']}" +
                             "    }" +
                             ")";

        runQuery(createQuery, r -> {
            System.out.println(r.resultAsString());
            return true;
        });
    }

    private GraphStore graphStore(String graphName) {
        return GraphStoreCatalog.get("", api.databaseId(), graphName).graphStore();
    }

    private Graph graph(String graphName) {
        return graphStore(graphName).getUnion();
    }

    private void runQuery(String query) {
        api.executeTransactionally(query, Map.of(), r-> {return true;});
    }

    private void runQuery(String query, ResultTransformer<Object> transformer) {
        api.executeTransactionally(query, Map.of(), transformer);
    }

    private void runQuery(String query, Map<String, Object> parameters, ResultTransformer<Object> transformer) {
        api.executeTransactionally(query, parameters, transformer);
    }

    @BeforeAll
    static void setup() {
        api = open(DUMP_PATH);
    }

    @AfterAll
    static void tearDown() {
        api.shutdown();
        try {
            FileUtils.deleteDirectory(dbPath.toFile());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static GdsGraphDatabaseAPI open(String dumpPath) {
        var creator = CommunityDbCreator.getInstance();
        createEmptyDb(creator);
        runLoadCommand(dumpPath);
        return makeDbmsAndApi();
    }

    private static GdsGraphDatabaseAPI makeDbmsAndApi() {
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
        GlobalProcedures proceduresService = api
            .getDependencyResolver()
            .resolveDependency(GlobalProcedures.class);

        registerProcedures(proceduresService);
        registerFunctions(proceduresService);
        return api;
    }

    private static void registerProcedures(GlobalProcedures proceduresService) {
        PROC_CLASSES
            .forEach(procedureClass -> {
                try {
                    proceduresService.registerProcedure(procedureClass);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
    }

    private static void registerFunctions(GlobalProcedures proceduresService) {
        PROC_CLASSES
            .forEach(functionClass -> {
                try {
                    proceduresService.registerFunction(functionClass);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
    }

    private static void runLoadCommand(String dumpPath) {
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

    private static void createEmptyDb(CommunityDbCreator creator) {
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

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
package org.neo4j.graphalgo;

import org.neo4j.graphalgo.core.concurrency.ConcurrencyControllerExtension;
import org.neo4j.graphdb.factory.GraphDatabaseBuilder;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.LogProvider;
import org.neo4j.test.TestGraphDatabaseFactory;

import java.io.File;
import java.util.UUID;

<<<<<<< HEAD:public/compatibility/3.5/test-utils/src/main/java/org/neo4j/graphalgo/TestDatabaseCreator.java
import static org.neo4j.graphalgo.config.ConcurrencyValidation.CORE_LIMITATION_SETTING;

public final class TestDatabaseCreator {

    private TestDatabaseCreator() {}
||||||| parent of 24632e814... Split test-utils out of compat:public/compatibility/3.5/test-utils/src/main/java/org/neo4j/graphalgo/TestDatabaseCreator.java
public class TestDatabaseCreator {
=======
public final class TestDatabaseCreator {
>>>>>>> 24632e814... Split test-utils out of compat:public/compatibility/3.5/test-utils-adapter/src/main/java/org/neo4j/graphalgo/TestDatabaseCreator.java

<<<<<<< HEAD:public/compatibility/3.5/test-utils/src/main/java/org/neo4j/graphalgo/TestDatabaseCreator.java
    public static GraphDatabaseAPI createTestDatabase() {
        return (GraphDatabaseAPI) dbBuilder().setConfig(GraphDatabaseSettings.procedure_unrestricted, "gds.*")
||||||| parent of 24632e814... Split test-utils out of compat:public/compatibility/3.5/test-utils/src/main/java/org/neo4j/graphalgo/TestDatabaseCreator.java
    public static GraphDatabaseAPI createTestDatabase() {
        return (GraphDatabaseAPI) new TestGraphDatabaseFactory()
            .newImpermanentDatabaseBuilder(new File(UUID.randomUUID().toString()))
            .setConfig(GraphDatabaseSettings.procedure_unrestricted, "gds.*")
=======
    public static TestDatabaseApi createTestDatabase() {
        return new TestDatabaseApi(createDefault());
    }

    public static GraphDatabaseAPI createTestDatabaseWithCustomLoadCsvRoot(String value) {
        return new TestDatabaseApi(createWithCustomLoadCsvRoot(value));
    }

    public static GraphDatabaseAPI createTestDatabase(LogProvider logProvider) {
        return new TestDatabaseApi(createWithLogger(logProvider));
    }

    public static GraphDatabaseAPI createTestDatabase(File storeDir) {
        return new TestDatabaseApi(createEmbedded(storeDir));
    }

    private static GraphDatabaseAPI createDefault() {
        return (GraphDatabaseAPI) builder()
            .setConfig(GraphDatabaseSettings.procedure_unrestricted, "gds.*")
>>>>>>> 24632e814... Split test-utils out of compat:public/compatibility/3.5/test-utils-adapter/src/main/java/org/neo4j/graphalgo/TestDatabaseCreator.java
            .newGraphDatabase();
    }

    private static GraphDatabaseAPI createWithCustomLoadCsvRoot(String value) {
        return (GraphDatabaseAPI) builder()
            .setConfig(GraphDatabaseSettings.load_csv_file_url_root, value)
            .setConfig(GraphDatabaseSettings.procedure_unrestricted, "gds.*")
            .newGraphDatabase();
    }

<<<<<<< HEAD:public/compatibility/3.5/test-utils/src/main/java/org/neo4j/graphalgo/TestDatabaseCreator.java
    public static GraphDatabaseAPI createTestDatabase(Consumer<GraphDatabaseBuilder> configuration) {
        GraphDatabaseBuilder builder = dbBuilder();
        configuration.accept(builder);
        return (GraphDatabaseAPI) builder.newGraphDatabase();
    }

    public static GraphDatabaseAPI createUnlimitedConcurrencyTestDatabase() {
        return createTestDatabase(builder -> builder.setConfig(CORE_LIMITATION_SETTING, "true"));
    }

    public static GraphDatabaseAPI createTestDatabase(LogProvider logProvider) {
||||||| parent of 24632e814... Split test-utils out of compat:public/compatibility/3.5/test-utils/src/main/java/org/neo4j/graphalgo/TestDatabaseCreator.java
    public static GraphDatabaseAPI createTestDatabase(LogProvider logProvider) {
=======
    private static GraphDatabaseAPI createWithLogger(LogProvider logProvider) {
>>>>>>> 24632e814... Split test-utils out of compat:public/compatibility/3.5/test-utils-adapter/src/main/java/org/neo4j/graphalgo/TestDatabaseCreator.java
        return (GraphDatabaseAPI) new TestGraphDatabaseFactory(logProvider)
            .newImpermanentDatabaseBuilder(new File(UUID.randomUUID().toString()))
            .newGraphDatabase();
    }

    private static GraphDatabaseAPI createEmbedded(File storeDir) {
        return (GraphDatabaseAPI) new TestGraphDatabaseFactory().newEmbeddedDatabase(storeDir);
    }
<<<<<<< HEAD:public/compatibility/3.5/test-utils/src/main/java/org/neo4j/graphalgo/TestDatabaseCreator.java

    private static GraphDatabaseBuilder dbBuilder() {
        return dbBuilder(new File(UUID.randomUUID().toString()));
    }

    private static GraphDatabaseBuilder dbBuilder(File storeDir) {
        return new TestGraphDatabaseFactory()
            .addKernelExtension(new ConcurrencyControllerExtension())
            .newImpermanentDatabaseBuilder(storeDir);
    }

||||||| parent of 24632e814... Split test-utils out of compat:public/compatibility/3.5/test-utils/src/main/java/org/neo4j/graphalgo/TestDatabaseCreator.java
=======

    private static GraphDatabaseBuilder builder() {
        return new TestGraphDatabaseFactory()
            .newImpermanentDatabaseBuilder(new File(UUID.randomUUID().toString()));
    }

    private TestDatabaseCreator() {}
>>>>>>> 24632e814... Split test-utils out of compat:public/compatibility/3.5/test-utils-adapter/src/main/java/org/neo4j/graphalgo/TestDatabaseCreator.java
}

/*
 * Copyright (c) 2017 "Neo4j, Inc." <http://neo4j.com>
 *
 * This file is part of Neo4j Graph Algorithms <http://github.com/neo4j-contrib/neo4j-graph-algorithms>.
 *
 * Neo4j Graph Algorithms is free software: you can redistribute it and/or modify
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
package org.neo4j.graphalgo.bench;

import com.carrotsearch.hppc.ObjectLongHashMap;
import com.carrotsearch.hppc.ObjectLongMap;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.kernel.api.TokenWrite;
import org.neo4j.internal.kernel.api.Write;
import org.neo4j.internal.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
public class HeroGraph extends BaseBenchmark {

    private static final String DATA_URL = "https://raw.githubusercontent.com/tomasonjo/neo4j-marvel/d3c39a8cc97f3e89373cd6d065d7e44e0236694e/data/edges.csv";

    public HeroGraph() {
        db = createDb();
    }

    private GraphDatabaseAPI createDb() {
        try {
            return mkDb();
        } catch (KernelException | IOException e) {
            throw new RuntimeException(e);
        }
    }

    private GraphDatabaseAPI mkDb() throws IOException, KernelException {
        System.out.print("Loading graph from " + DATA_URL + " ... ");
        URL url = new URL(DATA_URL);

        GraphDatabaseAPI db = (GraphDatabaseAPI)
                new TestGraphDatabaseFactory()
                        .newImpermanentDatabaseBuilder()
                        .newGraphDatabase();
        ThreadToStatementContextBridge bridge = resolveDependency(ThreadToStatementContextBridge.class);

        long rels = 0;
        long nodes = 0;
        long startTime = System.nanoTime();

        try (Transaction tx = db.beginTx();
             KernelTransaction statement = bridge.getKernelTransactionBoundToThisThread(true);
             BufferedReader reader = new BufferedReader(
                     new InputStreamReader(
                             new BufferedInputStream(url.openStream()),
                             StandardCharsets.UTF_8))) {

            TokenWrite tokens = statement.tokenWrite();
            int hero = tokens.labelGetOrCreateForName("Hero");
            int comic = tokens.labelGetOrCreateForName("Comic");
            int appearedIn = tokens.relationshipTypeGetOrCreateForName(
                    "APPEARED_IN");

            ObjectLongMap<String> heroIds = new ObjectLongHashMap<>();
            ObjectLongMap<String> comicIds = new ObjectLongHashMap<>();

            Write write = statement.dataWrite();

            // headers
            reader.readLine();

            String line, heroName, comicName;
            while ((line = reader.readLine()) != null) {
                if (line.startsWith("\"")) {
                    int sep = line.indexOf('"', 1);
                    heroName = line.substring(1, sep);
                    comicName = line.substring(sep + 2);
                } else {
                    int sep = line.indexOf(',');
                    heroName = line.substring(0, sep);
                    comicName = line.substring(sep + 1);
                }

                long heroId = heroIds.getOrDefault(heroName, -1L);
                if (heroId == -1L) {
                    heroId = write.nodeCreate();
                    write.nodeAddLabel(heroId, hero);
                    heroIds.put(heroName, heroId);
                    ++nodes;
                }

                long comicId = comicIds.getOrDefault(comicName, -1L);
                if (comicId == -1L) {
                    comicId = write.nodeCreate();
                    write.nodeAddLabel(comicId, comic);
                    comicIds.put(comicName, comicId);
                    ++nodes;
                }

                write.relationshipCreate(heroId, appearedIn, comicId);
                ++rels;
            }
            tx.success();
        }

        long took = System.nanoTime() - startTime;
        System.out.println("Created " + nodes + " Nodes and " + rels + " Relationships in " + TimeUnit.NANOSECONDS.toMillis(took) + " ms");

        return db;
    }
}

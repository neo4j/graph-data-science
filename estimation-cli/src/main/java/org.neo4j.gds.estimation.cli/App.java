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
package org.neo4j.gds.estimation.cli;

import picocli.CommandLine;

import java.util.concurrent.Callable;

@CommandLine.Command(description = "Estimates the memory consumption of a GDS procedure.",
    name = "checksum", mixinStandardHelpOptions = true, version = "checksum 3.0")
public class App implements Callable<Integer> {

    @CommandLine.Option(
        names = {"-p", "--procedure"},
        description = "Procedure call, e.g. gds.pagerank.stream, gds.wcc.write, ...",
        split = ","
    )
    private String[] procedures;

    public static void main(String... args) {
        int exitCode = new CommandLine(new App()).execute(args);
        System.exit(exitCode);
    }

    @Override
    public Integer call() {
        System.out.println("procedures = " + procedures);
        return 0;
    }
}

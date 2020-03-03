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
package org.neo4j.graphalgo.doc;

import org.asciidoctor.ast.Cell;
import org.asciidoctor.ast.Document;
import org.asciidoctor.ast.Row;
import org.asciidoctor.ast.StructuralNode;
import org.asciidoctor.ast.Table;
import org.asciidoctor.extension.Treeprocessor;

import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

public class AppendixAProcedureListingProcessor extends Treeprocessor {

    private static final String PROCEDURE_LISTING_ROLE = "procedure-listing";

    private final List<String> procedures;

    public AppendixAProcedureListingProcessor() {
        // Use `List` instead of `Set` because we have procedures and functions that have the same name and namespace,
        // i.e. `gds.graph.exists`
        procedures = new LinkedList<>();
    }

    @Override
    public Document process(Document document) {
        extractProcedures(document);
        return document;
    }

    private void extractProcedures(StructuralNode document) {

        List<Table> tables = findProcedureListings(document);

        tables.forEach(table -> {
            table.getBody().forEach(row -> {
                Cell cell = getProcedureListingCell(row);
                String procedureName = getProcedureName(cell);
                procedures.add(procedureName);
            });
        });
    }

    private String getProcedureName(Cell cell) {
        return cell.getContent().toString().replaceAll("<[^>]*>|\\[\"|\"]", "");
    }

    private Cell getProcedureListingCell(Row row) {
        List<Cell> cells = row.getCells();
        Cell cell;
        if (cells.size() == 2) {
            cell = cells.get(1);
        } else {
            cell = cells.get(0); // This is needed to handle the row span formatting.
        }
        return cell;
    }

    private List<Table> findProcedureListings(StructuralNode document) {
        return document
            .getBlocks()
            .stream()
            .flatMap(it -> it.getBlocks().stream())
            .filter(it -> it instanceof Table)
            .filter(it -> it.getRoles().contains(PROCEDURE_LISTING_ROLE))
            .map(it -> (Table)it)
            .collect(Collectors.toList());
    }

    List<String> procedures() {
        return procedures;
    }
}

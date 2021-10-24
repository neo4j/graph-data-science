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
package org.neo4j.gds.collections;

import java.util.concurrent.atomic.AtomicInteger;

public final class DrainingIterator<PAGE> {
    private final PAGE[] pages;
    private final int pageSize;
    private final AtomicInteger globalPageId;

    DrainingIterator(PAGE[] pages, int pageSize) {
        this.pages = pages;
        this.pageSize = pageSize;
        this.globalPageId = new AtomicInteger(0);
    }

    public DrainingBatch<PAGE> drainingBatch() {
        return new DrainingBatch<>();
    }

    public boolean next(DrainingBatch<PAGE> reuseBatch) {
        int nextPageId = 0;
        PAGE nextPage = null;

        while (nextPage == null) {
            nextPageId = globalPageId.getAndIncrement();

            if (nextPageId >= pages.length) {
                return false;
            }

            nextPage = pages[nextPageId];
        }

        // drain: clear the reference to the page
        pages[nextPageId] = null;

        reuseBatch.reset(nextPage, (long) nextPageId * pageSize);

        return true;
    }

    public static class DrainingBatch<PAGE> {

        public PAGE page;
        public long offset;

        void reset(PAGE page, long offset) {
            this.page = page;
            this.offset = offset;
        }
    }
}

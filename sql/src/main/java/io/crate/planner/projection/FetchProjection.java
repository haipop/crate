/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial agreement.
 */

package io.crate.planner.projection;

import com.carrotsearch.hppc.IntSet;
import com.google.common.collect.ImmutableMap;
import io.crate.analyze.symbol.Symbol;
import io.crate.collections.Lists2;
import io.crate.metadata.TableIdent;
import io.crate.operation.Paging;
import io.crate.planner.ExplainLeaf;
import io.crate.planner.node.fetch.FetchSource;
import io.crate.types.DataType;
import io.crate.types.FixedWidthType;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.monitor.jvm.JvmInfo;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.Function;

public class FetchProjection extends Projection {

    private final int collectPhaseId;
    private final int fetchSize;
    private final Map<TableIdent, FetchSource> fetchSources;
    private final List<Symbol> outputSymbols;
    private final Map<String, IntSet> nodeReaders;
    private final TreeMap<Integer, String> readerIndices;
    private final Map<String, TableIdent> indicesToIdents;

    public FetchProjection(int collectPhaseId,
                           int fetchSize,
                           Map<TableIdent, FetchSource> fetchSources,
                           List<Symbol> outputSymbols,
                           Map<String, IntSet> nodeReaders,
                           TreeMap<Integer, String> readerIndices,
                           Map<String, TableIdent> indicesToIdents) {
        this.collectPhaseId = collectPhaseId;
        this.fetchSize = calculateFetchSize(
            JvmInfo.jvmInfo().getConfiguredMaxHeapSize(),fetchSize, fetchSources.size(), outputSymbols);
        this.fetchSources = fetchSources;
        this.outputSymbols = outputSymbols;
        this.nodeReaders = nodeReaders;
        this.readerIndices = readerIndices;
        this.indicesToIdents = indicesToIdents;
    }

    /**
     * Calculate a fetchSize that is not too large.
     * The pre-fetch AND the post-fetch values must fit into memory
     */
    static int calculateFetchSize(long maxHeapInBytes, int fetchSize, int numSources, List<Symbol> outputSymbols) {
        /*
         *
         * fetchIds are Long (~24 bytes)
         * 16 Bytes are accounted for the container type (object array)
         */
        long expectedSizePerPreFetchRow = 16 + numSources * 24;
        long expectedSizePerFetchResultRow = expectedRowSize(outputSymbols);
        // subtract a "base-footprint", this causes lower fetchSizes on low-heap machines
        // but doesn't really influence the fetchSize on machines with a lot of heap
        int baseFootprintInMB = 80;
        double bytesAvailableForFetch = Math.max(
            // expect minimum 1MB to be available for fetch, even if the max heap is tiny
            1024 * 1024,
            (maxHeapInBytes * 0.45) - (baseFootprintInMB * 1024 * 1024));

        long maxFetchSize = (long) bytesAvailableForFetch / (expectedSizePerPreFetchRow + expectedSizePerFetchResultRow);
        if (maxFetchSize > Paging.PAGE_SIZE) {
            return Paging.PAGE_SIZE;
        }
        if (fetchSize == 0) {
            return Math.max(1000, (int) maxFetchSize);
        }
        return Math.min(fetchSize, (int) maxFetchSize);
    }

    private static long expectedRowSize(List<Symbol> outputSymbols) {
        long size = 16;
        for (int i = 0; i < outputSymbols.size(); i++) {
            DataType dataType = outputSymbols.get(i).valueType();
            if (dataType instanceof FixedWidthType) {
                size += ((FixedWidthType) dataType).fixedSize();
            } else {
                size += 1000L; // random large value; if in doubt we want to have a smaller but safe fetchSize
            }
        }
        return size;
    }

    public int collectPhaseId() {
        return collectPhaseId;
    }

    public int getFetchSize() {
        return fetchSize;
    }

    public Map<TableIdent, FetchSource> fetchSources() {
        return fetchSources;
    }

    public List<Symbol> outputSymbols() {
        return outputSymbols;
    }

    public Map<String, IntSet> nodeReaders() {
        return nodeReaders;
    }

    public TreeMap<Integer, String> readerIndices() {
        return readerIndices;
    }

    public Map<String, TableIdent> indicesToIdents() {
        return indicesToIdents;
    }

    @Override
    public void replaceSymbols(Function<Symbol, Symbol> replaceFunction) {
        Lists2.replaceItems(outputSymbols, replaceFunction);
    }

    @Override
    public ProjectionType projectionType() {
        return ProjectionType.FETCH;
    }

    @Override
    public <C, R> R accept(ProjectionVisitor<C, R> visitor, C context) {
        return visitor.visitFetchProjection(this, context);
    }

    @Override
    public List<? extends Symbol> outputs() {
        return outputSymbols;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        FetchProjection that = (FetchProjection) o;
        return collectPhaseId == that.collectPhaseId;
    }

    @Override
    public int hashCode() {
        return collectPhaseId;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        throw new UnsupportedOperationException("writeTo is not supported for " +
                                                FetchProjection.class.getSimpleName());
    }

    @Override
    public Map<String, Object> mapRepresentation() {
        return ImmutableMap.of(
            "type", "Fetch",
            "outputs", ExplainLeaf.printList(outputSymbols),
            "fetchSize", fetchSize
        );
    }
}

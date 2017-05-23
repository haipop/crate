/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.planner.projection;

import io.crate.analyze.symbol.Literal;
import io.crate.analyze.symbol.Symbol;
import io.crate.operation.Paging;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

public class FetchProjectionTest {

    private List<Symbol> tenLongOutputs = Arrays.asList(
        Literal.of(10L),
        Literal.of(20L),
        Literal.of(30L),
        Literal.of(40L),
        Literal.of(50L),
        Literal.of(60L),
        Literal.of(70L),
        Literal.of(80L),
        Literal.of(90L),
        Literal.of(100L)
    );

    @Test
    public void testFetchSizeCalcWithTinyHeap() throws Exception {
        long tinyHeap = 56 * 1024 * 1024;
        int fetchSize = FetchProjection.calculateFetchSize(tinyHeap, 0, 1, tenLongOutputs);
        assertThat(fetchSize, is(24272));
    }

    @Test
    public void testFetchSizeCalcWithLowHeap() throws Exception {
        long lowHeap = 256 * 1024 * 1024;
        int fetchSize = FetchProjection.calculateFetchSize(lowHeap, 0, 1, tenLongOutputs);
        assertThat(fetchSize, is(24272));
    }

    @Test
    public void testFetchSizeCalcWithNormalHeap() throws Exception {
        long normalHeap = 2048L * 1024 * 1024;
        int fetchSize = FetchProjection.calculateFetchSize(normalHeap, 0, 1, tenLongOutputs);
        assertThat(fetchSize, is(Paging.PAGE_SIZE));
    }

    @Test
    public void testFetchSizeCalcWithLargeHeap() throws Exception {
        long largeHeap = 28L * 1024 * 1024 * 1024;
        int fetchSize = FetchProjection.calculateFetchSize(largeHeap, 0, 1, tenLongOutputs);
        assertThat(fetchSize, is(Paging.PAGE_SIZE));
    }

    @Test
    public void testFetchSizeWithLargeUserSuppliedFetchSize() throws Exception {
        long normalHeap = 2048L * 1024 * 1024;
        int fetchSize = FetchProjection.calculateFetchSize(normalHeap, 10_000_000, 1, tenLongOutputs);
        assertThat(fetchSize, is(Paging.PAGE_SIZE));
    }
}

package io.crate.operation.projectors;


import com.carrotsearch.randomizedtesting.annotations.Repeat;
import io.crate.action.sql.SQLActionException;
import io.crate.analyze.symbol.InputColumn;
import io.crate.analyze.symbol.Symbol;
import io.crate.data.*;
import io.crate.executor.transport.ShardUpsertRequest;
import io.crate.executor.transport.TransportShardUpsertAction;
import io.crate.integrationtests.SQLTransportIntegrationTest;
import io.crate.metadata.*;
import io.crate.metadata.doc.DocSysColumns;
import io.crate.operation.NodeJobsCounter;
import io.crate.operation.collect.CollectExpression;
import io.crate.operation.collect.InputCollectExpression;
import io.crate.operation.collect.RowShardResolver;
import io.crate.testing.TestingBatchConsumer;
import io.crate.types.DataTypes;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.admin.indices.create.TransportBulkCreateIndicesAction;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.hamcrest.Matchers;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static io.crate.testing.TestingHelpers.isRow;
import static org.hamcrest.Matchers.*;
import static org.hamcrest.collection.IsIterableContainingInOrder.contains;


public class ShardingUpsertExecutorTest extends SQLTransportIntegrationTest {

    private static final ColumnIdent O_IDENT = new ColumnIdent("o");
    private static final TableIdent tIdent = new TableIdent(null, "t");
    private static final TableIdent t2Ident = new TableIdent(null, "t2");


    @Test
    public void testShardingUpsertExecutorWithLimitedResources() throws Throwable {
        execute("create table t (o int) with (number_of_replicas=0)");
        ensureGreen();

        InputCollectExpression sourceInput = new InputCollectExpression(1);
        List<CollectExpression<Row, ?>> collectExpressions = Collections.<CollectExpression<Row, ?>>singletonList(sourceInput);
        UUID jobID = UUID.randomUUID();
        Functions functions = internalCluster().getInstance(Functions.class);
        List<ColumnIdent> primaryKeyIdents = Arrays.asList(O_IDENT);
        List<? extends Symbol> primaryKeySymbols = Arrays.<Symbol>asList(new InputColumn(0));
        RowShardResolver rowShardResolver = new RowShardResolver(functions, primaryKeyIdents, primaryKeySymbols, null, null);

        ShardUpsertRequest.Builder builder = new ShardUpsertRequest.Builder(
            ShardingUpsertExecutor.BULK_REQUEST_TIMEOUT_SETTING.setting().get(Settings.EMPTY),
            false,
            true,
            null,
            new Reference[]{new Reference(new ReferenceIdent(tIdent, DocSysColumns.RAW), RowGranularity.DOC, DataTypes.STRING)},
            jobID,
            false);

        Function<String, ShardUpsertRequest.Item> itemFactory = id ->
            new ShardUpsertRequest.Item(id, null, new Object[]{sourceInput.value()}, null);


        ShardingUpsertExecutor shardingUpsertExecutor = new ShardingUpsertExecutor<>(
            internalCluster().getInstance(ClusterService.class),
            new NodeJobsCounter(),
            internalCluster().getInstance(ThreadPool.class).scheduler(),
            1,
            jobID,
            rowShardResolver,
            itemFactory,
            builder::newRequest,
            collectExpressions,
            IndexNameResolver.forTable(new TableIdent(null, "t")),
            false,
            internalCluster().getInstance(TransportShardUpsertAction.class)::execute,
            internalCluster().getInstance(TransportBulkCreateIndicesAction.class));


        BatchIterator rowsIterator = RowsBatchIterator.newInstance(IntStream.range(0, 100)
            .mapToObj(i -> new RowN(new Object[]{i, new BytesRef("{\"o\": " + i + "}")}))
            .collect(Collectors.toList()), 2);

        TestingBatchConsumer consumer = new TestingBatchConsumer();
        consumer.accept(CollectingBatchIterator.newInstance(rowsIterator, shardingUpsertExecutor, 1), null);
        Bucket objects = consumer.getBucket();

        assertThat(objects, contains(isRow(100L)));

        execute("refresh table t");
        execute("select count(*) from t");
        assertThat(response.rowCount(), is(1L));
        assertThat(response.rows()[0][0], is(100L));
    }

    @Test
    public void testShardingUpsertExecutorWithSchedulerException() throws Throwable {
        expectedException.expect(EsRejectedExecutionException.class);
//        expectedException.expectMessage("");
        execute("create table t2 (o int)");
        ensureYellow();

        InputCollectExpression sourceInput = new InputCollectExpression(1);
        List<CollectExpression<Row, ?>> collectExpressions = Collections.<CollectExpression<Row, ?>>singletonList(sourceInput);
        UUID jobID = UUID.randomUUID();
        Functions functions = internalCluster().getInstance(Functions.class);
        List<ColumnIdent> primaryKeyIdents = Arrays.asList(O_IDENT);
        List<? extends Symbol> primaryKeySymbols = Arrays.<Symbol>asList(new InputColumn(0));
        RowShardResolver rowShardResolver = new RowShardResolver(functions, primaryKeyIdents, primaryKeySymbols, null, null);

        ShardUpsertRequest.Builder builder = new ShardUpsertRequest.Builder(
            ShardingUpsertExecutor.BULK_REQUEST_TIMEOUT_SETTING.setting().get(Settings.EMPTY),
            false,
            false,
            null,
            new Reference[]{new Reference(new ReferenceIdent(t2Ident, DocSysColumns.RAW), RowGranularity.DOC, DataTypes.INTEGER)},
            jobID,
            false);

        Function<String, ShardUpsertRequest.Item> itemFactory = o ->
            new ShardUpsertRequest.Item(o, null, new Object[]{sourceInput.value()}, null);

        ScheduledExecutorService scheduler = internalCluster().getInstance(ThreadPool.class).scheduler();

        ShardingUpsertExecutor shardingUpsertExecutor = new ShardingUpsertExecutor<>(
            internalCluster().getInstance(ClusterService.class),
            new NodeJobsCounter(),
            scheduler,
            5,
            jobID,
            rowShardResolver,
            itemFactory,
            builder::newRequest,
            collectExpressions,
            IndexNameResolver.forTable(new TableIdent(null, "t2")),
            false,
            internalCluster().getInstance(TransportShardUpsertAction.class)::execute,
            internalCluster().getInstance(TransportBulkCreateIndicesAction.class));


        BatchIterator rowsIterator = RowsBatchIterator.newInstance(IntStream.range(0, 100)
            .mapToObj(i -> new RowN(new Object[]{i, new BytesRef("{\"o\": " + i + "}")}))
            .collect(Collectors.toList()), 2);

        TestingBatchConsumer consumer = new TestingBatchConsumer();

        scheduler.shutdown();
        consumer.accept(CollectingBatchIterator.newInstance(rowsIterator, shardingUpsertExecutor, 1), null);
        Bucket objects = consumer.getBucket();

        assertThat((long)objects.size(), is(lessThan(100L)));

        execute("refresh table t2");
        execute("select count(*) from t2");
        assertThat(response.rowCount(), is(1L));
        assertThat((long)response.rows()[0][0],  is(lessThan(100L)));
    }
}

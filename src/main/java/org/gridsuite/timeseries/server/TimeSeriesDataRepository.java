/**
 * Copyright (c) 2022, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package org.gridsuite.timeseries.server;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Repository;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Stopwatch;
import com.powsybl.timeseries.DoubleDataChunk;
import com.powsybl.timeseries.DoubleTimeSeries;
import com.powsybl.timeseries.StoredDoubleTimeSeries;
import com.powsybl.timeseries.StringDataChunk;
import com.powsybl.timeseries.StringTimeSeries;
import com.powsybl.timeseries.TimeSeries;
import com.powsybl.timeseries.TimeSeriesDataType;
import com.powsybl.timeseries.TimeSeriesIndex;
import com.powsybl.timeseries.TimeSeriesMetadata;
import com.powsybl.timeseries.UncompressedDoubleDataChunk;
import com.powsybl.timeseries.UncompressedStringDataChunk;
import com.zaxxer.hikari.HikariDataSource;

/**
 * @author Jon Schuhmacher <jon.harper at rte-france.com>
 */
@Repository
public class TimeSeriesDataRepository {

    private static final Logger LOGGER = LoggerFactory.getLogger(TimeSeriesDataRepository.class);

    private final ObjectMapper objectMapper;
    private final HikariDataSource datasource;
    private final TimeSeriesMetadataService timeSeriesMetadataService;

    public TimeSeriesDataRepository(ObjectMapper objectMapper, HikariDataSource datasource,
            TimeSeriesMetadataService timeSeriesMetadataService) {
        this.objectMapper = objectMapper;
        this.datasource = datasource;
        this.timeSeriesMetadataService = timeSeriesMetadataService;
    }

    // TODO tune these parameters for performance
    // TODO make these parameters in application.yaml
    // TODO difference between strings and double ?
    // 30000 values => e.g. 100 rows of 300 cols
    @Value("${timeseries.write-batch-size:30000}")
    private int writebatchsize;
    // 3 batches per connection => e.g. 300 rows of 300 cols
    // TODO do we need this or is 1 always superior ??
    @Value("${timeseries.write-batch-per-connection:3}")
    private int writebatchperconnection;
    // 10000 values => e.g. 17 rows of 300 cols
    // TODO difference between strings and double ?
    @Value("${timeseries.read-batch-size:5000}")
    private int readbatchsize;
    // 1 batch per connection values => e.g. 17 rows of 300 cols
    @Value("${timeseries.read-batch-per-connection:1}") // TODO do we need this or always 1 ??
    private int readbatchperconnection;

    public void save(UUID uuid, List<TimeSeries> listTimeSeries) {
        try {
            doSave(uuid, listTimeSeries);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    // TODO untangle multithreaded scatter/gather from actual work
    private void doSave(UUID uuid, List<TimeSeries> listTimeSeries) throws Exception {

        int colcount = listTimeSeries.size();
        int rowcount = listTimeSeries.get(0).getMetadata().getIndex().getPointCount();

        int batchrow = (writebatchsize + colcount - 1) / colcount;
        int batchcount = (rowcount + batchrow - 1) / batchrow;

        int threadcount = (batchcount + writebatchperconnection - 1) / writebatchperconnection;
        int batchinthread = (batchcount + threadcount - 1) / threadcount;

        List<Callable<Void>> callables = new ArrayList<>(Collections.nCopies(threadcount, null));

        LOGGER.debug(
                "insert start {}, {} instants by {} time series, in batch of {} rows ({} doubles for each batch), numbatch={}, numthreads={}, batchinthread={}",
                uuid, rowcount, colcount, batchrow, batchrow * colcount, batchcount, threadcount, batchinthread);
        Stopwatch stopwatch = Stopwatch.createStarted();

        // TODO here we transpose, which means it's impossible to stream
        // data from the client to the database, the server has to buffer in memory.
        // try to change the API to allow streaming.
        // TODO avoid copying the data (timeSeries toArray())?
        // TODO, using toArray() doesn't allow to know if the client has missing data
        // at the end of the time series in the json. For example, [1,2,3, NaN] or [1,2,3] both
        // return the same toArray() of {1,2,3, Double.NaN}. For Strings, it's {"foo", "bar", null}.
        // This can have a big impact for a timeSeries with only missing data ( [] vs [null,null, ..., null]
        BiFunction<Integer, Integer, Object> stringOrDoubledataGetter;
        TimeSeriesMetadata metadata = listTimeSeries.get(0).getMetadata();
        if (TimeSeriesDataType.DOUBLE == metadata.getDataType()) {
            List<double[]> datadouble = new ArrayList<>();
            for (int i = 0; i < listTimeSeries.size(); i++) {
                // TODO timeSeries raw type
                datadouble.add(((DoubleTimeSeries) listTimeSeries.get(i)).toArray());
            }
            stringOrDoubledataGetter = (row, col) -> {
                double d = datadouble.get(row)[col];
                //NaN is not valid JSON, serialize as null
                return Double.isNaN(d) ? null : d;
            };
        } else if (TimeSeriesDataType.STRING == metadata.getDataType()) {
            List<String[]> datastring = new ArrayList<>();
            for (int i = 0; i < listTimeSeries.size(); i++) {

                datastring.add(((StringTimeSeries) listTimeSeries.get(i)).toArray());
            }
            stringOrDoubledataGetter = (row, col) -> datastring.get(row)[col];
        } else {
            throw new RuntimeException("Unsupported save of timeSeries type " + metadata.getDataType());
        }

        for (int i = 0; i < threadcount; i++) {
            int iCopy = i;
            callables.set(i, () -> {
                try (var conn = datasource.getConnection();
                ) {
                    conn.setAutoCommit(false);
                    try (var ps = conn.prepareStatement(TimeSeriesDataQueryCatalog.INSERT);) {

                        int threadrowstart = iCopy * batchinthread * batchrow;
                        int remainingrows = rowcount % (batchinthread * batchrow);
                        int threadrowcount = iCopy == threadcount - 1 && remainingrows > 0 ? remainingrows
                                : batchinthread * batchrow;
                        for (int l = 0; l < threadrowcount; l++) {
                            int row = threadrowstart + l;
                            Map<String, Object> tsdata = new HashMap<>();
                            for (int m = 0; m < colcount; m++) {
                                int col = m;
                                String tsName = listTimeSeries.get(col).getMetadata().getName();
                                Object tsData = stringOrDoubledataGetter.apply(col, row);
                                tsdata.put(tsName, tsData);
                            }
                            ps.setObject(1, uuid);
                            // TODO instants/durations ?
                            // ps.setObject(1,
                            //   Timestamp.from(listTimeSeries.get(0).getMetadata().getIndex().getInstantAt(row)));
                            ps.setInt(2, row);
                            ps.setObject(3, objectMapper.writeValueAsString(tsdata), java.sql.Types.OTHER);
                            ps.addBatch();

                            if (l == threadrowcount - 1 || (l % batchrow) == batchrow - 1) {
                                ps.executeBatch();
                            }
                        }

                        conn.commit();
                    } catch (Exception e) {
                        LOGGER.error("Error saving timeSeries data", e);
                        conn.rollback();
                        throw new RuntimeException(e);
                    } finally {
                        conn.setAutoCommit(true);
                    }
                }
                return null;
            });
        }
        //TODO improve multithread impl ? use better APIs than forkjoinpool ? don't create the pool for each request ?
        if (threadcount > 1) {
            List<ForkJoinTask<Void>> tasks = new ArrayList<>(Collections.nCopies(threadcount, null));
            int size = datasource.getMaximumPoolSize();
            LOGGER.debug("insert in forkjoinpool size={}", size);
            ForkJoinPool pool = new ForkJoinPool(size);
            for (int i = 0; i < threadcount; i++) {
                tasks.set(i, pool.submit(callables.get(i)));
            }
            for (int i = 0; i < threadcount; i++) {
                tasks.get(i).get();
            }
        } else {
            LOGGER.debug("insert in http thread");
            callables.get(0).call();
        }
        long b = System.nanoTime();
        LOGGER.debug("insert done {}, took {}ms", uuid, stopwatch.elapsed(TimeUnit.MILLISECONDS));
    }

    public List<TimeSeries> findById(TimeSeriesIndex index, Map<String, Object> individualMetadatas, UUID uuid, boolean tryToCompress, String time, List<String> timeSeriesNames) {
        try {
            return doFindById(index, individualMetadatas, uuid, tryToCompress, time, timeSeriesNames);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    // TODO untangle multithreaded scatter/gather from actual work
    private List<TimeSeries> doFindById(TimeSeriesIndex index, Map<String, Object> individualMetadatas, UUID uuid, boolean tryToCompress, String time, List<String> timeSeriesNames) throws Exception {
        Stopwatch stopwatch = Stopwatch.createStarted();

        int colcount = timeSeriesNames != null ? timeSeriesNames.size() : individualMetadatas.size();
        int rowcount = index.getPointCount();

        int batchrow = (readbatchsize + colcount - 1) / colcount;
        int batchcount = (rowcount + batchrow - 1) / batchrow;

        int threadcount = (batchcount + readbatchperconnection - 1) / readbatchperconnection;
        int batchinthread = (batchcount + threadcount - 1) / threadcount;

        LOGGER.debug(
                "select start {}, {} instants by {}/{} time series, in batch of {} rows ({} doubles for each batch), numbatch={}, numthreads={}, batchinthread={}",
                uuid, rowcount, timeSeriesNames != null ? Integer.toString(timeSeriesNames.size()) : "all", individualMetadatas.size(),
                batchrow, batchrow * colcount, batchcount, threadcount, batchinthread);

        List<Callable<Map<Object, Object>>> callables = new ArrayList<>(Collections.nCopies(threadcount, null));
        for (int i = 0; i < threadcount; i++) {
            int iCopy = i;
            callables.set(i, () -> {
                Map<Object, Object> threadres = new LinkedHashMap<>();
                try (var connection = datasource.getConnection();) {
                    for (int l = 0; l < batchinthread; l++) {
                        int threadrowstart = iCopy * batchinthread * batchrow;
                        int remainingrows = rowcount % (batchinthread * batchrow);
                        int threadrowcount = iCopy == threadcount - 1 && remainingrows > 0 ? remainingrows
                                : batchinthread * batchrow;
                        int threadrowend = threadrowstart + threadrowcount;
                        //TODO, add filter on cols by individual timeSeries tag ? to select a tagged subgroup?
                        // if we add subgroup tagging, then we can allow double and strings in the same group,
                        // because we can then do aggregates (min, max, mean, kpercentile) etc in compatible subgroups
                        // this is only useful if subgroups overlap, otherwise you can just create separate groups
                        //     var ps = connection.prepareStatement("select  sim_time,  from simulations_10 where group_id=? and and sim_time >= ? and sim_time < ?;");
                        try (var ps = connection.prepareStatement(
                                TimeSeriesDataQueryCatalog.makeSelect(timeSeriesNames));
                        ) {
                            ps.setObject(1, uuid);
                            // TODO instants/durations ?
                            ps.setInt(2, threadrowstart);
                            ps.setInt(3, threadrowend);
                            try (var resultSet = ps.executeQuery();) {
                                while (resultSet.next()) {
                                    // TODO avoid copying the data by writing directly from each thread to the final
                                    // structure ?
                                    // TODO instants/durations ?
                                    threadres.put(resultSet.getInt(1), objectMapper.readValue(resultSet.getString(2), Map.class));
                                }
                            }
                        }
                    }
                }
                return threadres;
            });
        }

        Map<Object, Object> res;
        if (threadcount > 1) {
            List<ForkJoinTask<Map<Object, Object>>> tasks = new ArrayList<>(Collections.nCopies(threadcount, null));
            int size = datasource.getMaximumPoolSize();
            LOGGER.debug("select in forkjoinpool size={}", size);
            ForkJoinPool pool = new ForkJoinPool(size);
            for (int i = 0; i < threadcount; i++) {
                tasks.set(i, pool.submit(callables.get(i)));
            }
            res = new LinkedHashMap<>();
            for (int i = 0; i < threadcount; i++) {
                // TODO avoid copying the data by writing directly from each thread to the final
                // structure ?
                res.putAll(tasks.get(i).get());
            }
        } else {
            LOGGER.debug("select in http thread");
            res = callables.get(0).call();
        }
        LOGGER.debug("select done, {} took {}ms", uuid, stopwatch.elapsed(TimeUnit.MILLISECONDS));

        // TODO same as save, avoid the transpose to allow stream from database to
        // clients ?
        // TODO avoid this extra copy to an intermediate transposed map
        Map<String, List<Object>> data = new HashMap<>();
        for (Map.Entry<Object, Object> entry : res.entrySet()) {
            Map<Object, Object> dict = (Map<Object, Object>) entry.getValue();
            for (Map.Entry<Object, Object> entryPoint : dict.entrySet()) {
                String tsname = (String) entryPoint.getKey();
                // TODO more types
                Object val = entryPoint.getValue();
                data.computeIfAbsent(tsname, _ignored -> new ArrayList<>()).add(val);
            }
        }
        List<TimeSeries> ret = new ArrayList<>();
        for (Map.Entry<String, List<Object>> entry : data.entrySet()) {
            TimeSeriesMetadata metadata = timeSeriesMetadataService.getMetadata(index, individualMetadatas, entry.getKey());
            // TODO remove duplication
            if (TimeSeriesDataType.DOUBLE == metadata.getDataType()) {
                double[] doubles = entry.getValue().stream().map(Double.class::cast)
                        .mapToDouble(d -> d == null ? Double.NaN : d).toArray();

                // TODO should be in the timeSeries API ?
                DoubleDataChunk ddc = new UncompressedDoubleDataChunk(0, doubles);
                // TODO get compress mode from the metadata sent by the client
                if (tryToCompress) {
                    ddc = ddc.tryToCompress();
                }
                // TODO more types
                // TODO index from client
                TimeSeries timeSeries = new StoredDoubleTimeSeries(metadata, List.of(ddc));
                ret.add(timeSeries);
            } else if (TimeSeriesDataType.STRING == metadata.getDataType()) {
                String[] strings = entry.getValue().toArray(new String[0]);

                // TODO should be in the timeSeries API ?
                StringDataChunk ddc = new UncompressedStringDataChunk(0, strings);
                // TODO get compress mode from the metadata sent by the client
                if (tryToCompress) {
                    ddc = ddc.tryToCompress();
                }
                // TODO more types
                // TODO index from client
                TimeSeries timeSeries = new StringTimeSeries(metadata, List.of(ddc));
                ret.add(timeSeries);
            } else {
                throw new RuntimeException("Unsupported read of timeSeries type " + metadata.getDataType());
            }
        }
        return ret;
    }

    private void doDelete(UUID uuid) throws Exception {
        try (var conn = datasource.getConnection();
                var ps = conn.prepareStatement(TimeSeriesDataQueryCatalog.DELETE);
           ) {
            ps.setObject(1, uuid);
            ps.executeUpdate();
        }
    }

    public void delete(UUID uuid) {
        try {
            doDelete(uuid);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

}

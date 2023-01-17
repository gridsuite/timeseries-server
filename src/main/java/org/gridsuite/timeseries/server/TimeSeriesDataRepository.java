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
import java.util.function.BiFunction;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Repository;

import com.fasterxml.jackson.databind.ObjectMapper;
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

    private static final String INSERT = "insert into timeseries_group_data ( group_id, time, json_obj ) values (?,?,?);";
    private static final String COUNT = "select count(*) from timeseries_group_data where group_id=?;";
    private static final String SELECTALL = "select time, json_obj from timeseries_group_data where group_id=? and time>=? and time <? order by time;";
    private static final String DELETE = "delete from timeseries_group_data where group_id=?";

    private String makeSelectAll(List<String> col) {
        //TODO validate col sql injection
        //filter on cols ( select time, json_build_object ('a', json_obj->'a', 'd', json_obj->'d', 'e', json_obj->'e'), ...  instead of the whole json_obj)
        if (col == null || col.isEmpty()) {
            return SELECTALL;
        } else {
            // TODO here if we try with 51 cols we get an exception
            // Caused by: org.postgresql.util.PSQLException: ERROR: cannot pass more than 100 arguments to a function
            return col.stream().map(c -> "'" + c + "', json_obj->'" + c + "'").collect(Collectors.joining(", ",
                    "select time, json_build_object (",
                    ") json_obj from timeseries_group_data where group_id=? and time>=? and time <? order by time;"));
        }
    }

    private final ObjectMapper objectMapper;
    private final HikariDataSource datasource;
    private final TimeSeriesMetadataService timeseriesMetadataService;

    public TimeSeriesDataRepository(ObjectMapper objectMapper, HikariDataSource datasource,
            TimeSeriesMetadataService timeseriesMetadataService) {
        this.objectMapper = objectMapper;
        this.datasource = datasource;
        this.timeseriesMetadataService = timeseriesMetadataService;
    }

    // TODO tune these parameters for performance
    // TODO make these parameters in application.yaml
    private static final int WRITE_BATCHSIZE = 30000;
    private static final int WRITE_THREADSIZE = 3; // 3 batches => e.g. 300 rows of 300cols
    private static final int READ_THREADSIZE = 300; // 300 db rows, TODO take the number of cols into account

    public void save(UUID uuid, List<TimeSeries> listTimeseries) {
        try {
            doSave(uuid, listTimeseries);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    // TODO untangle multithreaded scatter/gather from actual work
    private void doSave(UUID uuid, List<TimeSeries> listTimeseries) throws Exception {

        int colcount = listTimeseries.size();
        int rowcount = listTimeseries.get(0).getMetadata().getIndex().getPointCount();

        int batchrow = (WRITE_BATCHSIZE + colcount - 1) / colcount;
        int batchcount = (rowcount + batchrow - 1) / batchrow;

        int threadcount = (batchcount + WRITE_THREADSIZE - 1) / WRITE_THREADSIZE;
        int threadbatches = (batchcount + threadcount - 1) / threadcount;

        List<Callable<Void>> callables = new ArrayList<>(Collections.nCopies(threadcount, null));

        LOGGER.debug(
                "insert {} in batch of {} rows ({} doubles for each batch), numbatch={}, numthreads={}, threadbatches={}",
                uuid, batchrow, batchrow * colcount, batchcount, threadcount, threadbatches);
        long a = System.nanoTime();

        // TODO here we transpose, which means it's impossible to stream
        // data from the client to the database, the server has to buffer in memory.
        // try to change the API to allow streaming.
        // TODO avoid copying the data (timeseries toArray())?
        // TODO, using toArray() doesn't allow to know if the client has missing data
        // at the end of the time series in the json. For example, [1,2,3, NaN] or [1,2,3] both
        // return the same toArray() of {1,2,3, Double.NaN}. For Strings, it's {"foo", "bar", null}.
        // This can have a big impact for a timeseries with only missing data ( [] vs [null,null, ..., null]
        BiFunction<Integer, Integer, Object> stringOrDoubledataGetter;
        if (TimeSeriesDataType.DOUBLE.equals(listTimeseries.get(0).getMetadata().getDataType())) {
            List<double[]> datadouble = new ArrayList<>();
            for (int i = 0; i < listTimeseries.size(); i++) {
                // TODO timeseries raw type
                datadouble.add(((DoubleTimeSeries) listTimeseries.get(i)).toArray());
            }
            stringOrDoubledataGetter = (row, col) -> {
                double d = datadouble.get(row)[col];
                //NaN is not valid JSON, serialize as null
                return Double.isNaN(d) ? null : d;
            };
        } else if (TimeSeriesDataType.STRING.equals(listTimeseries.get(0).getMetadata().getDataType())) {
            List<String[]> datastring = new ArrayList<>();
            for (int i = 0; i < listTimeseries.size(); i++) {

                datastring.add(((StringTimeSeries) listTimeseries.get(i)).toArray());
            }
            stringOrDoubledataGetter = (row, col) -> datastring.get(row)[col];
        } else {
            throw new RuntimeException("Unsupported save of timeseries type" + listTimeseries.get(0).getClass());
        }

        for (int i = 0; i < threadcount; i++) {
            int iCopy = i;
            callables.set(i, () -> {
                try (var conn = datasource.getConnection();
                ) {
                    conn.setAutoCommit(false);
                    try (var ps = conn.prepareStatement(INSERT);) {

                        int threadrowstart = iCopy * threadbatches * batchrow;
                        int remainingrows = rowcount % (threadbatches * batchrow);
                        int threadrowcount = iCopy == threadcount - 1 && remainingrows > 0 ? remainingrows
                                : threadbatches * batchrow;
                        for (int l = 0; l < threadrowcount; l++) {
                            int row = threadrowstart + l;
                            Map<String, Object> tsdata = new HashMap<>();
                            for (int m = 0; m < colcount; m++) {
                                int col = m;
                                String tsName = listTimeseries.get(col).getMetadata().getName();
                                Object tsData = stringOrDoubledataGetter.apply(col, row);
                                tsdata.put(tsName, tsData);
                            }
                            ps.setObject(1, uuid);
                            // TODO instants/durations ?
                            // ps.setObject(1,
                            //   Timestamp.from(listTimeseries.get(0).getMetadata().getIndex().getInstantAt(row)));
                            ps.setInt(2, row);
                            ps.setObject(3, objectMapper.writeValueAsString(tsdata), java.sql.Types.OTHER);
                            ps.addBatch();

                            if (l == threadrowcount - 1 || (l % batchrow) == batchrow - 1) {
                                ps.executeBatch();
                            }
                        }

                        conn.commit();
                    } catch (Exception e) {
                        LOGGER.error("Error saving timeseries data", e);
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
            LOGGER.debug("Starting inserts in forkjoinpool size={}", size);
            ForkJoinPool pool = new ForkJoinPool(size);
            for (int i = 0; i < threadcount; i++) {
                tasks.set(i, pool.submit(callables.get(i)));
            }
            for (int i = 0; i < threadcount; i++) {
                tasks.get(i).get();
            }
        } else {
            LOGGER.debug("Starting inserts in http thread");
            callables.get(0).call();
        }
        long b = System.nanoTime();
        LOGGER.debug("inserted {} took: {}ms", uuid, (b - a) / 1000000);
    }

    public List<TimeSeries> findById(TimeSeriesIndex index, Map<String, Object> individualMetadatas, UUID uuid, boolean tryToCompress, String time, List<String> col) {
        try {
            return doFindById(index, individualMetadatas, uuid, tryToCompress, time, col);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    // TODO untangle multithreaded scatter/gather from actual work
    private List<TimeSeries> doFindById(TimeSeriesIndex index, Map<String, Object> individualMetadatas, UUID uuid, boolean tryToCompress, String time, List<String> col) throws Exception {
        long a = System.nanoTime();
        int cnt = -1;
        //TODO maintain this as a separate metadata instead of select count(*) when requesting all rows ?
        //TODO maintain an estimated col count as metadata instead of just guessing when requesting all cols ?
        try (var connection = datasource.getConnection();
             var ps = connection.prepareStatement(COUNT);) {
            ps.setObject(1, uuid);
            try (var resultSet = ps.executeQuery();) {
                if (resultSet.next()) {
                    cnt = resultSet.getInt(1);
                }
            }
        }
        long b = System.nanoTime();
        int threadcount = (cnt + READ_THREADSIZE - 1) / READ_THREADSIZE;
        List<Callable<Map<Object, Object>>> callables = new ArrayList<>(Collections.nCopies(threadcount, null));
        for (int i = 0; i < threadcount; i++) {
            int iCopy = i;
            int threadrowstart = iCopy * READ_THREADSIZE;
            int threadrowend = (iCopy + 1) * READ_THREADSIZE;
            callables.set(i, () -> {
                Map<Object, Object> threadres = new LinkedHashMap<>();
                try (var connection = datasource.getConnection();
                //TODO, add filter on rows (start < time < end)
                //TODO, add filter on cols by range (alphabetical) ?
                //TODO, add filter on cols by individual timeseries tag ? to select a tagged subgroup?
                // if we add subgroup tagging, then we can allow double and strings in the same group,
                // because we can then do aggregates (min, max, mean, kpercentile) etc in compatible subgroups
                // this is only useful if subgroups overlap, otherwise you can just create separate groups
                //     var ps = connection.prepareStatement("select  sim_time,  from simulations_10 where group_id=? and and sim_time >= ? and sim_time < ?;");
                     var ps = connection.prepareStatement(makeSelectAll(col));
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
                return threadres;
            });
        }

        Map<Object, Object> res;
        if (threadcount > 1) {
            List<ForkJoinTask<Map<Object, Object>>> tasks = new ArrayList<>(Collections.nCopies(threadcount, null));
            int size = datasource.getMaximumPoolSize();
            LOGGER.debug("Starting selects in forkjoinpool size={}", size);
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
            LOGGER.debug("Starting inserts in http thread");
            res = callables.get(0).call();
        }
        long c = System.nanoTime();
        LOGGER.debug("read {} took {}ms (count {}ms, {} queries in {} threads {}ms)", uuid, (c - a) / 1000000,
                (b - a) / 1000000, cnt, threadcount, (c - b) / 1000000);

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
            TimeSeriesMetadata metadata = timeseriesMetadataService.getMetadata(index, individualMetadatas, entry.getKey());
            // TODO remove duplication
            if (TimeSeriesDataType.DOUBLE.equals(metadata.getDataType())) {
                double[] doubles = entry.getValue().stream().map(Double.class::cast)
                        .mapToDouble(d -> d == null ? Double.NaN : d).toArray();

                // TODO should be in the timeseries API ?
                DoubleDataChunk ddc = new UncompressedDoubleDataChunk(0, doubles);
                // TODO get compress mode from the metadata sent by the client
                if (tryToCompress) {
                    ddc = ddc.tryToCompress();
                }
                // TODO more types
                // TODO index from client
                TimeSeries timeseries = new StoredDoubleTimeSeries(metadata, List.of(ddc));
                ret.add(timeseries);
            } else if (TimeSeriesDataType.STRING.equals(metadata.getDataType())) {
                String[] strings = entry.getValue().toArray(new String[0]);

                // TODO should be in the timeseries API ?
                StringDataChunk ddc = new UncompressedStringDataChunk(0, strings);
                // TODO get compress mode from the metadata sent by the client
                if (tryToCompress) {
                    ddc = ddc.tryToCompress();
                }
                // TODO more types
                // TODO index from client
                TimeSeries timeseries = new StringTimeSeries(metadata, List.of(ddc));
                ret.add(timeseries);
            } else {
                throw new RuntimeException("Unsupported read of timeseries type" + entry.getValue().get(0).getClass());
            }
        }
        return ret;
    }

    private void doDelete(UUID uuid) throws Exception {
        try (var conn = datasource.getConnection();
                var ps = conn.prepareStatement(DELETE);
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

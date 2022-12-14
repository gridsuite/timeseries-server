/**
 * Copyright (c) 2022, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package org.gridsuite.timeseries.server;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.delete;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.powsybl.timeseries.IrregularTimeSeriesIndex;
import com.powsybl.timeseries.RegularTimeSeriesIndex;
import com.powsybl.timeseries.StoredDoubleTimeSeries;
import com.powsybl.timeseries.StringTimeSeries;
import com.powsybl.timeseries.TimeSeries;
import com.powsybl.timeseries.TimeSeriesDataType;
import com.powsybl.timeseries.TimeSeriesMetadata;
import com.powsybl.timeseries.UncompressedDoubleDataChunk;

/**
 * @author Jon Schuhmacher <jon.harper at rte-france.com>
 *
 */
@RunWith(SpringRunner.class)
@AutoConfigureMockMvc
@SpringBootTest
public class TimeSeriesIT {

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private ObjectMapper mapper;

    // TODO check more infos in tsgroups getAll
    private String getAllRef(Map<String, List<TimeSeries>> groupsById) throws JsonProcessingException {
        return mapper.writeValueAsString(
            groupsById.entrySet().stream()
                .map(entry -> Map.of("id", entry.getKey()))
                .collect(Collectors.toList())
        );
    }

    // TODO compare more than just the data
    // TODO more types
    private void assertTimeseriesEquals(List<TimeSeries> tsRefs, String actual) {
        List<TimeSeries> tsGets = TimeSeries.parseJson(actual);

        assertEquals(tsRefs.size(), tsGets.size());
        if (tsRefs.get(0) instanceof StoredDoubleTimeSeries) {
            for (int i = 0; i < tsRefs.size(); i++) {
                StoredDoubleTimeSeries tsRef = (StoredDoubleTimeSeries) tsRefs.get(i);
                StoredDoubleTimeSeries tsGet = (StoredDoubleTimeSeries) tsGets.get(i);
                assertArrayEquals(tsRef.toArray(), tsGet.toArray(), 0);
                assertEquals(tsRef.getMetadata(), tsGet.getMetadata());
            }
        } else if (tsRefs.get(0) instanceof StringTimeSeries) {
            for (int i = 0; i < tsRefs.size(); i++) {
                StringTimeSeries tsRef = (StringTimeSeries) tsRefs.get(i);
                StringTimeSeries tsGet = (StringTimeSeries) tsGets.get(i);
                assertArrayEquals(tsRef.toArray(), tsGet.toArray());
                assertEquals(tsRef.getMetadata(), tsGet.getMetadata());
            }
        }
    }

    private String testCreateGetTs(List<TimeSeries> tsRef)
            throws Exception, JsonProcessingException, JsonMappingException, UnsupportedEncodingException {
        MvcResult resCreate =
                mockMvc.perform(
                    post("/v1/timeseries-group")
                        .content(TimeSeries.toJson(tsRef))
                ).andExpect(status().isOk())
                .andReturn();
        String createdUuid = (String) mapper.readValue(resCreate.getResponse().getContentAsString(), Map.class).get("id");

        MvcResult resGet = mockMvc.perform(get("/v1/timeseries-group/{uuid}", createdUuid)).andExpect(status().isOk())
                .andReturn();
        String getJson = resGet.getResponse().getContentAsString();
        assertTimeseriesEquals(tsRef, getJson);
        return createdUuid;
    }

    @Test
    // TODO only one test for now to avoid cleaning up the db
    public void test() throws Exception {
        mockMvc.perform(get("/v1/timeseries-group")).andExpectAll(status().isOk(), content().json("[]"));

        RegularTimeSeriesIndex regularIndex = new RegularTimeSeriesIndex(0, 2, 1);
        List<TimeSeries> tsRef1 = List.of(
            TimeSeries.createDouble("first", regularIndex, 2d, 3d, 4d),
            // this one has tags, little more verbose
            new StoredDoubleTimeSeries(new TimeSeriesMetadata("second", TimeSeriesDataType.DOUBLE, Map.of("unit", "kV"), regularIndex), List.of(new UncompressedDoubleDataChunk(0, new double[] {5d, 6d, 7d})))
        );

        String createdUuid1 = testCreateGetTs(tsRef1);

        MvcResult res = mockMvc.perform(get("/v1/timeseries-group")).andExpectAll(
                status().isOk(),
                content().json(getAllRef(Map.of(createdUuid1, tsRef1)))
        ).andReturn();
        System.out.println(res.getResponse().getContentAsString());

        IrregularTimeSeriesIndex irregularIndex = new IrregularTimeSeriesIndex(new long[] {0, 1, 2 });
        List<TimeSeries> tsRef2 = List.of(
            TimeSeries.createDouble("first", regularIndex, 2d, 3d, 4d),
            TimeSeries.createDouble("second", regularIndex, 5d, 6d, 7d)
        );

        String createdUuid2 = testCreateGetTs(tsRef2);

        mockMvc.perform(get("/v1/timeseries-group")).andExpectAll(
            status().isOk(),
            content().json(
                getAllRef(Map.of(
                    createdUuid1, tsRef1,
                    createdUuid2, tsRef2
                ))
            )
        );

        mockMvc.perform(delete("/v1/timeseries-group/{uuid}", createdUuid1)).andExpect(status().isOk());

        mockMvc.perform(get("/v1/timeseries-group")).andExpectAll(
                status().isOk(),
                content().json(getAllRef(Map.of(createdUuid2, tsRef2)))
        );

        mockMvc.perform(delete("/v1/timeseries-group/{uuid}", createdUuid2)).andExpect(status().isOk());

        mockMvc.perform(get("/v1/timeseries-group")).andExpectAll(
                status().isOk(),
                content().json("[]")
        );

        List<TimeSeries> tsRef3 = List.of(
            TimeSeries.createString("first", regularIndex, "two", "three", "four"),
            TimeSeries.createString("second", regularIndex, "five", "six", "seven")
        );

        String createdUuid3 = testCreateGetTs(tsRef3);

        mockMvc.perform(get("/v1/timeseries-group")).andExpectAll(
                status().isOk(),
                content().json(getAllRef(Map.of(createdUuid3, tsRef3)))
        );

        List<TimeSeries> tsRef4 = List.of(
            TimeSeries.createDouble("first", regularIndex, 2d, 3d, 4d),
            TimeSeries.createDouble("second", irregularIndex, 5d, 6d, 7d)
        );
        mockMvc.perform(
            post("/v1/timeseries-group")
                .content(TimeSeries.toJson(tsRef4))
        ).andExpect(status().isBadRequest());
    }

}

/**
 * Copyright (c) 2021, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package org.gridsuite.timeseries.server;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

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
import com.powsybl.timeseries.TimeSeries;

/**
 * @author Jon Schuhmacher <jon.harper at rte-france.com>
 *
 *         adapted from
 *         https://www.baeldung.com/spring-boot-testcontainers-integration-test
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
    private void assertTimeseriesEquals(List<TimeSeries> tsRef, String actual) {
        List<TimeSeries> tsGet = TimeSeries.parseJson(actual);

        assertEquals(tsRef.size(), tsGet.size());
        for (int i = 0; i < tsRef.size(); i++) {
            assertArrayEquals(((StoredDoubleTimeSeries) tsRef.get(i)).toArray(),
                    ((StoredDoubleTimeSeries) tsGet.get(i)).toArray(), 0);
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
        String createdUuid = mapper.readValue(resCreate.getResponse().getContentAsString(), String.class);

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
           TimeSeries.createDouble("first", regularIndex, 2d,3d,4d),
           TimeSeries.createDouble("second", regularIndex, 5d,6d,7d)
        );

        String createdUuid1 = testCreateGetTs(tsRef1);

        mockMvc.perform(get("/v1/timeseries-group")).andExpectAll(
                status().isOk(),
                content().json(getAllRef(Map.of(createdUuid1, tsRef1)))
        );

        IrregularTimeSeriesIndex irregularIndex = new IrregularTimeSeriesIndex(new long[] { 0, 1, 2 });
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
    }

}

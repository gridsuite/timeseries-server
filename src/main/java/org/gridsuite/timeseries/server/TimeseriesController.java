/*
 * Copyright (c) 2022, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package org.gridsuite.timeseries.server;

import java.util.List;
import java.util.UUID;

import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import com.powsybl.timeseries.TimeSeries;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;

/**
 * @author Jon Schuhmacher <jon.harper at rte-france.com>
 */

@RestController
@RequestMapping(value = "/v1")
@Tag(name = "Timeseries server")
public class TimeseriesController {

    private final TimeseriesService timeseriesService;

    public TimeseriesController(TimeseriesService timeseriesService) {
        this.timeseriesService = timeseriesService;
    }

    @GetMapping(value = "/timeseries-group")
    @Operation(summary = "Get all timeseries groups")
    @ApiResponses(value = {@ApiResponse(responseCode = "200", description = "The list of timeseries groups")})
    public ResponseEntity<List<TimeseriesGroupEntity>> getTimeseriesGroupsList() {
        return ResponseEntity.ok().contentType(MediaType.APPLICATION_JSON)
                .body(timeseriesService.getTimeseriesGroupsList());
    }

    @PostMapping(value = "/timeseries-group")
    @Operation(summary = "create a timeseries group")
    @ApiResponses(value = {@ApiResponse(responseCode = "200", description = "The timeseries group was successfully created")})
    //TODO better interface with springboot's objectmapper using the timeseries jackson in powsybl ?
    public UUID createTimeseriesGroup(@RequestBody String timeseries) {
        List<TimeSeries> list = TimeSeries.parseJson(timeseries);
        return timeseriesService.createTimeseriesGroup(list);
    }

    @GetMapping(value = "/timeseries-group/{uuid}")
    @Operation(summary = "Get data of a timeseries groups")
    @ApiResponses(value = {@ApiResponse(responseCode = "200", description = "The list of timeseries groups")})
    public ResponseEntity<String> getTimeseriesGroup(
        @PathVariable UUID uuid,
        //TODO more kinds of filters
        @RequestParam( required = false) boolean tryToCompress,
        @RequestParam( required = false) String time,
        @RequestParam( required = false) String col
    ) {
        List<TimeSeries> list = timeseriesService.getTimeseriesGroup(uuid, tryToCompress, time, col);
        return ResponseEntity.ok().contentType(MediaType.APPLICATION_JSON).body(TimeSeries.toJson(list));
    }

    @DeleteMapping(value = "/timeseries-group/{uuid}")
    @Operation(summary = "Delete a timeseries groups")
    @ApiResponses(value = {@ApiResponse(responseCode = "200", description = "The timeseries group was successfully deleted")})
    public void deleteTimeseriesGroup(
        @PathVariable UUID uuid
    ) {
        timeseriesService.deleteTimeseriesGroup(uuid);
    }
}

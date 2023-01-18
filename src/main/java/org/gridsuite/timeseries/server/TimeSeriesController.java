/**
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
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

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
@Tag(name = "Time series server")
public class TimeSeriesController {

    private final TimeSeriesService timeSeriesService;

    public TimeSeriesController(TimeSeriesService timeSeriesService) {
        this.timeSeriesService = timeSeriesService;
    }

    @GetMapping(value = "/timeseries-group")
    @Operation(summary = "Get all time series groups ids")
    @ApiResponses(value = { @ApiResponse(responseCode = "200", description = "The list of time series groups ids") })
    public List<TimeSeriesGroupInfos> getAllTimeSeriesGroupsInfos() {
        return timeSeriesService.getAllTimeSeriesGroupsInfos();
    }

    @PostMapping(value = "/timeseries-group")
    @Operation(summary = "create a time series group")
    @ApiResponses(value = {@ApiResponse(responseCode = "200", description = "The time series group was successfully created")})
    //TODO better interface with springboot's objectmapper using the time series jackson in powsybl ?
    public TimeSeriesGroupInfos createTimeSeriesGroup(@RequestBody String timeSeries) {
        List<TimeSeries> list = TimeSeries.parseJson(timeSeries);
        return timeSeriesService.createTimeSeriesGroup(list);
    }

    @GetMapping(value = "/timeseries-group/{uuid}/metadata")
    public String getTimeSeriesGroupMetadata(@PathVariable UUID uuid) {
        return timeSeriesService.getTimeSeriesGroupMetadataJson(uuid);
    }

    @GetMapping(value = "/timeseries-group/{uuid}")
    @Operation(summary = "Get data of a time series groups")
    @ApiResponses(value = {@ApiResponse(responseCode = "200", description = "The list of time series groups")})
    public ResponseEntity<String> getTimeSeriesGroup(
        @PathVariable UUID uuid,
        //TODO more kinds of filters
        @RequestParam(required = false) boolean tryToCompress,
        @RequestParam(required = false) String time,
        @RequestParam(required = false) List<String> timeSeriesNames
    ) {
        List<TimeSeries> list = timeSeriesService.getTimeSeriesGroup(uuid, tryToCompress, time, timeSeriesNames);
        return ResponseEntity.ok().contentType(MediaType.APPLICATION_JSON).body(TimeSeries.toJson(list));
    }

    @DeleteMapping(value = "/timeseries-group/{uuid}")
    @Operation(summary = "Delete a time series groups")
    @ApiResponses(value = {@ApiResponse(responseCode = "200", description = "The time series group was successfully deleted")})
    public void deleteTimeSeriesGroup(
        @PathVariable UUID uuid
    ) {
        timeSeriesService.deleteTimeSeriesGroup(uuid);
    }
}

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pulsar.functions.worker.rest.api.v3;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.common.functions.UpdateOptionsImpl;
import org.apache.pulsar.common.io.ConfigFieldDefinition;
import org.apache.pulsar.common.io.ConnectorDefinition;
import org.apache.pulsar.common.io.SourceConfig;
import org.apache.pulsar.common.policies.data.SourceStatus;
import org.apache.pulsar.functions.worker.WorkerService;
import org.apache.pulsar.functions.worker.rest.FunctionApiResource;
import org.apache.pulsar.functions.worker.service.api.Sources;
import org.glassfish.jersey.media.multipart.FormDataContentDisposition;
import org.glassfish.jersey.media.multipart.FormDataParam;

@Slf4j
@Api(value = "/sources", description = "Sources admin apis", tags = "sources")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
@Path("/sources")
public class SourcesApiV3Resource extends FunctionApiResource {

    Sources<? extends WorkerService> sources() {
        return get().getSources();
    }

    @POST
    @Path("/{tenant}/{namespace}/{sourceName}")
    @Consumes(MediaType.MULTIPART_FORM_DATA)
    public void registerSource(final @PathParam("tenant") String tenant,
                                   final @PathParam("namespace") String namespace,
                                   final @PathParam("sourceName") String sourceName,
                                   final @FormDataParam("data") InputStream uploadedInputStream,
                                   final @FormDataParam("data") FormDataContentDisposition fileDetail,
                                   final @FormDataParam("url") String functionPkgUrl,
                                   final @FormDataParam("sourceConfig") SourceConfig sourceConfig) {

        sources().registerSource(tenant, namespace, sourceName, uploadedInputStream, fileDetail,
                functionPkgUrl, sourceConfig, authParams());

    }

    @PUT
    @Path("/{tenant}/{namespace}/{sourceName}")
    @Consumes(MediaType.MULTIPART_FORM_DATA)
    public void updateSource(final @PathParam("tenant") String tenant,
                             final @PathParam("namespace") String namespace,
                             final @PathParam("sourceName") String sourceName,
                             final @FormDataParam("data") InputStream uploadedInputStream,
                             final @FormDataParam("data") FormDataContentDisposition fileDetail,
                             final @FormDataParam("url") String functionPkgUrl,
                             final @FormDataParam("sourceConfig") SourceConfig sourceConfig,
                             final @FormDataParam("updateOptions") UpdateOptionsImpl updateOptions) {

        sources().updateSource(tenant, namespace, sourceName, uploadedInputStream, fileDetail,
                functionPkgUrl, sourceConfig, authParams(), updateOptions);
    }


    @DELETE
    @Path("/{tenant}/{namespace}/{sourceName}")
    public void deregisterSource(final @PathParam("tenant") String tenant,
                                 final @PathParam("namespace") String namespace,
                                 final @PathParam("sourceName") String sourceName) {
        sources().deregisterFunction(tenant, namespace, sourceName, authParams());
    }

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/{tenant}/{namespace}/{sourceName}")
    public SourceConfig getSourceInfo(final @PathParam("tenant") String tenant,
                                      final @PathParam("namespace") String namespace,
                                      final @PathParam("sourceName") String sourceName)
            throws IOException {
        return sources().getSourceInfo(tenant, namespace, sourceName, authParams());
    }

    @GET
    @ApiOperation(
            value = "Displays the status of a Pulsar Source instance",
            response = SourceStatus.SourceInstanceStatus.SourceInstanceStatusData.class
    )
    @ApiResponses(value = {
            @ApiResponse(code = 307, message = "Current broker doesn't serve the namespace of this source"),
            @ApiResponse(code = 400, message = "Invalid request"),
            @ApiResponse(code = 403, message = "The requester doesn't have admin permissions"),
            @ApiResponse(code = 404, message = "The source doesn't exist")
    })
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/{tenant}/{namespace}/{sourceName}/{instanceId}/status")
    public SourceStatus.SourceInstanceStatus.SourceInstanceStatusData getSourceInstanceStatus(
            final @PathParam("tenant") String tenant,
            final @PathParam("namespace") String namespace,
            final @PathParam("sourceName") String sourceName,
            final @PathParam("instanceId") String instanceId) throws IOException {
        return sources().getSourceInstanceStatus(tenant, namespace, sourceName, instanceId, uri.getRequestUri(),
                authParams());
    }

    @GET
    @ApiOperation(
            value = "Displays the status of a Pulsar Source running in cluster mode",
            response = SourceStatus.class
    )
    @ApiResponses(value = {
            @ApiResponse(code = 307, message = "Current broker doesn't serve the namespace of this source"),
            @ApiResponse(code = 400, message = "Invalid request"),
            @ApiResponse(code = 403, message = "The requester doesn't have admin permissions"),
            @ApiResponse(code = 404, message = "The source doesn't exist")
    })
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/{tenant}/{namespace}/{sourceName}/status")
    public SourceStatus getSourceStatus(final @PathParam("tenant") String tenant,
                                    final @PathParam("namespace") String namespace,
                                    final @PathParam("sourceName") String sourceName) throws IOException {
        return sources()
                .getSourceStatus(tenant, namespace, sourceName, uri.getRequestUri(), authParams());
    }

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/{tenant}/{namespace}")
    public List<String> listSources(final @PathParam("tenant") String tenant,
                                    final @PathParam("namespace") String namespace) {
        return sources().listFunctions(tenant, namespace, authParams());
    }

    @POST
    @ApiOperation(value = "Restart source instance", response = Void.class)
    @ApiResponses(value = {
            @ApiResponse(code = 307, message = "Current broker doesn't serve the namespace of this source"),
            @ApiResponse(code = 400, message = "Invalid request"),
            @ApiResponse(code = 404, message = "The function does not exist"),
            @ApiResponse(code = 500, message = "Internal server error")})
    @Path("/{tenant}/{namespace}/{sourceName}/{instanceId}/restart")
    @Consumes(MediaType.APPLICATION_JSON)
    public void restartSource(final @PathParam("tenant") String tenant,
                              final @PathParam("namespace") String namespace,
                              final @PathParam("sourceName") String sourceName,
                              final @PathParam("instanceId") String instanceId) {
        sources().restartFunctionInstance(tenant, namespace, sourceName, instanceId, this.uri.getRequestUri(),
                authParams());
    }

    @POST
    @ApiOperation(value = "Restart all source instances", response = Void.class)
    @ApiResponses(value = { @ApiResponse(code = 400, message = "Invalid request"),
    @ApiResponse(code = 404, message = "The function does not exist"),
    @ApiResponse(code = 500, message = "Internal server error") })
    @Path("/{tenant}/{namespace}/{sourceName}/restart")
    @Consumes(MediaType.APPLICATION_JSON)
    public void restartSource(final @PathParam("tenant") String tenant,
                              final @PathParam("namespace") String namespace,
                              final @PathParam("sourceName") String sourceName) {
        sources().restartFunctionInstances(tenant, namespace, sourceName, authParams());
    }

    @POST
    @ApiOperation(value = "Stop source instance", response = Void.class)
    @ApiResponses(value = {@ApiResponse(code = 400, message = "Invalid request"),
            @ApiResponse(code = 404, message = "The function does not exist"),
            @ApiResponse(code = 500, message = "Internal server error")})
    @Path("/{tenant}/{namespace}/{sourceName}/{instanceId}/stop")
    @Consumes(MediaType.APPLICATION_JSON)
    public void stopSource(final @PathParam("tenant") String tenant,
                           final @PathParam("namespace") String namespace,
                           final @PathParam("sourceName") String sourceName,
                           final @PathParam("instanceId") String instanceId) {
        sources().stopFunctionInstance(tenant, namespace, sourceName, instanceId, this.uri.getRequestUri(),
                authParams());
    }

    @POST
    @ApiOperation(value = "Stop all source instances", response = Void.class)
    @ApiResponses(value = {@ApiResponse(code = 400, message = "Invalid request"),
            @ApiResponse(code = 404, message = "The function does not exist"),
            @ApiResponse(code = 500, message = "Internal server error")})
    @Path("/{tenant}/{namespace}/{sourceName}/stop")
    @Consumes(MediaType.APPLICATION_JSON)
    public void stopSource(final @PathParam("tenant") String tenant,
                           final @PathParam("namespace") String namespace,
                           final @PathParam("sourceName") String sourceName) {
        sources().stopFunctionInstances(tenant, namespace, sourceName, authParams());
    }

    @POST
    @ApiOperation(value = "Start source instance", response = Void.class)
    @ApiResponses(value = {@ApiResponse(code = 400, message = "Invalid request"),
            @ApiResponse(code = 404, message = "The function does not exist"),
            @ApiResponse(code = 500, message = "Internal server error")})
    @Path("/{tenant}/{namespace}/{sourceName}/{instanceId}/start")
    @Consumes(MediaType.APPLICATION_JSON)
    public void startSource(final @PathParam("tenant") String tenant,
                            final @PathParam("namespace") String namespace,
                            final @PathParam("sourceName") String sourceName,
                            final @PathParam("instanceId") String instanceId) {
        sources().startFunctionInstance(tenant, namespace, sourceName, instanceId, this.uri.getRequestUri(),
                authParams());
    }

    @POST
    @ApiOperation(value = "Start all source instances", response = Void.class)
    @ApiResponses(value = {@ApiResponse(code = 400, message = "Invalid request"),
            @ApiResponse(code = 404, message = "The function does not exist"),
            @ApiResponse(code = 500, message = "Internal server error")})
    @Path("/{tenant}/{namespace}/{sourceName}/start")
    @Consumes(MediaType.APPLICATION_JSON)
    public void startSource(final @PathParam("tenant") String tenant,
                            final @PathParam("namespace") String namespace,
                            final @PathParam("sourceName") String sourceName) {
        sources().startFunctionInstances(tenant, namespace, sourceName, authParams());
    }

    @GET
    @ApiOperation(
            value = "Fetches a list of supported Pulsar IO source connectors currently running in cluster mode",
            response = List.class
    )
    @ApiResponses(value = {
            @ApiResponse(code = 403, message = "The requester doesn't have admin permissions"),
            @ApiResponse(code = 400, message = "Invalid request"),
            @ApiResponse(code = 408, message = "Request timeout")
    })
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/builtinsources")
    public List<ConnectorDefinition> getSourceList() {
        return sources().getSourceList();
    }

    @GET
    @ApiOperation(
            value = "Fetches information about config fields associated with the specified builtin source",
            response = ConfigFieldDefinition.class,
            responseContainer = "List"
    )
    @ApiResponses(value = {
            @ApiResponse(code = 403, message = "The requester doesn't have admin permissions"),
            @ApiResponse(code = 404, message = "builtin source does not exist"),
            @ApiResponse(code = 500, message = "Internal server error"),
            @ApiResponse(code = 503, message = "Function worker service is now initializing. Please try again later.")
    })
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/builtinsources/{name}/configdefinition")
    public List<ConfigFieldDefinition> getSourceConfigDefinition(
            @ApiParam(value = "The name of the builtin source") final @PathParam("name") String name)
            throws IOException {
        return sources().getSourceConfigDefinition(name);
    }

    @POST
    @ApiOperation(
            value = "Reload the built-in connectors, including Sources and Sinks",
            response = Void.class
    )
    @ApiResponses(value = {
            @ApiResponse(code = 401, message = "This operation requires super-user access"),
            @ApiResponse(code = 503, message = "Function worker service is now initializing. Please try again later."),
            @ApiResponse(code = 500, message = "Internal server error")
    })
    @Path("/reloadBuiltInSources")
    public void reloadSources() {
        sources().reloadConnectors(authParams());
    }
}

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
package org.apache.pulsar.functions.instance;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.Getter;
import lombok.Setter;
import org.apache.pulsar.client.api.ClientBuilder;
import org.apache.pulsar.common.io.ConnectorDefinition;
import org.apache.pulsar.common.nar.NarClassLoader;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.apache.pulsar.functions.api.Context;
import org.apache.pulsar.functions.api.Function;
import org.apache.pulsar.functions.api.SerDe;
import org.apache.pulsar.functions.proto.Function.FunctionDetails;
import org.apache.pulsar.functions.proto.Function.SinkSpec;
import org.apache.pulsar.functions.proto.InstanceCommunication;
import org.mockito.Mockito;
import org.apache.pulsar.functions.secretsprovider.EnvironmentBasedSecretsProvider;
import org.jetbrains.annotations.NotNull;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.lang.reflect.Method;
import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class JavaInstanceRunnableTest {

    static class IntegerSerDe implements SerDe<Integer> {
        @Override
        public Integer deserialize(byte[] input) {
            return null;
        }

        @Override
        public byte[] serialize(Integer input) {
            return new byte[0];
        }
    }

    private static InstanceConfig createInstanceConfig(String outputSerde) {
        FunctionDetails.Builder functionDetailsBuilder = FunctionDetails.newBuilder();
        if (outputSerde != null) {
            functionDetailsBuilder.setSink(SinkSpec.newBuilder().setSerDeClassName(outputSerde).build());
        }
        InstanceConfig instanceConfig = new InstanceConfig();
        instanceConfig.setFunctionDetails(functionDetailsBuilder.build());
        instanceConfig.setMaxBufferedTuples(1024);
        return instanceConfig;
    }

    private JavaInstanceRunnable createRunnable(String outputSerde) throws Exception {
        InstanceConfig config = createInstanceConfig(outputSerde);
        ClientBuilder clientBuilder = mock(ClientBuilder.class);
        when(clientBuilder.build()).thenReturn(null);
        JavaInstanceRunnable javaInstanceRunnable = new JavaInstanceRunnable(
                config, clientBuilder, null, null, null, null, null, null, null, null);
        return javaInstanceRunnable;
    }

    private Method makeAccessible(JavaInstanceRunnable javaInstanceRunnable) throws Exception {
        Method method = javaInstanceRunnable.getClass().getDeclaredMethod("setupSerDe", Class[].class, ClassLoader.class);
        method.setAccessible(true);
        return method;
    }

    @Getter
    @Setter
    private class ComplexUserDefinedType {
        private String name;
        private Integer age;
    }

    private class ComplexTypeHandler implements Function<String, ComplexUserDefinedType> {
        @Override
        public ComplexUserDefinedType process(String input, Context context) throws Exception {
            return new ComplexUserDefinedType();
        }
    }

    private class ComplexSerDe implements SerDe<ComplexUserDefinedType> {
        @Override
        public ComplexUserDefinedType deserialize(byte[] input) {
            return null;
        }

        @Override
        public byte[] serialize(ComplexUserDefinedType input) {
            return new byte[0];
        }
    }

    private class VoidInputHandler implements Function<Void, String> {
        @Override
        public String process(Void input, Context context) throws Exception {
            return new String("Interesting");
        }
    }

    private class VoidOutputHandler implements Function<String, Void> {
        @Override
        public Void process(String input, Context context) throws Exception {
            return null;
        }
    }

    @Test
    public void testStatsManagerNull() throws Exception {
        JavaInstanceRunnable javaInstanceRunnable = createRunnable(null);

        Assert.assertEquals(javaInstanceRunnable.getFunctionStatus().build(), InstanceCommunication.FunctionStatus.newBuilder().build());

        Assert.assertEquals(javaInstanceRunnable.getMetrics(), InstanceCommunication.MetricsData.newBuilder().build());
    }

    @Test
    public void testSinkConfigParsingPreservesOriginalType() throws Exception {
        final Map<String, Object> parsedConfig = JavaInstanceRunnable.augmentAndFilterConnectorConfig(
                "{\"ttl\": 9223372036854775807}",
                new InstanceConfig(),
                new EnvironmentBasedSecretsProvider(),
                null,
                FunctionDetails.ComponentType.SINK
        );
        Assert.assertEquals(parsedConfig.get("ttl").getClass(), Long.class);
        Assert.assertEquals(parsedConfig.get("ttl"), Long.MAX_VALUE);
    }

    @Test
    public void testSourceConfigParsingPreservesOriginalType() throws Exception {
        final Map<String, Object> parsedConfig = JavaInstanceRunnable.augmentAndFilterConnectorConfig(
                "{\"ttl\": 9223372036854775807}",
                new InstanceConfig(),
                new EnvironmentBasedSecretsProvider(),
                null,
                FunctionDetails.ComponentType.SOURCE
        );
        Assert.assertEquals(parsedConfig.get("ttl").getClass(), Long.class);
        Assert.assertEquals(parsedConfig.get("ttl"), Long.MAX_VALUE);
    }

    @DataProvider(name = "component")
    public Object[][] component() {
        return new Object[][]{
                // Schema: component type, whether to map in secrets
                { FunctionDetails.ComponentType.SINK },
                { FunctionDetails.ComponentType.SOURCE },
                { FunctionDetails.ComponentType.FUNCTION },
                { FunctionDetails.ComponentType.UNKNOWN },
        };
    }

    @Test(dataProvider = "component")
    public void testEmptyStringInput(FunctionDetails.ComponentType componentType) throws Exception {
        final Map<String, Object> parsedConfig = JavaInstanceRunnable.augmentAndFilterConnectorConfig(
                "",
                new InstanceConfig(),
                new EnvironmentBasedSecretsProvider(),
                null,
                componentType
        );
        Assert.assertEquals(parsedConfig.size(), 0);
    }

    // Environment variables are set in the pom.xml file
    @Test(dataProvider = "component")
    public void testInterpolatingEnvironmentVariables(FunctionDetails.ComponentType componentType) throws Exception {
        final Map<String, Object> parsedConfig = JavaInstanceRunnable.augmentAndFilterConnectorConfig(
                "{\"key\":{\"key1\":\"${TEST_JAVA_INSTANCE_PARSE_ENV_VAR}\",\"key2\":" +
                        "\"${unset-env-var}\"},\"key3\":\"${TEST_JAVA_INSTANCE_PARSE_ENV_VAR}\"}",
                new InstanceConfig(),
                new EnvironmentBasedSecretsProvider(),
                null,
                componentType
        );
        if ((componentType == FunctionDetails.ComponentType.SOURCE
                || componentType == FunctionDetails.ComponentType.SINK)) {
            Assert.assertEquals(((Map) parsedConfig.get("key")).get("key1"), "some-configuration");
            Assert.assertEquals(((Map) parsedConfig.get("key")).get("key2"), "${unset-env-var}");
            Assert.assertEquals(parsedConfig.get("key3"), "some-configuration");
        } else {
            Assert.assertEquals(((Map) parsedConfig.get("key")).get("key1"), "${TEST_JAVA_INSTANCE_PARSE_ENV_VAR}");
            Assert.assertEquals(((Map) parsedConfig.get("key")).get("key2"), "${unset-env-var}");
            Assert.assertEquals(parsedConfig.get("key3"), "${TEST_JAVA_INSTANCE_PARSE_ENV_VAR}");
        }
    }

    public static class ConnectorTestConfig1 {
        public String field1;
    }

    @DataProvider(name = "configIgnoreUnknownFields")
    public static Object[][] configIgnoreUnknownFields() {
        return new Object[][]{
                {false, FunctionDetails.ComponentType.SINK},
                {true, FunctionDetails.ComponentType.SINK},
                {false, FunctionDetails.ComponentType.SOURCE},
                {true, FunctionDetails.ComponentType.SOURCE}
        };
    }

    @Test(dataProvider = "configIgnoreUnknownFields")
    public void testSinkConfigIgnoreUnknownFields(boolean ignoreUnknownConfigFields,
                                                  FunctionDetails.ComponentType type) throws Exception {
        NarClassLoader narClassLoader = mock(NarClassLoader.class);
        final ConnectorDefinition connectorDefinition = new ConnectorDefinition();
        if (type == FunctionDetails.ComponentType.SINK) {
            connectorDefinition.setSinkConfigClass(ConnectorTestConfig1.class.getName());
        } else {
            connectorDefinition.setSourceConfigClass(ConnectorTestConfig1.class.getName());
        }
        when(narClassLoader.getServiceDefinition(any())).thenReturn(ObjectMapperFactory
                .getThreadLocal().writeValueAsString(connectorDefinition));
        final InstanceConfig instanceConfig = new InstanceConfig();
        instanceConfig.setIgnoreUnknownConfigFields(ignoreUnknownConfigFields);

        final Map<String, Object> parsedConfig = JavaInstanceRunnable.augmentAndFilterConnectorConfig(
                "{\"field1\": \"value\", \"field2\": \"value2\"}",
                instanceConfig,
                new EnvironmentBasedSecretsProvider(),
                narClassLoader,
                type
        );
        if (ignoreUnknownConfigFields) {
            Assert.assertEquals(parsedConfig.size(), 1);
            Assert.assertEquals(parsedConfig.get("field1"), "value");
        } else {
            Assert.assertEquals(parsedConfig.size(), 2);
            Assert.assertEquals(parsedConfig.get("field1"), "value");
            Assert.assertEquals(parsedConfig.get("field2"), "value2");
        }
    }

    public static class ConnectorTestConfig2 {
        public static int constantField = 1;
        public String field1;
        private long withGetter;
        @JsonIgnore
        private ConnectorTestConfig1 ignore;

        public long getWithGetter() {
            return withGetter;
        }
    }

    @Test
    public void testBeanPropertiesReader() throws Exception {
        final List<String> beanProperties = JavaInstanceRunnable.BeanPropertiesReader
                .getBeanProperties(ConnectorTestConfig2.class);
        Assert.assertEquals(new TreeSet<>(beanProperties), new TreeSet<>(Arrays.asList("field1", "withGetter")));
    }
}

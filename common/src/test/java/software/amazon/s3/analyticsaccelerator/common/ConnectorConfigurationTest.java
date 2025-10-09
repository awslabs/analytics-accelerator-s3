/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package software.amazon.s3.analyticsaccelerator.common;

import static org.junit.jupiter.api.Assertions.*;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;

@SuppressFBWarnings(
    value = "NP_NONNULL_PARAM_VIOLATION",
    justification = "We mean to pass nulls to checks")
public class ConnectorConfigurationTest {
  private static final String TEST_PREFIX = "s3.connector";
  private static final String MAP_PREFIX = "map";

  @Test
  void testConstructorWithMap() {
    ConnectorConfiguration configuration =
        new ConnectorConfiguration(getDefaultConfigurationMap(TEST_PREFIX), TEST_PREFIX);

    assertNotNull(configuration);
    assertEquals("stringConfigValue", configuration.getString("stringConfig", "randomString"));
  }

  @Test
  void testConstructorWithIterable() {
    ConnectorConfiguration configuration =
        new ConnectorConfiguration(getDefaultConfigurationIterator(TEST_PREFIX), TEST_PREFIX);

    assertNotNull(configuration);
    assertEquals("stringConfigValue", configuration.getString("stringConfig", "randomString"));
  }

  @Test
  void testConstructorWithNulls() {
    assertThrows(
        NullPointerException.class, () -> new ConnectorConfiguration((Map<String, String>) null));
    assertThrows(
        NullPointerException.class,
        () -> new ConnectorConfiguration((Map<String, String>) null, "Foo"));
    assertThrows(
        NullPointerException.class,
        () -> new ConnectorConfiguration(getDefaultConfigurationMap("Foo"), null));
    assertThrows(
        NullPointerException.class,
        () -> new ConnectorConfiguration((Iterable<Map.Entry<String, String>>) null, "Foo"));
    assertThrows(
        NullPointerException.class,
        () -> new ConnectorConfiguration(getDefaultConfigurationMap("Foo").entrySet(), null));
  }

  @Test
  void testMapPrefix() {
    ConnectorConfiguration configuration =
        new ConnectorConfiguration(
            getDefaultConfigurationMap(TEST_PREFIX + "." + MAP_PREFIX), TEST_PREFIX);
    ConnectorConfiguration mappedConfiguration = configuration.map(MAP_PREFIX);

    assertNotNull(mappedConfiguration);
    assertEquals(
        "stringConfigValue", mappedConfiguration.getString("stringConfig", "randomString"));
  }

  @Test
  void testFilterWorksWithMap() {
    Map<String, String> defaultMap = getDefaultConfigurationMap(TEST_PREFIX);
    defaultMap.put("key", "value");
    ConnectorConfiguration configuration = new ConnectorConfiguration(defaultMap, TEST_PREFIX);

    assertNotNull(configuration);
    assertEquals("randomString", configuration.getString("key", "randomString"));
  }

  @Test
  void testGetWithNullKey() {
    ConnectorConfiguration configuration = getDefaultConfiguration(TEST_PREFIX);
    assertThrows(NullPointerException.class, () -> configuration.getInt(null, 0));
    assertThrows(NullPointerException.class, () -> configuration.getRequiredInt(null));
    assertThrows(NullPointerException.class, () -> configuration.getLong(null, 0));
    assertThrows(NullPointerException.class, () -> configuration.getRequiredLong(null));
    assertThrows(NullPointerException.class, () -> configuration.getString(null, ""));
    assertThrows(NullPointerException.class, () -> configuration.getRequiredString(null));
    assertThrows(NullPointerException.class, () -> configuration.getDouble(null, 0));
    assertThrows(NullPointerException.class, () -> configuration.getRequiredDouble(null));
    assertThrows(NullPointerException.class, () -> configuration.getBoolean(null, true));
    assertThrows(NullPointerException.class, () -> configuration.getRequiredBoolean(null));
  }

  @Test
  void testReturnValues() {
    ConnectorConfiguration configuration = getDefaultConfiguration(TEST_PREFIX);
    assertEquals("1", configuration.getString("intConfig", "randomString"));
    assertEquals("1.0", configuration.getString("doubleConfig", "randomString"));
    assertEquals("1", configuration.getString("longConfig", "randomString"));
    assertEquals("true", configuration.getString("booleanConfig", "randomString"));
    assertEquals("stringConfigValue", configuration.getString("stringConfig", "randomString"));
  }

  @Test
  void testReturnValuesNoPrefix() {
    ConnectorConfiguration configuration = new ConnectorConfiguration(getDefaultNoPrefixMap());
    assertEquals("1", configuration.getString("intConfig", "randomString"));
    assertEquals("1.0", configuration.getString("doubleConfig", "randomString"));
    assertEquals("1", configuration.getString("longConfig", "randomString"));
    assertEquals("true", configuration.getString("booleanConfig", "randomString"));
    assertEquals("stringConfigValue", configuration.getString("stringConfig", "randomString"));
  }

  @Test
  void testReturnsDefault() {
    ConnectorConfiguration configuration = getDefaultConfiguration(TEST_PREFIX);
    assertEquals(configuration.getInt("KeyNotExists", 1), 1);
    assertEquals(configuration.getDouble("KeyNotExists", 1.0), 1.0);
    assertTrue(configuration.getBoolean("KeyNotExists", true));
    assertFalse(configuration.getBoolean("KeyNotExists", false));
    assertEquals(0L, configuration.getLong("KeyNotExists", 0));
    assertEquals(configuration.getString("KeyNotExists", "randomstring"), "randomstring");
  }

  @Test
  void testGetInt() {
    ConnectorConfiguration configuration = getDefaultConfiguration(TEST_PREFIX);
    int intConfig = configuration.getInt("intConfig", 0);
    assertEquals(1, intConfig);
  }

  @Test
  void testGetIntThrows() {
    ConnectorConfiguration configuration = getDefaultConfiguration(TEST_PREFIX);
    assertThrows(NumberFormatException.class, () -> configuration.getInt("stringConfig", 0));
  }

  @Test
  void testGetIntRequired() {
    ConnectorConfiguration configuration = getDefaultConfiguration(TEST_PREFIX);
    int intConfig = configuration.getRequiredInt("intConfig");
    assertEquals(1, intConfig);
  }

  @Test
  void testGetIntRequiredThrowsIfNotSet() {
    ConnectorConfiguration configuration = getDefaultConfiguration(TEST_PREFIX);
    assertThrows(IllegalArgumentException.class, () -> configuration.getRequiredInt("intConfig1"));
  }

  @Test
  void testGetLong() {
    ConnectorConfiguration configuration = getDefaultConfiguration(TEST_PREFIX);
    long longConfig = configuration.getLong("longConfig", 0);
    assertEquals(1, longConfig);
  }

  @Test
  void testGetLongThrows() {
    ConnectorConfiguration configuration = getDefaultConfiguration(TEST_PREFIX);
    assertThrows(NumberFormatException.class, () -> configuration.getLong("stringConfig", 0));
  }

  @Test
  void testGetLongRequired() {
    ConnectorConfiguration configuration = getDefaultConfiguration(TEST_PREFIX);
    long longConfig = configuration.getRequiredLong("longConfig");
    assertEquals(1, longConfig);
  }

  @Test
  void testGetLongRequiredThrowsIfNotSet() {
    ConnectorConfiguration configuration = getDefaultConfiguration(TEST_PREFIX);
    assertThrows(
        IllegalArgumentException.class, () -> configuration.getRequiredLong("longConfig1"));
  }

  @Test
  void testGetDouble() {
    ConnectorConfiguration configuration = getDefaultConfiguration(TEST_PREFIX);
    double doubleConfig = configuration.getDouble("doubleConfig", 0.0);
    assertEquals(1.0, doubleConfig);
  }

  @Test
  void testGetDoubleThrows() {
    ConnectorConfiguration configuration = getDefaultConfiguration(TEST_PREFIX);
    assertThrows(NumberFormatException.class, () -> configuration.getDouble("stringConfig", 0.0));
  }

  @Test
  void testGetRequiredDouble() {
    ConnectorConfiguration configuration = getDefaultConfiguration(TEST_PREFIX);
    double doubleConfig = configuration.getRequiredDouble("doubleConfig");
    assertEquals(1.0, doubleConfig);
  }

  @Test
  void testGetDoubleThrowsIfNotSet() {
    ConnectorConfiguration configuration = getDefaultConfiguration(TEST_PREFIX);
    assertThrows(
        IllegalArgumentException.class, () -> configuration.getRequiredDouble("stringConfig"));
  }

  @Test
  void testGetBoolean() {
    ConnectorConfiguration configuration = getDefaultConfiguration(TEST_PREFIX);
    boolean booleanConfig = configuration.getBoolean("booleanConfig", false);
    assertTrue(booleanConfig);
  }

  @Test
  void testGetBooleanReturnsFalse() {
    ConnectorConfiguration configuration = getDefaultConfiguration(TEST_PREFIX);
    assertFalse(configuration.getBoolean("stringConfig", true));
  }

  @Test
  void testGetRequiredBoolean() {
    ConnectorConfiguration configuration = getDefaultConfiguration(TEST_PREFIX);
    boolean booleanConfig = configuration.getRequiredBoolean("booleanConfig");
    assertTrue(booleanConfig);
  }

  @Test
  void testGetRequiredBooleanThrowsIfNotSet() {
    ConnectorConfiguration configuration = getDefaultConfiguration(TEST_PREFIX);
    assertThrows(
        IllegalArgumentException.class, () -> configuration.getRequiredBoolean("booleanConfig1"));
  }

  @Test
  void testGetString() {
    ConnectorConfiguration configuration = getDefaultConfiguration(TEST_PREFIX);
    String stringConfig = configuration.getString("stringConfig", "randomString");
    assertInstanceOf(String.class, stringConfig);
    assertEquals("stringConfigValue", stringConfig);
  }

  @Test
  void testGetRequiredString() {
    ConnectorConfiguration configuration = getDefaultConfiguration(TEST_PREFIX);
    String stringConfig = configuration.getRequiredString("stringConfig");
    assertInstanceOf(String.class, stringConfig);
    assertEquals("stringConfigValue", stringConfig);
  }

  @Test
  void testGetRequiredStringThrowsIfNotSet() {
    ConnectorConfiguration configuration = getDefaultConfiguration(TEST_PREFIX);
    assertThrows(
        IllegalArgumentException.class, () -> configuration.getRequiredString("stringConfig1"));
  }

  @Test
  void testGetPositiveLong() {
    ConnectorConfiguration configuration = getDefaultConfiguration(TEST_PREFIX);
    long positiveLong = configuration.getPositiveLong("longConfig", 5);
    assertEquals(1, positiveLong);
  }

  @Test
  void testGetPositiveLongWithDefault() {
    ConnectorConfiguration configuration = getDefaultConfiguration(TEST_PREFIX);
    long positiveLong = configuration.getPositiveLong("nonExistentKey", 10);
    assertEquals(10, positiveLong);
  }

  @Test
  void testGetPositiveLongThrowsOnZero() {
    Map<String, String> configMap = new HashMap<>();
    configMap.put(TEST_PREFIX + ".zeroConfig", "0");
    ConnectorConfiguration configuration = new ConnectorConfiguration(configMap, TEST_PREFIX);
    assertThrows(
        IllegalArgumentException.class, () -> configuration.getPositiveLong("zeroConfig", 5));
  }

  @Test
  void testGetPositiveLongThrowsOnNegative() {
    Map<String, String> configMap = new HashMap<>();
    configMap.put(TEST_PREFIX + ".negativeConfig", "-5");
    ConnectorConfiguration configuration = new ConnectorConfiguration(configMap, TEST_PREFIX);
    assertThrows(
        IllegalArgumentException.class, () -> configuration.getPositiveLong("negativeConfig", 5));
  }

  @Test
  void testGetPositiveLongThrowsOnNegativeDefault() {
    ConnectorConfiguration configuration = getDefaultConfiguration(TEST_PREFIX);
    assertThrows(
        IllegalArgumentException.class, () -> configuration.getPositiveLong("nonExistentKey", -1));
  }

  private static ConnectorConfiguration getDefaultConfiguration(String prefix) {

    return new ConnectorConfiguration(getDefaultConfigurationMap(prefix), prefix);
  }

  private static Map<String, String> getDefaultConfigurationMap(String prefix) {
    Map<String, String> defaultMap = new HashMap<>();
    defaultMap.put(prefix + ".intConfig", "1");
    defaultMap.put(prefix + ".doubleConfig", "1.0");
    defaultMap.put(prefix + ".longConfig", "1");
    defaultMap.put(prefix + ".booleanConfig", "true");
    defaultMap.put(prefix + ".stringConfig", "stringConfigValue");
    return defaultMap;
  }

  private static Map<String, String> getDefaultNoPrefixMap() {
    Map<String, String> defaultMap = new HashMap<>();
    defaultMap.put("intConfig", "1");
    defaultMap.put("doubleConfig", "1.0");
    defaultMap.put("longConfig", "1");
    defaultMap.put("booleanConfig", "true");
    defaultMap.put("stringConfig", "stringConfigValue");
    return defaultMap;
  }

  private static Set<Map.Entry<String, String>> getDefaultConfigurationIterator(String prefix) {
    return getDefaultConfigurationMap(prefix).entrySet();
  }
}

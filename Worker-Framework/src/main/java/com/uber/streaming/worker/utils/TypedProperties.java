/*
 * Copyright (C) 2015-2019 Uber Technologies, Inc. (streaming-data@uber.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.uber.streaming.worker.utils;

import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * TypedProperties extends Properties to provide type converting methods
 */
public class TypedProperties extends Properties {

  private static final Logger LOGGER = LoggerFactory.getLogger(TypedProperties.class);

  public int getInt(String key) {
    return getInt(key, null);
  }

  public int getInt(String key, Integer defaultValue) {
    try {
      return Integer.parseInt(super.getProperty(key, String.valueOf(defaultValue)));
    } catch (Exception e) {
      if (defaultValue == null) {
        LOGGER.error("convert {}:{} to int failed", key, values(), e);
      }
      return defaultValue.intValue();
    }
  }

}

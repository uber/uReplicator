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
package com.uber.stream.kafka.mirrormaker.common;

public final class Constants {

  private Constants() {
  }

  public static final String HELIX_OFFLINE_STATE = "OFFLINE";
  public static final String HELIX_ONLINE_STATE = "ONLINE";


  // for store AUTO_SCALING/AUTO_BALANCING in Helix, like AutoScaling:enable, AutoBalancing:disable
  public static final String ENABLE = "enable";
  public static final String DISABLE = "disable";
  public static final String AUTO_SCALING = "AutoScaling";
  public static final String AUTO_BALANCING = "AutoBalancing";

}

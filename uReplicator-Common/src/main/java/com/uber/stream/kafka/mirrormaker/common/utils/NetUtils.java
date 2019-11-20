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
package com.uber.stream.kafka.mirrormaker.common.utils;

import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Enumeration;


/**
 * @author Tboy
 */
public class NetUtils {

  public static String getFirstNoLoopbackIP4Address() {
    Collection<String> allNoLoopbackIP4Addresses = getNoLoopbackIP4Addresses();
    if (allNoLoopbackIP4Addresses.isEmpty()) {
      return null;
    }
    return allNoLoopbackIP4Addresses.iterator().next();
  }

  public static Collection<String> getNoLoopbackIP4Addresses() {
    Collection<String> noLoopbackIP4Addresses = new ArrayList<>();
    Collection<InetAddress> allInetAddresses = getAllHostAddress();

    for (InetAddress address : allInetAddresses) {
      if (!address.isLoopbackAddress() && !address.isSiteLocalAddress()
          && !Inet6Address.class.isInstance(address)) {
        noLoopbackIP4Addresses.add(address.getHostAddress());
      }
    }
    if (noLoopbackIP4Addresses.isEmpty()) {
      for (InetAddress address : allInetAddresses) {
        if (!address.isLoopbackAddress() && !Inet6Address.class.isInstance(address)) {
          noLoopbackIP4Addresses.add(address.getHostAddress());
        }
      }
    }
    return noLoopbackIP4Addresses;
  }

  public static Collection<InetAddress> getAllHostAddress() {
    try {
      Enumeration<NetworkInterface> networkInterfaces = NetworkInterface.getNetworkInterfaces();
      Collection<InetAddress> addresses = new ArrayList<>();

      while (networkInterfaces.hasMoreElements()) {
        NetworkInterface networkInterface = networkInterfaces.nextElement();
        Enumeration<InetAddress> inetAddresses = networkInterface.getInetAddresses();
        while (inetAddresses.hasMoreElements()) {
          InetAddress inetAddress = inetAddresses.nextElement();
          addresses.add(inetAddress);
        }
      }
      return addresses;
    } catch (SocketException e) {
      throw new RuntimeException(e.getMessage(), e);
    }
  }
}

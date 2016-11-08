/**
 * Copyright (C) 2015-2016 Uber Technology Inc. (streaming-core@uber.com)
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
package com.uber.stream.kafka.mirrormaker.controller.rest;

import org.restlet.Application;
import org.restlet.Context;
import org.restlet.Restlet;
import org.restlet.routing.Router;
import org.restlet.routing.Template;

import com.uber.stream.kafka.mirrormaker.controller.rest.resources.AdminRestletResource;
import com.uber.stream.kafka.mirrormaker.controller.rest.resources.HealthCheckRestletResource;
import com.uber.stream.kafka.mirrormaker.controller.rest.resources.TopicManagementRestletResource;
import com.uber.stream.kafka.mirrormaker.controller.rest.resources.ValidationRestletResource;

/**
 * Register different REST endpoints
 */
public class ControllerRestApplication extends Application {

  public ControllerRestApplication(Context context) {
    super(context);
  }

  @Override
  public Restlet createInboundRoot() {
    final Router router = new Router(getContext());
    router.setDefaultMatchingMode(Template.MODE_EQUALS);

    // Topic Servlet
    router.attach("/topics", TopicManagementRestletResource.class);
    router.attach("/topics/", TopicManagementRestletResource.class);
    router.attach("/topics/{topicName}", TopicManagementRestletResource.class);
    router.attach("/topics/{topicName}/", TopicManagementRestletResource.class);

    // Health Check Servlet
    router.attach("/health", HealthCheckRestletResource.class);
    router.attach("/health/", HealthCheckRestletResource.class);

    // Validation Servlet
    router.attach("/validation", ValidationRestletResource.class);
    router.attach("/validation/", ValidationRestletResource.class);
    router.attach("/validation/{option}", ValidationRestletResource.class);
    router.attach("/validation/{option}/", ValidationRestletResource.class);

    // Admin Servlet
    router.attach("/admin", AdminRestletResource.class);
    router.attach("/admin/{opt}", AdminRestletResource.class);

    return router;
  }
}

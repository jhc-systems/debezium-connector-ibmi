/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package com.fnz.logging.structured;

import org.apache.logging.log4j.core.config.plugins.Plugin;
import org.apache.logging.log4j.core.config.plugins.PluginFactory;
import org.apache.logging.log4j.layout.template.json.resolver.EventResolver;
import org.apache.logging.log4j.layout.template.json.resolver.EventResolverContext;
import org.apache.logging.log4j.layout.template.json.resolver.EventResolverFactory;
import org.apache.logging.log4j.layout.template.json.resolver.TemplateResolverConfig;
import org.apache.logging.log4j.layout.template.json.resolver.TemplateResolverFactory;

@Plugin(name = "StructuredMessageResolverFactory", category = TemplateResolverFactory.CATEGORY)
public final class StructuredMessageResolverFactory implements EventResolverFactory {

    private static final StructuredMessageResolverFactory INSTANCE = new StructuredMessageResolverFactory();

    private StructuredMessageResolverFactory() {
    }

    @PluginFactory
    public static StructuredMessageResolverFactory getInstance() {
        return INSTANCE;
    }

    @Override
    public String getName() {
        return StructuredMessageResolver.getName();
    }

    @Override
    public EventResolver create(EventResolverContext context, TemplateResolverConfig config) {
        return StructuredMessageResolver.getInstance();
    }

}
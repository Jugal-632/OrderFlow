package com.orderflow.order.config;

import com.fasterxml.jackson.databind.*;
import org.springframework.boot.jackson2.autoconfigure.Jackson2ObjectMapperBuilderCustomizer;
import org.springframework.context.annotation.*;

@Configuration
public class StrictRequestJsonConfig {
    @Bean Jackson2ObjectMapperBuilderCustomizer strictRequests() {
        return builder->builder.featuresToEnable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES,
                DeserializationFeature.FAIL_ON_NULL_FOR_PRIMITIVES,com.fasterxml.jackson.core.JsonParser.Feature.STRICT_DUPLICATE_DETECTION)
                .featuresToDisable(DeserializationFeature.ACCEPT_FLOAT_AS_INT,MapperFeature.ALLOW_COERCION_OF_SCALARS);
    }
}

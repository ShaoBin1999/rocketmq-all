package com.bsren.rocketmq.logging;

import java.util.concurrent.ConcurrentHashMap;

public abstract class InternalLoggerFactory {

    public static final String LOGGER_SLF4J = "slf4j";

    public static final String LOGGER_INNER = "inner";

    private static String loggerType = null;

    public static final String DEFAULT_LOGGER = LOGGER_SLF4J;


    private static ConcurrentHashMap<String, InternalLoggerFactory> loggerFactoryCache =
            new ConcurrentHashMap<>();

    public static InternalLogger getLogger(String name) {
        return getLoggerFactory().getLoggerInstance(name);
    }


    private static InternalLoggerFactory getLoggerFactory() {
        InternalLoggerFactory internalLoggerFactory = null;
        if (loggerType != null) {
            internalLoggerFactory = loggerFactoryCache.get(loggerType);
        }
        if (internalLoggerFactory == null) {
            internalLoggerFactory = loggerFactoryCache.get(DEFAULT_LOGGER);
        }
        if (internalLoggerFactory == null) {
            internalLoggerFactory = loggerFactoryCache.get(LOGGER_INNER);
        }
        if (internalLoggerFactory == null) {
            throw new RuntimeException("[RocketMQ] Logger init failed, please check logger");
        }
        return internalLoggerFactory;
    }


    protected abstract InternalLogger getLoggerInstance(String name);

}

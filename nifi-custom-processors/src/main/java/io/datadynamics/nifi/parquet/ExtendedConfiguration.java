package io.datadynamics.nifi.parquet;

import org.apache.hadoop.conf.Configuration;
import org.apache.nifi.logging.ComponentLog;
import org.slf4j.Logger;

import java.lang.ref.WeakReference;
import java.util.Collections;
import java.util.Map;
import java.util.WeakHashMap;
import java.util.function.BiConsumer;

/**
 * Extending Hadoop Configuration to prevent it from caching classes that can't be found. Since users may be
 * adding additional JARs to the classpath we don't want them to have to restart the JVM to be able to load
 * something that was previously not found, but might now be available.
 * <p>
 * Reference the original getClassByNameOrNull from Configuration.
 */
public class ExtendedConfiguration extends Configuration {

    private final BiConsumer<String, Throwable> loggerMethod;
    private final Map<ClassLoader, Map<String, WeakReference<Class<?>>>> CACHE_CLASSES = new WeakHashMap<>();

    public ExtendedConfiguration(final Logger logger) {
        this.loggerMethod = logger::error;
    }

    public ExtendedConfiguration(final ComponentLog logger) {
        this.loggerMethod = logger::error;
    }

    @Override
    public Class<?> getClassByNameOrNull(String name) {
        final ClassLoader classLoader = getClassLoader();

        Map<String, WeakReference<Class<?>>> map;
        synchronized (CACHE_CLASSES) {
            map = CACHE_CLASSES.get(classLoader);
            if (map == null) {
                map = Collections.synchronizedMap(new WeakHashMap<>());
                CACHE_CLASSES.put(classLoader, map);
            }
        }

        Class<?> clazz = null;
        WeakReference<Class<?>> ref = map.get(name);
        if (ref != null) {
            clazz = ref.get();
        }

        if (clazz == null) {
            try {
                clazz = Class.forName(name, true, classLoader);
            } catch (ClassNotFoundException e) {
                loggerMethod.accept(e.getMessage(), e);
                return null;
            }
            // two putters can race here, but they'll put the same class
            map.put(name, new WeakReference<>(clazz));
            return clazz;
        } else {
            // cache hit
            return clazz;
        }
    }

}


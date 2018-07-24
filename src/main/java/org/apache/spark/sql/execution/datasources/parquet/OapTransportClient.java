package org.apache.spark.sql.execution.datasources.parquet;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.network.NetworkModule;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.reindex.ReindexPlugin;
import org.elasticsearch.join.ParentJoinPlugin;
import org.elasticsearch.percolator.PercolatorPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.script.mustache.MustachePlugin;
import org.elasticsearch.transport.Netty4Plugin;

import io.netty.util.ThreadDeathWatcher;
import io.netty.util.concurrent.GlobalEventExecutor;

public class OapTransportClient extends TransportClient {

		/**
		 * Netty wants to do some unwelcome things like use unsafe and replace a private field, or use a poorly considered
		 * buffer recycler. This method disables these things by default, but can be overridden by setting the corresponding
		 * system properties.
		 */
		private static void initializeNetty() {
			/*
			 * We disable three pieces of Netty functionality here:
			 *  - we disable Netty from being unsafe
			 *  - we disable Netty from replacing the selector key set
			 *  - we disable Netty from using the recycler
			 *
			 * While permissions are needed to read and set these, the permissions needed here are innocuous and thus should simply be granted
			 * rather than us handling a security exception here.
			 */
			setSystemPropertyIfUnset("io.netty.noUnsafe", Boolean.toString(true));
			setSystemPropertyIfUnset("io.netty.noKeySetOptimization", Boolean.toString(true));
			setSystemPropertyIfUnset("io.netty.recycler.maxCapacityPerThread", Integer.toString(0));
		}

		private static void setSystemPropertyIfUnset(final String key, final String value) {
			final String currentValue = System.getProperty(key);
			if (currentValue == null) {
				System.setProperty(key, value);
			}
		}

		private static final List<String> OPTIONAL_DEPENDENCIES = Arrays.asList( //
				"org.elasticsearch.transport.Netty3Plugin", //
				"org.elasticsearch.transport.Netty4Plugin");

		private static final Collection<Class<? extends Plugin>> PRE_INSTALLED_PLUGINS;

		static {

			initializeNetty();

			List<Class<? extends Plugin>> plugins = new ArrayList<>();

			plugins.add(Netty4Plugin.class);
			plugins.add(ReindexPlugin.class);
			plugins.add(PercolatorPlugin.class);
			plugins.add(MustachePlugin.class);
			plugins.add(ParentJoinPlugin.class);

			PRE_INSTALLED_PLUGINS = Collections.unmodifiableList(plugins);
		}

		public OapTransportClient(Settings settings) {
			super(settings, PRE_INSTALLED_PLUGINS);
		}

		@Override
		public void close() {
			super.close();
			if (NetworkModule.TRANSPORT_TYPE_SETTING.exists(settings) == false
					|| NetworkModule.TRANSPORT_TYPE_SETTING.get(settings).equals(Netty4Plugin.NETTY_TRANSPORT_NAME)) {
				try {
					GlobalEventExecutor.INSTANCE.awaitInactivity(5, TimeUnit.SECONDS);
				} catch (InterruptedException e) {
					Thread.currentThread().interrupt();
				}
				try {
					ThreadDeathWatcher.awaitInactivity(5, TimeUnit.SECONDS);
				} catch (InterruptedException e) {
					Thread.currentThread().interrupt();
				}
			}
		}
	}
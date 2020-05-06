/*
 * Copyright 2016-2019 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.integration.dsl;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.springframework.context.SmartLifecycle;
import org.springframework.messaging.MessageChannel;

/**
 * The standard implementation of the {@link IntegrationFlow} interface instantiated
 * by the Framework. Represents a logical container for the components configured
 * for the integration flow. It can be treated as a single component, especially
 * when declaring dynamically, using the
 * {@link org.springframework.integration.dsl.context.IntegrationFlowContext}.
 * <p>
 * Being the logical container for the target integration components, this class controls
 * the lifecycle of all those components, when its {@code start()} and {@code stop()} are
 * invoked.
 * <p>
 * This component is never {@code autoStartup}, because all the components are
 * registered as beans in the application context and their initial start up phase is
 * controlled from the lifecycle processor automatically.
 * <p>
 * However, when we register an {@link IntegrationFlow} dynamically using the
 * {@link org.springframework.integration.dsl.context.IntegrationFlowContext} API,
 * the lifecycle processor from the application context is not involved;
 * therefore we should control the lifecycle of the beans manually, or rely on the
 * {@link org.springframework.integration.dsl.context.IntegrationFlowContext} API.
 * Its created registration <b>is</b> {@code autoStartup} by default and
 * starts the flow when it is registered. If you disable the registration's auto-
 * startup behavior, you are responsible for starting the flow or its component
 * beans.
 * <p>
 * This component doesn't track its {@code running} state during {@code stop()} action
 * and delegates directly to stop the registered components, to avoid dangling processes
 * after a registered {@link IntegrationFlow} is removed from the flow context.
 *
 * @author Artem Bilan
 *
 * @since 5.0
 *
 * @see IntegrationFlows
 * @see org.springframework.integration.dsl.context.IntegrationFlowBeanPostProcessor
 * @see org.springframework.integration.dsl.context.IntegrationFlowContext
 * IntegrationFlowBuilder 构建的最终结果
 */
public class StandardIntegrationFlow implements IntegrationFlow, SmartLifecycle {

	/**
	 * 将会被构建的一组 component
	 */
	private final Map<Object, String> integrationComponents;

	/**
	 * 本此包含的所有 component 中实现 SmartLifecycle接口的对象
	 */
	private final List<SmartLifecycle> lifecycles = new LinkedList<>();

	/**
	 * 最上游的数据管道
	 */
	private MessageChannel inputChannel;

	private boolean running;

	StandardIntegrationFlow(Map<Object, String> integrationComponents) {
		this.integrationComponents = new LinkedHashMap<>(integrationComponents);
	}

	@Override
	public void configure(IntegrationFlowDefinition<?> flow) {
		throw new UnsupportedOperationException();
	}

	@Override
	public MessageChannel getInputChannel() {
		if (this.inputChannel == null) {
			this.inputChannel =
					this.integrationComponents.keySet()
							.stream()
							.filter(MessageChannel.class::isInstance)
							.map(MessageChannel.class::cast)
							.findFirst()
							.orElseThrow(() -> new IllegalStateException("The 'IntegrationFlow' [" + this + "] " +
									"doesn't start with 'MessageChannel' for direct message sending."));
		}

		return this.inputChannel;
	}

	public void setIntegrationComponents(Map<Object, String> integrationComponents) {
		this.integrationComponents.clear();
		this.integrationComponents.putAll(integrationComponents);
	}

	public Map<Object, String> getIntegrationComponents() {
		return Collections.unmodifiableMap(this.integrationComponents);
	}

	/**
	 * 执行整个管道链
	 */
	@Override
	public void start() {
		if (!this.running) {
			List<Object> components = new LinkedList<>(this.integrationComponents.keySet());
			ListIterator<Object> iterator = components.listIterator(this.integrationComponents.size());
			this.lifecycles.clear();
			// 返回的迭代器是一个双向迭代器  根据传入的 size 作为偏移量 其余数据会追加到前面后者后面
			// 当传入长度就是 指定长度时 就是一个往前迭代的迭代器
			while (iterator.hasPrevious()) {
				// 按照从下往上的顺序启动
				Object component = iterator.previous();
				if (component instanceof SmartLifecycle) {
					this.lifecycles.add((SmartLifecycle) component);
					((SmartLifecycle) component).start();
				}
			}
			this.running = true;
		}
	}

	@Override
	public void stop(Runnable callback) {
		if (this.lifecycles.size() > 0) {
			AggregatingCallback aggregatingCallback = new AggregatingCallback(this.lifecycles.size(), callback);
			ListIterator<SmartLifecycle> iterator = this.lifecycles.listIterator(this.lifecycles.size());
			while (iterator.hasPrevious()) {
				SmartLifecycle lifecycle = iterator.previous();
				if (lifecycle.isRunning()) {
					lifecycle.stop(aggregatingCallback);
				}
				else {
					aggregatingCallback.run();
				}
			}
		}
		else {
			callback.run();
		}
		this.running = false;
	}

	@Override
	public void stop() {
		ListIterator<SmartLifecycle> iterator = this.lifecycles.listIterator(this.lifecycles.size());
		while (iterator.hasPrevious()) {
			iterator.previous().stop();
		}
		this.running = false;
	}

	@Override
	public boolean isRunning() {
		return this.running;
	}

	@Override
	public boolean isAutoStartup() {
		return false;
	}

	@Override
	public int getPhase() {
		return 0;
	}

	@Override
	public String toString() {
		return "StandardIntegrationFlow{integrationComponents=" + this.integrationComponents + '}';
	}

	private static final class AggregatingCallback implements Runnable {

		private final AtomicInteger count;

		private final Runnable finishCallback;

		AggregatingCallback(int count, Runnable finishCallback) {
			this.count = new AtomicInteger(count);
			this.finishCallback = finishCallback;
		}

		@Override
		public void run() {
			if (this.count.decrementAndGet() <= 0) {
				this.finishCallback.run();
			}
		}

	}

}

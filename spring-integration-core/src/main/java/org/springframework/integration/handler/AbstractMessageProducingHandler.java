/*
 * Copyright 2014-2019 the original author or authors.
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

package org.springframework.integration.handler;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.reactivestreams.Publisher;

import org.springframework.core.ReactiveAdapter;
import org.springframework.core.ReactiveAdapterRegistry;
import org.springframework.integration.IntegrationMessageHeaderAccessor;
import org.springframework.integration.channel.ReactiveStreamsSubscribableChannel;
import org.springframework.integration.context.IntegrationContextUtils;
import org.springframework.integration.core.MessageProducer;
import org.springframework.integration.core.MessagingTemplate;
import org.springframework.integration.routingslip.RoutingSlipRouteStrategy;
import org.springframework.integration.support.AbstractIntegrationMessageBuilder;
import org.springframework.integration.support.utils.IntegrationUtils;
import org.springframework.lang.Nullable;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHandlingException;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.MessagingException;
import org.springframework.messaging.core.DestinationResolutionException;
import org.springframework.messaging.support.ErrorMessage;
import org.springframework.util.Assert;
import org.springframework.util.ObjectUtils;
import org.springframework.util.StringUtils;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;
import org.springframework.util.concurrent.SettableListenableFuture;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * The base {@link AbstractMessageHandler} implementation for the {@link MessageProducer}.
 *
 * @author David Liu
 * @author Artem Bilan
 * @author Gary Russell
 * @author Marius Bogoevici
 *
 * since 4.1
 * 该抽象类 相当于是一个桥梁 向上处理接收到的消息 同时将结果发往下游
 */
public abstract class AbstractMessageProducingHandler extends AbstractMessageHandler
		implements MessageProducer, HeaderPropagationAware {

	/**
	 * 该对象是用于简化发送的操作
	 */
	protected final MessagingTemplate messagingTemplate = new MessagingTemplate(); // NOSONAR final

	private boolean async;

	@Nullable
	private String outputChannelName;

	@Nullable
	private MessageChannel outputChannel;

	/**
	 * 某些被强制声明的 请求头是不需要被传播的
	 */
	private String[] notPropagatedHeaders;

	private boolean selectiveHeaderPropagation;

	/**
	 * 代表禁止传播消息头
	 */
	private boolean noHeadersPropagation;

	/**
	 * Set the timeout for sending reply Messages.
	 * @param sendTimeout The send timeout.
	 */
	public void setSendTimeout(long sendTimeout) {
		this.messagingTemplate.setSendTimeout(sendTimeout);
	}

	@Override
	public void setOutputChannel(MessageChannel outputChannel) {
		this.outputChannel = outputChannel;
	}

	@Override
	public void setOutputChannelName(String outputChannelName) {
		Assert.hasText(outputChannelName, "'outputChannelName' must not be empty");
		this.outputChannelName = outputChannelName; //NOSONAR (inconsistent sync)
	}

	/**
	 * Allow async replies. If the handler reply is a {@link ListenableFuture}, send
	 * the output when it is satisfied rather than sending the future as the result.
	 * Ignored for return types other than {@link ListenableFuture}.
	 * @param async true to allow.
	 * @since 4.3
	 */
	public final void setAsync(boolean async) {
		this.async = async;
	}

	/**
	 * @see #setAsync(boolean)
	 * @return true if this handler supports async replies.
	 * @since 4.3
	 */
	protected boolean isAsync() {
		return this.async;
	}

	/**
	 * Set header patterns ("xxx*", "*xxx", "*xxx*" or "xxx*yyy")
	 * that will NOT be copied from the inbound message if
	 * {@link #shouldCopyRequestHeaders() shouldCopyRequestHeaaders} is true.
	 * At least one pattern as "*" means do not copy headers at all.
	 * @param headers the headers to not propagate from the inbound message.
	 * @since 4.3.10
	 * @see org.springframework.util.PatternMatchUtils
	 */
	@Override
	public void setNotPropagatedHeaders(String... headers) {
		updateNotPropagatedHeaders(headers, false);
	}

	/**
	 * Set or replace not propagated headers. Exposed so that subclasses can set specific
	 * headers in a constructor, since {@link #setNotPropagatedHeaders(String...)} is not
	 * final.
	 * @param headers Header patterns to not propagate.
	 * @param merge true to merge with existing patterns; false to replace.   代表增强更新 还是全量更新
	 * @since 5.0.2
	 */
	protected final void updateNotPropagatedHeaders(String[] headers, boolean merge) {
		Set<String> headerPatterns = new HashSet<>();

		if (merge && this.notPropagatedHeaders != null) {
			headerPatterns.addAll(Arrays.asList(this.notPropagatedHeaders));
		}

		if (!ObjectUtils.isEmpty(headers)) {
			Assert.noNullElements(headers, "null elements are not allowed in 'headers'");

			headerPatterns.addAll(Arrays.asList(headers));

			this.notPropagatedHeaders = headerPatterns.toArray(new String[0]);
		}

		if (headerPatterns.contains("*")) {
			this.notPropagatedHeaders = new String[] { "*" };
			this.noHeadersPropagation = true;
		}

		this.selectiveHeaderPropagation = !ObjectUtils.isEmpty(this.notPropagatedHeaders);
	}

	/**
	 * Get the header patterns this handler doesn't propagate.
	 * @return an immutable {@link java.util.Collection} of headers that will not be
	 * copied from the inbound message if {@link #shouldCopyRequestHeaders()} is true.
	 * @since 4.3.10
	 * @see #setNotPropagatedHeaders(String...)
	 * @see org.springframework.util.PatternMatchUtils
	 */
	@Override
	public Collection<String> getNotPropagatedHeaders() {
		return this.notPropagatedHeaders != null
				? Collections.unmodifiableSet(new HashSet<>(Arrays.asList(this.notPropagatedHeaders)))
				: Collections.emptyList();
	}

	/**
	 * Add header patterns ("xxx*", "*xxx", "*xxx*" or "xxx*yyy")
	 * that will NOT be copied from the inbound message if
	 * {@link #shouldCopyRequestHeaders()} is true, instead of overwriting the existing
	 * set.
	 * @param headers the headers to not propagate from the inbound message.
	 * @since 4.3.10
	 * @see #setNotPropagatedHeaders(String...)
	 */
	@Override
	public void addNotPropagatedHeaders(String... headers) {
		updateNotPropagatedHeaders(headers, true);
	}

	@Override
	protected void onInit() {
		super.onInit();
		Assert.state(!(this.outputChannelName != null && this.outputChannel != null), //NOSONAR (inconsistent sync)
				"'outputChannelName' and 'outputChannel' are mutually exclusive.");
		if (getBeanFactory() != null) {
			this.messagingTemplate.setBeanFactory(getBeanFactory());
		}
		this.messagingTemplate.setDestinationResolver(getChannelResolver());
	}

	/**
	 * 获取下游管道  如果设置的是 channelName  尝试从 beanFactory中获取对应的bean
	 * @return
	 */
	@Override
	@Nullable
	public MessageChannel getOutputChannel() {
		String channelName = this.outputChannelName;
		if (channelName != null) {
			this.outputChannel = getChannelResolver().resolveDestination(channelName);
			this.outputChannelName = null;
		}
		return this.outputChannel;
	}

	/**
	 * 将处理后的结果发往下游
	 * @param result
	 * @param requestMessage
	 */
	protected void sendOutputs(Object result, Message<?> requestMessage) {
		if (result instanceof Iterable<?> && shouldSplitOutput((Iterable<?>) result)) {
			for (Object o : (Iterable<?>) result) {
				this.produceOutput(o, requestMessage);
			}
		}
		else if (result != null) {
			this.produceOutput(result, requestMessage);
		}
	}

	/**
	 * 消息本身是否需要被打散
	 * @param reply
	 * @return
	 */
	protected boolean shouldSplitOutput(Iterable<?> reply) {
		for (Object next : reply) {
			if (next instanceof Message<?> || next instanceof AbstractIntegrationMessageBuilder<?>) {
				return true;
			}
		}
		return false;
	}

	/**
	 * 将结果输出到下游
	 * @param replyArg   本次处理的结果
	 * @param requestMessage  这个是本handler 接收到的消息源
	 */
	protected void produceOutput(Object replyArg, final Message<?> requestMessage) {
		MessageHeaders requestHeaders = requestMessage.getHeaders();
		Object reply = replyArg;
		Object replyChannel = null;
		if (getOutputChannel() == null) {
			// slip的可以先忽略
			Map<?, ?> routingSlipHeader = obtainRoutingSlipHeader(requestHeaders, reply);
			if (routingSlipHeader != null) {
				Assert.isTrue(routingSlipHeader.size() == 1,
						"The RoutingSlip header value must be a SingletonMap");
				Object key = routingSlipHeader.keySet().iterator().next();
				Object value = routingSlipHeader.values().iterator().next();
				Assert.isInstanceOf(List.class, key, "The RoutingSlip key must be List");
				Assert.isInstanceOf(Integer.class, value, "The RoutingSlip value must be Integer");
				List<?> routingSlip = (List<?>) key;
				AtomicInteger routingSlipIndex = new AtomicInteger((Integer) value);
				replyChannel = getOutputChannelFromRoutingSlip(reply, requestMessage, routingSlip, routingSlipIndex);
				if (replyChannel != null) {
					reply = addRoutingSlipHeader(reply, routingSlip, routingSlipIndex);
				}
			}
			// 解析出响应通道
			if (replyChannel == null) {
				replyChannel = obtainReplyChannel(requestHeaders, reply);
			}
		}
		doProduceOutput(requestMessage, requestHeaders, reply, replyChannel);
	}

	/**
	 * 首先尝试从上游消息获取相关的请求头  没有获取到的情况 从本次生成的结果消息获取相关请求头
	 * @param requestHeaders
	 * @param reply
	 * @return
	 */
	@Nullable
	private Map<?, ?> obtainRoutingSlipHeader(MessageHeaders requestHeaders, Object reply) {
		Map<?, ?> routingSlipHeader = requestHeaders.get(IntegrationMessageHeaderAccessor.ROUTING_SLIP, Map.class);
		if (routingSlipHeader == null) {
			if (reply instanceof Message) {
				routingSlipHeader = ((Message<?>) reply).getHeaders()
						.get(IntegrationMessageHeaderAccessor.ROUTING_SLIP, Map.class);
			}
			else if (reply instanceof AbstractIntegrationMessageBuilder<?>) {
				routingSlipHeader = ((AbstractIntegrationMessageBuilder<?>) reply)
						.getHeader(IntegrationMessageHeaderAccessor.ROUTING_SLIP, Map.class);
			}
		}
		return routingSlipHeader;
	}

	@Nullable
	private Object obtainReplyChannel(MessageHeaders requestHeaders, Object reply) {
		Object replyChannel = requestHeaders.getReplyChannel();
		if (replyChannel == null) {
			if (reply instanceof Message) {
				replyChannel = ((Message<?>) reply).getHeaders().getReplyChannel();
			}
			else if (reply instanceof AbstractIntegrationMessageBuilder<?>) {
				replyChannel = ((AbstractIntegrationMessageBuilder<?>) reply)
						.getHeader(MessageHeaders.REPLY_CHANNEL, Object.class);
			}
		}
		return replyChannel;
	}

	/**
	 * 该方法真正将数据发往下游
	 * @param requestMessage  上游下发的消息
	 * @param requestHeaders
	 * @param reply   本次处理结果
	 * @param replyChannel
	 */
	private void doProduceOutput(Message<?> requestMessage, MessageHeaders requestHeaders, Object reply,
			Object replyChannel) {

		// 如果是异步处理 且 支持设置监听器
		if (this.async && (reply instanceof ListenableFuture<?> || reply instanceof Publisher<?>)) {
			MessageChannel messageChannel = getOutputChannel();
			if (reply instanceof ListenableFuture<?> ||
					!(messageChannel instanceof ReactiveStreamsSubscribableChannel)) {
				asyncNonReactiveReply(requestMessage, reply, replyChannel);
			}
			else {
				((ReactiveStreamsSubscribableChannel) messageChannel)
						.subscribeTo(
								Flux.from((Publisher<?>) reply)
										.doOnError((ex) -> sendErrorMessage(requestMessage, ex))
										.map(result -> createOutputMessage(result, requestHeaders)));
			}
		}
		else {
			// 默认情况下 请求头中的数据是要传播的
			sendOutput(createOutputMessage(reply, requestHeaders), replyChannel, false);
		}
	}

	private AbstractIntegrationMessageBuilder<?> addRoutingSlipHeader(Object reply, List<?> routingSlip,
			AtomicInteger routingSlipIndex) {

		return messageBuilderForReply(reply)
				.setHeader(IntegrationMessageHeaderAccessor.ROUTING_SLIP,
						Collections.singletonMap(routingSlip, routingSlipIndex.get()));
	}

	protected AbstractIntegrationMessageBuilder<?> messageBuilderForReply(Object reply) {
		AbstractIntegrationMessageBuilder<?> builder;
		if (reply instanceof Message) {
			builder = getMessageBuilderFactory().fromMessage((Message<?>) reply);
		}
		else if (reply instanceof AbstractIntegrationMessageBuilder) {
			builder = (AbstractIntegrationMessageBuilder<?>) reply;
		}
		else {
			builder = getMessageBuilderFactory().withPayload(reply);
		}
		return builder;
	}

	private void asyncNonReactiveReply(Message<?> requestMessage, Object reply, Object replyChannel) {
		ListenableFuture<?> future;
		if (reply instanceof ListenableFuture<?>) {
			future = (ListenableFuture<?>) reply;
		}
		else {
			SettableListenableFuture<Object> settableListenableFuture = new SettableListenableFuture<>();
			Mono<?> reactiveReply;
			ReactiveAdapter adapter = ReactiveAdapterRegistry.getSharedInstance().getAdapter(null, reply);
			if (adapter != null && adapter.isMultiValue()) {
				reactiveReply = Mono.just(reply);
			}
			else {
				reactiveReply = Mono.from((Publisher<?>) reply);
			}
			reactiveReply.subscribe(settableListenableFuture::set, settableListenableFuture::setException);
			future = settableListenableFuture;
		}
		future.addCallback(new ReplyFutureCallback(requestMessage, replyChannel));
	}

	private Object getOutputChannelFromRoutingSlip(Object reply, Message<?> requestMessage, List<?> routingSlip,
			AtomicInteger routingSlipIndex) {

		if (routingSlipIndex.get() >= routingSlip.size()) {
			return null;
		}

		Object path = routingSlip.get(routingSlipIndex.get());
		Object routingSlipPathValue = null;

		if (path instanceof String) {
			routingSlipPathValue = getBeanFactory().getBean((String) path);
		}
		else if (path instanceof RoutingSlipRouteStrategy) {
			routingSlipPathValue = path;
		}
		else {
			throw new IllegalArgumentException("The RoutingSlip 'path' can be of " +
					"String or RoutingSlipRouteStrategy type, but got: " + path.getClass());
		}

		if (routingSlipPathValue instanceof MessageChannel) {
			routingSlipIndex.incrementAndGet();
			return routingSlipPathValue;
		}
		else {
			Object nextPath = ((RoutingSlipRouteStrategy) routingSlipPathValue).getNextPath(requestMessage, reply);
			if (nextPath != null && (!(nextPath instanceof String) || StringUtils.hasText((String) nextPath))) {
				return nextPath;
			}
			else {
				routingSlipIndex.incrementAndGet();
				return getOutputChannelFromRoutingSlip(reply, requestMessage, routingSlip, routingSlipIndex);
			}
		}
	}

	/**
	 * 将本次处理结果 与上游消息的消息头合并成一个新的消息 用于发往下游
	 * @param output
	 * @param requestHeaders
	 * @return
	 */
	protected Message<?> createOutputMessage(Object output, MessageHeaders requestHeaders) {
		AbstractIntegrationMessageBuilder<?> builder;
		if (output instanceof Message<?>) {
			if (this.noHeadersPropagation || !shouldCopyRequestHeaders()) {
				return (Message<?>) output;
			}
			builder = this.getMessageBuilderFactory().fromMessage((Message<?>) output);
		}
		else if (output instanceof AbstractIntegrationMessageBuilder) {
			builder = (AbstractIntegrationMessageBuilder<?>) output;
		}
		else {
			builder = this.getMessageBuilderFactory().withPayload(output);
		}
		// 复制消息头
		if (!this.noHeadersPropagation && shouldCopyRequestHeaders()) {
			builder.filterAndCopyHeadersIfAbsent(requestHeaders,
					this.selectiveHeaderPropagation ? this.notPropagatedHeaders : null);
		}
		return builder.build();
	}

	/**
	 * Send an output Message. The 'replyChannel' will be considered only if this handler's
	 * 'outputChannel' is <code>null</code>. In that case, the 'replyChannel' value must not also be
	 * <code>null</code>, and it must be an instance of either String or {@link MessageChannel}.
	 * @param output the output object to send                                     需要被发送的消息
	 * @param replyChannelArg the 'replyChannel' value from the original request    目标通道
	 * @param useArgChannel - use the replyChannel argument (must not be null), not    是否要使用目标通道 而不是用一开始设置的 outputChannel
	 * the configured output channel.
	 *                      将消息发送到对应通道
	 */
	protected void sendOutput(Object output, @Nullable Object replyChannelArg, boolean useArgChannel) {
		Object replyChannel = replyChannelArg;
		// 获取下游通道
		MessageChannel outChannel = getOutputChannel();
		if (!useArgChannel && outChannel != null) {
			replyChannel = outChannel;
		}
		if (replyChannel == null) {
			throw new DestinationResolutionException("no output-channel or replyChannel header available");
		}

		if (replyChannel instanceof MessageChannel) {
			if (output instanceof Message<?>) {
				this.messagingTemplate.send((MessageChannel) replyChannel, (Message<?>) output);
			}
			else {
				// 其余类型要先进行转换  (也就是生成某个message 并且将该参数作为 payload)
				this.messagingTemplate.convertAndSend((MessageChannel) replyChannel, output);
			}
		}
		else if (replyChannel instanceof String) {
			if (output instanceof Message<?>) {
				this.messagingTemplate.send((String) replyChannel, (Message<?>) output);
			}
			else {
				this.messagingTemplate.convertAndSend((String) replyChannel, output);
			}
		}
		else {
			throw new MessagingException("replyChannel must be a MessageChannel or String");
		}
	}

	/**
	 * Subclasses may override this. True by default.
	 * @return true if the request headers should be copied.
	 */
	protected boolean shouldCopyRequestHeaders() {
		return true;
	}

	/**
	 * 当处理消息出现异常时
	 * @param requestMessage
	 * @param ex
	 */
	protected void sendErrorMessage(Message<?> requestMessage, Throwable ex) {
		// 先从消息源 header中获取对应的异常处理通道
		Object errorChannel = resolveErrorChannel(requestMessage.getHeaders());
		Throwable result = ex;
		if (!(ex instanceof MessagingException)) {
			// 包装成 MessagingException
			result = new MessageHandlingException(requestMessage, ex);
		}
		if (errorChannel == null) {
			logger.error("Async exception received and no 'errorChannel' header exists and no default "
					+ "'errorChannel' found", result);
		}
		else {
			try {
				// 将异常信息发送出去
				sendOutput(new ErrorMessage(result), errorChannel, true);
			}
			catch (Exception e) {
				Exception exceptionToLog =
						IntegrationUtils.wrapInHandlingExceptionIfNecessary(requestMessage,
								() -> "failed to send error message in the [" + this + ']', e);
				logger.error("Failed to send async reply", exceptionToLog);
			}
		}
	}

	/**
	 * 从消息头中解析出 当发生异常时下游的异常通道
	 * @param requestHeaders
	 * @return
	 */
	protected Object resolveErrorChannel(final MessageHeaders requestHeaders) {
		Object errorChannel = requestHeaders.getErrorChannel();
		if (errorChannel == null) {
			try {
				errorChannel = getChannelResolver().resolveDestination(IntegrationContextUtils.ERROR_CHANNEL_BEAN_NAME);
			}
			catch (DestinationResolutionException e) {
				// ignore
			}
		}
		return errorChannel;
	}

	private final class ReplyFutureCallback implements ListenableFutureCallback<Object> {

		private final Message<?> requestMessage;

		private final Object replyChannel;

		ReplyFutureCallback(Message<?> requestMessage, Object replyChannel) {
			this.requestMessage = requestMessage;
			this.replyChannel = replyChannel;
		}


		@Override
		public void onSuccess(Object result) {
			Message<?> replyMessage = null;
			try {
				replyMessage = createOutputMessage(result, this.requestMessage.getHeaders());
				sendOutput(replyMessage, this.replyChannel, false);
			}
			catch (Exception ex) {
				Exception exceptionToLogAndSend = ex;
				if (!(ex instanceof MessagingException)) { // NOSONAR
					exceptionToLogAndSend = new MessageHandlingException(this.requestMessage, ex);
					if (replyMessage != null) {
						exceptionToLogAndSend = new MessagingException(replyMessage, exceptionToLogAndSend);
					}
				}
				logger.error("Failed to send async reply: " + result.toString(), exceptionToLogAndSend);
				onFailure(exceptionToLogAndSend);
			}
		}

		@Override
		public void onFailure(Throwable ex) {
			sendErrorMessage(this.requestMessage, ex);
		}

	}

}

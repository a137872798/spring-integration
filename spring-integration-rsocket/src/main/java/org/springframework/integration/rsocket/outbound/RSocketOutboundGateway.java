/*
 * Copyright 2019 the original author or authors.
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

package org.springframework.integration.rsocket.outbound;

import java.util.Arrays;
import java.util.Map;

import org.reactivestreams.Publisher;

import org.springframework.core.ParameterizedTypeReference;
import org.springframework.expression.EvaluationContext;
import org.springframework.expression.Expression;
import org.springframework.integration.expression.ExpressionUtils;
import org.springframework.integration.expression.ValueExpression;
import org.springframework.integration.handler.AbstractReplyProducingMessageHandler;
import org.springframework.integration.rsocket.ClientRSocketConnector;
import org.springframework.lang.Nullable;
import org.springframework.messaging.Message;
import org.springframework.messaging.rsocket.RSocketRequester;
import org.springframework.messaging.rsocket.annotation.support.RSocketRequesterMethodArgumentResolver;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;
import org.springframework.util.CollectionUtils;
import org.springframework.util.MimeType;

import reactor.core.publisher.Mono;

/**
 * An Outbound Messaging Gateway for RSocket requests.
 * The request logic is fully based on the {@link RSocketRequester}, which can be obtained from the
 * {@link ClientRSocketConnector} on the client side or from the
 * {@link RSocketRequesterMethodArgumentResolver#RSOCKET_REQUESTER_HEADER} request message header
 * on the server side.
 * <p>
 * An RSocket operation is determined by the configured {@link Command} or respective SpEL
 * expression to be evaluated at runtime against the request message.
 * By default the {@link Command#requestResponse} operation is used.
 * <p>
 * For a {@link Publisher}-based requests, it must be present in the request message {@code payload}.
 * The flattening via upstream {@link org.springframework.integration.channel.FluxMessageChannel} will work, too,
 * but this way we will lose a scope of particular request and every {@link Publisher} event
 * will be send in its own plain request.
 * <p>
 * If reply is a {@link reactor.core.publisher.Flux}, it is wrapped to the {@link Mono} to retain a request scope.
 * The downstream flow is responsible to obtain this {@link reactor.core.publisher.Flux} from a message payload
 * and subscribe to it by itself. The {@link Mono} reply from this component is subscribed from the downstream
 * {@link org.springframework.integration.channel.FluxMessageChannel} or it is adapted to the
 * {@link org.springframework.util.concurrent.ListenableFuture} otherwise.
 *
 * @author Artem Bilan
 *
 * @since 5.2
 *
 * @see Command
 * @see RSocketRequester
 */
public class RSocketOutboundGateway extends AbstractReplyProducingMessageHandler {

	private final Expression routeExpression;

	private Object[] routeVars;

	@Nullable
	private ClientRSocketConnector clientRSocketConnector;

	private Expression commandExpression = new ValueExpression<>(Command.requestResponse);

	private Expression publisherElementTypeExpression;

	private Expression expectedResponseTypeExpression = new ValueExpression<>(String.class);

	private Expression metadataExpression;

	private EvaluationContext evaluationContext;

	@Nullable
	private Mono<RSocketRequester> rsocketRequesterMono;

	/**
	 * Instantiate based on the provided RSocket endpoint {@code route}
	 * and optional variables to expand route template.
	 * @param route the RSocket endpoint route to use.
	 * @param routeVariables the variables to expand route template.
	 */
	public RSocketOutboundGateway(String route, @Nullable Object... routeVariables) {
		this(new ValueExpression<>(route));
		if (routeVariables != null) {
			this.routeVars = Arrays.copyOf(routeVariables, routeVariables.length);
		}
	}

	/**
	 * Instantiate based on the provided SpEL expression to evaluate an RSocket endpoint {@code route}
	 * at runtime against a request message.
	 * If route is a template and variables expansion is required, it is recommended to do that
	 * in this expression evaluation, for example using some bean with an appropriate logic.
	 * @param routeExpression the SpEL expression to use.
	 */
	public RSocketOutboundGateway(Expression routeExpression) {
		Assert.notNull(routeExpression, "'routeExpression' must not be null");
		this.routeExpression = routeExpression;
		setAsync(true);
		setPrimaryExpression(this.routeExpression);
	}

	/**
	 * Configure a {@link ClientRSocketConnector} for client side requests based on the connection
	 * provided by the {@link ClientRSocketConnector#getRSocketRequester()}.
	 * In case of server side, an {@link RSocketRequester} must be provided in the
	 * {@link RSocketRequesterMethodArgumentResolver#RSOCKET_REQUESTER_HEADER} header of request message.
	 * @param clientRSocketConnector the {@link ClientRSocketConnector} to use.
	 */
	public void setClientRSocketConnector(ClientRSocketConnector clientRSocketConnector) {
		Assert.notNull(clientRSocketConnector, "'clientRSocketConnector' must not be null");
		this.clientRSocketConnector = clientRSocketConnector;
	}

	/**
	 * Configure a {@link Command} for RSocket request type.
	 * @param command the {@link Command} to use.
	 */
	public void setCommand(Command command) {
		setCommandExpression(new ValueExpression<>(command));
	}

	/**
	 * Configure a SpEL expression to evaluate a {@link Command} for RSocket request type at runtime
	 * against a request message.
	 * @param commandExpression the SpEL expression to use.
	 */
	public void setCommandExpression(Expression commandExpression) {
		Assert.notNull(commandExpression, "'commandExpression' must not be null");
		this.commandExpression = commandExpression;
	}

	/**
	 * Configure a type for a request {@link Publisher} elements.
	 * @param publisherElementType the type of the request {@link Publisher} elements.
	 * @see RSocketRequester.RequestSpec#data(Object, Class)
	 */
	public void setPublisherElementType(Class<?> publisherElementType) {
		setPublisherElementTypeExpression(new ValueExpression<>(publisherElementType));

	}

	/**
	 * Configure a SpEL expression to evaluate a request {@link Publisher} elements type at runtime against
	 * a request message.
	 * @param publisherElementTypeExpression the expression to evaluate a type for the request
	 * {@link Publisher} elements.
	 * @see RSocketRequester.RequestSpec#data
	 */
	public void setPublisherElementTypeExpression(Expression publisherElementTypeExpression) {
		this.publisherElementTypeExpression = publisherElementTypeExpression;
	}

	/**
	 * Specify the expected response type for the RSocket response.
	 * @param expectedResponseType The expected type.
	 * @see #setExpectedResponseTypeExpression(Expression)
	 * @see RSocketRequester.RequestSpec#retrieveMono
	 * @see RSocketRequester.RequestSpec#retrieveFlux
	 */
	public void setExpectedResponseType(Class<?> expectedResponseType) {
		setExpectedResponseTypeExpression(new ValueExpression<>(expectedResponseType));
	}

	/**
	 * Specify the {@link Expression} to determine the type for the RSocket response.
	 * @param expectedResponseTypeExpression The expected response type expression.
	 * @see RSocketRequester.RequestSpec#retrieveMono
	 * @see RSocketRequester.RequestSpec#retrieveFlux
	 */
	public void setExpectedResponseTypeExpression(Expression expectedResponseTypeExpression) {
		this.expectedResponseTypeExpression = expectedResponseTypeExpression;
	}

	/**
	 * Specify a SpEL expression to evaluate a metadata for RSocket request
	 * as {@code Map<Object, MimeType>} against request message.
	 * @param metadataExpression the expression for metadata.
	 */
	public void setMetadataExpression(Expression metadataExpression) {
		this.metadataExpression = metadataExpression;
	}

	@Override
	protected void doInit() {
		super.doInit();
		this.evaluationContext = ExpressionUtils.createStandardEvaluationContext(getBeanFactory());
		if (this.clientRSocketConnector != null) {
			this.rsocketRequesterMono = this.clientRSocketConnector.getRSocketRequester();
		}
	}

	@Override
	protected Object handleRequestMessage(Message<?> requestMessage) {
		RSocketRequester rsocketRequester = requestMessage.getHeaders()
				.get(RSocketRequesterMethodArgumentResolver.RSOCKET_REQUESTER_HEADER, RSocketRequester.class);
		Mono<RSocketRequester> requesterMono;
		if (rsocketRequester != null) {
			requesterMono = Mono.just(rsocketRequester);
		}
		else {
			requesterMono = this.rsocketRequesterMono;
		}

		Assert.notNull(requesterMono,
				() -> "The 'RSocketRequester' must be configured via 'ClientRSocketConnector' or provided in the '" +
						RSocketRequesterMethodArgumentResolver.RSOCKET_REQUESTER_HEADER + "' request message headers.");

		return requesterMono
				.map((rSocketRequester) -> createRequestSpec(rSocketRequester, requestMessage))
				.map((requestSpec) -> prepareRequestSpec(requestSpec, requestMessage))
				.flatMap((responseSpec) -> performRequest(responseSpec, requestMessage));
	}

	@SuppressWarnings("unchecked")
	private RSocketRequester.RequestSpec createRequestSpec(RSocketRequester rsocketRequester,
			Message<?> requestMessage) {

		String route = this.routeExpression.getValue(this.evaluationContext, requestMessage, String.class);
		Assert.notNull(route, () -> "The 'routeExpression' [" + this.routeExpression + "] must not evaluate to null");

		RSocketRequester.RequestSpec requestSpec = rsocketRequester.route(route, this.routeVars);
		if (this.metadataExpression != null) {
			Map<Object, MimeType> metadata =
					this.metadataExpression.getValue(this.evaluationContext, requestMessage, Map.class);
			if (!CollectionUtils.isEmpty(metadata)) {
				requestSpec.metadata((spec) -> metadata.forEach(spec::metadata));
			}
		}

		return requestSpec;
	}

	private RSocketRequester.RetrieveSpec prepareRequestSpec(RSocketRequester.RequestSpec requestSpec,
			Message<?> requestMessage) {

		Object payload = requestMessage.getPayload();
		if (payload instanceof Publisher<?> && this.publisherElementTypeExpression != null) {
			Object publisherElementType = evaluateExpressionForType(requestMessage, this.publisherElementTypeExpression,
					"publisherElementType");
			return prepareRequestSpecForPublisher(requestSpec, (Publisher<?>) payload, publisherElementType);
		}
		else {
			return requestSpec.data(payload);
		}
	}

	private RSocketRequester.RetrieveSpec prepareRequestSpecForPublisher(RSocketRequester.RequestSpec requestSpec,
			Publisher<?> payload, Object publisherElementType) {

		if (publisherElementType instanceof Class<?>) {
			return requestSpec.data(payload, (Class<?>) publisherElementType);
		}
		else {
			return requestSpec.data(payload, (ParameterizedTypeReference<?>) publisherElementType);
		}
	}

	private Mono<?> performRequest(RSocketRequester.RetrieveSpec requestSpec, Message<?> requestMessage) {
		Command command = this.commandExpression.getValue(this.evaluationContext, requestMessage, Command.class);
		Assert.notNull(command,
				() -> "The 'command' [" + this.commandExpression + "] must not evaluate to null");

		Object expectedResponseType = null;
		if (!Command.fireAndForget.equals(command)) {
			expectedResponseType = evaluateExpressionForType(requestMessage, this.expectedResponseTypeExpression,
					"expectedResponseType");
		}

		switch (command) {
			case fireAndForget:
				return requestSpec.send();
			case requestResponse:
				if (expectedResponseType instanceof Class<?>) {
					return requestSpec.retrieveMono((Class<?>) expectedResponseType);
				}
				else {
					return requestSpec.retrieveMono((ParameterizedTypeReference<?>) expectedResponseType);
				}
			case requestStreamOrChannel:
				if (expectedResponseType instanceof Class<?>) {
					return Mono.just(requestSpec.retrieveFlux((Class<?>) expectedResponseType));
				}
				else {
					return Mono.just(requestSpec.retrieveFlux((ParameterizedTypeReference<?>) expectedResponseType));
				}
			default:
				throw new UnsupportedOperationException("Unsupported command: " + command);
		}
	}

	private Object evaluateExpressionForType(Message<?> requestMessage, Expression expression, String propertyName) {
		Object type = expression.getValue(this.evaluationContext, requestMessage);
		Assert.state(type instanceof Class<?>
						|| type instanceof String
						|| type instanceof ParameterizedTypeReference<?>,
				() -> "The '" + propertyName + "' [" + expression +
						"] must evaluate to 'String' (class FQN), 'Class<?>' " +
						"or 'ParameterizedTypeReference<?>', not to: " + type);

		if (type instanceof String) {
			try {
				return ClassUtils.forName((String) type, getBeanClassLoader());
			}
			catch (ClassNotFoundException e) {
				throw new IllegalStateException(e);
			}
		}
		else {
			return type;
		}
	}

	/**
	 * Enumeration of commands supported by the gateways.
	 */
	public enum Command {

		/**
		 * Perform {@link io.rsocket.RSocket#fireAndForget fireAndForget}.
		 * @see RSocketRequester.RequestSpec#send()
		 */
		fireAndForget,

		/**
		 * Perform {@link io.rsocket.RSocket#requestResponse requestResponse}.
		 * @see RSocketRequester.RequestSpec#retrieveMono
		 */
		requestResponse,

		/**
		 * Perform {@link io.rsocket.RSocket#requestStream requestStream} or
		 * {@link io.rsocket.RSocket#requestChannel requestChannel} depending on whether
		 * the request input consists of a single or multiple payloads.
		 * @see RSocketRequester.RequestSpec#retrieveFlux
		 */
		requestStreamOrChannel

	}

}
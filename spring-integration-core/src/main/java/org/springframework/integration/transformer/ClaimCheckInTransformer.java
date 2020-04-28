/*
 * Copyright 2002-2019 the original author or authors.
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

package org.springframework.integration.transformer;

import java.util.UUID;

import org.springframework.integration.store.MessageStore;
import org.springframework.integration.support.AbstractIntegrationMessageBuilder;
import org.springframework.messaging.Message;
import org.springframework.util.Assert;

/**
 * Transformer that stores a Message and returns a new Message whose payload
 * is the id of the stored Message.
 *
 * @author Mark Fisher
 * @since 2.0
 * 该对象可以在message传输的时候 临时性的存储payload 并在恰当的时候重新读取payload  针对数据流比较大的情况
 */
public class ClaimCheckInTransformer extends AbstractTransformer {

	/**
	 * 该对象负责存储相关数据
	 */
	private final MessageStore messageStore;


	/**
	 * Create a claim check-in transformer that will delegate to the provided MessageStore.
	 *
	 * @param messageStore The message store.
	 */
	public ClaimCheckInTransformer(MessageStore messageStore) {
		Assert.notNull(messageStore, "MessageStore must not be null");
		this.messageStore = messageStore;
	}

	@Override
	public String getComponentType() {
		return "claim-check-in";
	}

	@Override
	protected Object doTransform(Message<?> message) {
		Assert.notNull(message, "message must not be null");
		UUID id = message.getHeaders().getId();
		Assert.notNull(id, "ID header must not be null");
		this.messageStore.addMessage(message);
		// 将内部的消息负载仅仅转换成一个 id  之后在合适的时机 通过id 去store中获取数据
		AbstractIntegrationMessageBuilder<?> responseBuilder = getMessageBuilderFactory().withPayload(id);
		// headers on the 'current' message take precedence
		responseBuilder.copyHeaders(message.getHeaders());
		return responseBuilder.build();
	}

}

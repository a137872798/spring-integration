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

package org.springframework.integration.store;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Set;

import org.springframework.integration.IntegrationMessageHeaderAccessor;
import org.springframework.messaging.Message;
import org.springframework.util.Assert;

/**
 * Represents a mutable group of correlated messages that is bound to a certain {@link MessageStore} and group id.
 * The group will grow during its lifetime, when messages are <code>add</code>ed to it.
 * This MessageGroup is thread safe.
 *
 * @author Iwein Fuld
 * @author Oleg Zhurakousky
 * @author Dave Syer
 * @author Gary Russell
 * @author Artem Bilan
 * @since 2.0
 * 消息组对象
 */
public class SimpleMessageGroup implements MessageGroup {

	private final Object groupId;

	private final Collection<Message<?>> messages;

	private final Set<Integer> sequences = new HashSet<>();

	private final long timestamp;

	private volatile int lastReleasedMessageSequence;

	private volatile long lastModified;

	private volatile boolean complete;

	public SimpleMessageGroup(Object groupId) {
		this(Collections.<Message<?>>emptyList(), groupId);
	}

	public SimpleMessageGroup(Collection<? extends Message<?>> messages, Object groupId) {
		this(messages, groupId, System.currentTimeMillis(), false);
	}

	public SimpleMessageGroup(MessageGroup messageGroup) {
		this(messageGroup.getMessages(), messageGroup.getGroupId(), messageGroup.getTimestamp(),
				messageGroup.isComplete());
	}

	public SimpleMessageGroup(Collection<? extends Message<?>> messages, Object groupId, long timestamp,
							  boolean complete) {
		this(new LinkedHashSet<>(), messages, groupId, timestamp, complete, false);
	}

	/**
	 * 按照特定存储方式创建某个消息组
	 *
	 * @param internalStore  使用的存储容器
	 * @param messages       当前内部维护的消息数量
	 * @param groupId        用于标记该组的id
	 * @param timestamp
	 * @param complete
	 * @param storePreLoaded
	 */
	public SimpleMessageGroup(Collection<Message<?>> internalStore, Collection<? extends Message<?>> messages,
							  Object groupId, long timestamp, boolean complete, boolean storePreLoaded) {

		Assert.notNull(internalStore, "'internalStore' must not be null");
		this.messages = internalStore;
		this.groupId = groupId;
		this.timestamp = timestamp;
		this.complete = complete;
		if (!storePreLoaded) {
			Assert.notNull(messages, "'messages' must not be null");
			for (Message<?> message : messages) {
				if (message != null) { //see INT-2666
					addMessage(message);
				}
			}
		}
	}

	@Override
	public long getTimestamp() {
		return this.timestamp;
	}

	@Override
	public void setLastModified(long lastModified) {
		this.lastModified = lastModified;
	}

	@Override
	public long getLastModified() {
		return this.lastModified;
	}

	@Override
	public boolean canAdd(Message<?> message) {
		return true;
	}

	@Override
	public void add(Message<?> messageToAdd) {
		addMessage(messageToAdd);
	}

	@Override
	public boolean remove(Message<?> message) {
		this.sequences.remove(message.getHeaders().get(IntegrationMessageHeaderAccessor.SEQUENCE_NUMBER));
		return this.messages.remove(message);
	}

	@Override
	public int getLastReleasedMessageSequenceNumber() {
		return this.lastReleasedMessageSequence;
	}

	private boolean addMessage(Message<?> message) {
		Integer sequence = message.getHeaders().get(IntegrationMessageHeaderAccessor.SEQUENCE_NUMBER, Integer.class);
		this.sequences.add(sequence != null ? sequence : 0);
		return this.messages.add(message);
	}

	@Override
	public Collection<Message<?>> getMessages() {
		return Collections.unmodifiableCollection(this.messages);
	}

	@Override
	public void setLastReleasedMessageSequenceNumber(int sequenceNumber) {
		this.lastReleasedMessageSequence = sequenceNumber;
	}

	@Override
	public Object getGroupId() {
		return this.groupId;
	}

	@Override
	public boolean isComplete() {
		return this.complete;
	}

	@Override
	public void complete() {
		this.complete = true;
	}

	@Override
	public int getSequenceSize() {
		if (size() == 0) {
			return 0;
		}
		return new IntegrationMessageHeaderAccessor(getOne()).getSequenceSize();
	}

	@Override
	public int size() {
		return this.messages.size();
	}

	@Override
	public Message<?> getOne() {
		synchronized (this.messages) {
			Iterator<Message<?>> iterator = this.messages.iterator();
			return iterator.hasNext() ? iterator.next() : null;
		}
	}

	@Override
	public void clear() {
		this.messages.clear();
		this.sequences.clear();
	}

	/**
	 * Return true if a message with this sequence number header exists in
	 * the group.
	 *
	 * @param sequence the sequence number.
	 * @return true if it exists.
	 * @since 4.3.7
	 */
	public boolean containsSequence(Integer sequence) {
		return this.sequences.contains(sequence);
	}

	@Override
	public String toString() {
		return "SimpleMessageGroup{" +
				"groupId=" + this.groupId +
				", messages=" + this.messages +
				", timestamp=" + this.timestamp +
				", lastModified=" + this.lastModified +
				'}';
	}

}

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

package org.springframework.integration.metadata;

/**
 *
 * {@link ConcurrentMetadataStore} with the ability of registering {@link MetadataStoreListener} callbacks, to be
 * invoked when changes occur in the metadata store.
 *
 * @author Marius Bogoevici
 * @since 4.2
 * 该接口代表可以监听元数据的变化   并在相应的方法通知监听器
 */
public interface ListenableMetadataStore extends ConcurrentMetadataStore {

	/**
	 * Registers a listener with the metadata store
	 *
	 * @param callback the callback to be registered
	 */
	void addListener(MetadataStoreListener callback);

	/**
	 * Unregisters a listener
	 *
	 * @param callback the callback to be unregistered
	 */
	void removeListener(MetadataStoreListener callback);
}

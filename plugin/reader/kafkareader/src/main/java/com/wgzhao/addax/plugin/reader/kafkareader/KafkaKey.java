/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.wgzhao.addax.plugin.reader.kafkareader;

import com.wgzhao.addax.core.base.Key;

public class KafkaKey
        extends Key
{
    public final static String BROKER_LIST = "brokerList";
    public final static String TOPIC = "topic";
    public final static String PROPERTIES = "properties";
    public final static String MISSING_KEY_VALUE = "missingKeyValue";
}

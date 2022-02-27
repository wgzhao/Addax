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

package com.wgzhao.addax.plugin.writer.tdenginewriter;

public class ColumnMeta
{
    String field;
    String type;
    int length;
    String note;
    boolean isTag;
    boolean isPrimaryKey;
    Object value;

    @Override
    public String toString()
    {
        return "ColumnMeta{" +
                "field='" + field + '\'' +
                ", type='" + type + '\'' +
                ", length=" + length +
                ", note='" + note + '\'' +
                ", isTag=" + isTag +
                ", isPrimaryKey=" + isPrimaryKey +
                ", value=" + value +
                '}';
    }
}
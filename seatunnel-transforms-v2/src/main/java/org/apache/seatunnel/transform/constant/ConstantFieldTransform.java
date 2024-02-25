/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.transform.constant;

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.type.*;
import org.apache.seatunnel.common.exception.CommonError;
import org.apache.seatunnel.transform.common.MultipleFieldOutputTransform;
import org.apache.seatunnel.transform.common.SeaTunnelRowAccessor;
import org.apache.seatunnel.transform.copy.CopyTransformConfig;
import org.apache.seatunnel.transform.exception.TransformCommonError;

import java.lang.reflect.Array;
import java.util.*;
import java.util.stream.Collectors;

import static org.apache.seatunnel.api.table.type.BasicType.STRING_TYPE;

public class ConstantFieldTransform extends MultipleFieldOutputTransform {
    public static final String PLUGIN_NAME = "Constant";

    private final ConstantTransformConfig config;
    private LinkedHashMap<String, String> fields;

    public ConstantFieldTransform(ConstantTransformConfig constantTransformConfig, CatalogTable catalogTable) {
        super(catalogTable);
        this.config = constantTransformConfig;
        initOutputFields(config.getFields());
    }

    @Override
    public String getPluginName() {
        return PLUGIN_NAME;
    }

    private void initOutputFields(LinkedHashMap<String, String> fields) {
        this.fields = fields;
    }

    @Override
    protected Column[] getOutputColumns() {
        Column[] columns = new Column[fields.size()];
        int temp = 0;
        for (String addField : fields.keySet()) {
            PhysicalColumn addColumn =
                    PhysicalColumn.of(
                            addField,
                            STRING_TYPE,
                            64,
                            false,
                            null,
                            null);
            columns[temp] = addColumn;
            temp++;
        }
        return columns;
    }

    @Override
    protected Object[] getOutputFieldValues(SeaTunnelRowAccessor inputRow) {

        Object[] fieldValues = new Object[fields.size()];
        int temp = 0;
        for (String addField : fields.keySet()) {
            fieldValues[temp] = fields.get(addField);
            temp++;
        }
        return fieldValues;
    }

}

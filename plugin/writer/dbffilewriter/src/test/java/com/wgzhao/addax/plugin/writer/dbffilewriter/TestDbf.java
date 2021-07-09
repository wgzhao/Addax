/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  *   http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing,
 *  * software distributed under the License is distributed on an
 *  * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  * KIND, either express or implied.  See the License for the
 *  * specific language governing permissions and limitations
 *  * under the License.
 *
 */

package com.wgzhao.addax.plugin.writer.dbffilewriter;

import com.linuxense.javadbf.DBFDataType;
import com.linuxense.javadbf.DBFField;
import com.linuxense.javadbf.DBFReader;
import com.linuxense.javadbf.DBFWriter;
import org.junit.jupiter.api.Test;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.sql.Date;

public class TestDbf
{
    private final static String filePath = "/tmp/test.dbf";

    @Test
    public void testBasicWriter()
            throws FileNotFoundException
    {
        // let us create field definitions first
        // we will go for 4 fields

        DBFField[] fields = new DBFField[4];

        fields[0] = new DBFField();
        fields[0].setName("emp_code");
        fields[0].setType(DBFDataType.CHARACTER);
        fields[0].setLength(10);

        fields[1] = new DBFField();
        fields[1].setName("emp_name");
        fields[1].setType(DBFDataType.CHARACTER);
        fields[1].setLength(20);

        fields[2] = new DBFField();
        fields[2].setName("salary");
        fields[2].setType(DBFDataType.NUMERIC);
        fields[2].setLength(12);
        fields[2].setDecimalCount(2);

        fields[3] = new DBFField();
        fields[3].setName("cur_date");
        fields[3].setType(DBFDataType.DATE);

        DBFWriter writer = new DBFWriter(new FileOutputStream(filePath));
        writer.setFields(fields);

        // now populate DBFWriter

        Object[] rowData = new Object[4];
        rowData[0] = "1000";
        rowData[1] = "John";
        rowData[2] = 5000.00;
        rowData[3] = Date.valueOf("1989-06-04");

        writer.addRecord(rowData);

        rowData = new Object[4];
        rowData[0] = "1001";
        rowData[1] = "Lalit";
        rowData[2] = 3400.00;
        rowData[3] = Date.valueOf("1989-06-05");

        writer.addRecord(rowData);

        rowData = new Object[4];
        rowData[0] = "1002";
        rowData[1] = "Rohit";
        rowData[2] = null;
        rowData[3] = null;

        writer.addRecord(rowData);

        // write to file
        writer.close();
    }

    @Test
    public void testReadBasic()
    {
        DBFReader reader = null;
        try {
            reader = new DBFReader(new FileInputStream(filePath));
            int numberOfFields = reader.getFieldCount();

            // use this count to fetch all field information
            // if required

            for (int i = 0; i < numberOfFields; i++) {

                DBFField field = reader.getField(i);

                // do something with it if you want
                // refer the JavaDoc API reference for more details
                //
                System.out.print(field.getName() + "\t");
            }
            System.out.println();

            // Now, lets us start reading the rows

            Object[] rowObjects;

            while ((rowObjects = reader.nextRecord()) != null) {

                for (int i = 0; i < rowObjects.length; i++) {
                    System.out.print(rowObjects[i] + "\t");
                }
                System.out.println();
            }
        }
        catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }
}

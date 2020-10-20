package com.hazelcast.jet.sql;

import com.hazelcast.function.FunctionEx;
import com.hazelcast.sql.SqlService;
import com.hazelcast.sql.impl.calcite.opt.physical.visitor.CallFunction;
import com.hazelcast.sql.impl.calcite.parse.UnsupportedOperationVisitor;
import com.hazelcast.sql.impl.calcite.validate.HazelcastSqlOperatorTable;
import com.hazelcast.sql.impl.calcite.validate.types.HazelcastTypeFactory;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.FunctionParameter;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.FamilyOperandTypeChecker;
import org.apache.calcite.sql.type.InferTypes;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.validate.SqlUserDefinedFunction;
import org.apache.calcite.util.Util;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static java.util.Arrays.asList;
import static org.apache.calcite.sql.type.SqlTypeFamily.CHARACTER;

public class CustomFunctionTest extends SqlTestSupport{


    private static SqlService sqlService;

    @BeforeClass
    public static void setUpClass() {
        initialize(2, null);
        sqlService = instance().getSql();
    }

    @Test
    public void defineFunctionAndExecute() {
        String name = randomName();
        sqlService.execute(javaSerializableMapDdl(name, Integer.class, String.class));

        sqlService.execute("SINK INTO " + name + " (__key, this) VALUES (1, 'Alice')");

        sqlService.execute("CREATE FUNCTION saySomething(VARCHAR) RETURNS varchar AS 'fun myFunc(name: String) : String {   return name + \" says kotlin rocks\" }myFunc(param)' LANGUAGE 'KOTLIN'");

        assertRowsAnyOrder(
                "SELECT saySomething(this) FROM " + name,
                asList(
                        new Row("Alice says kotlin rocks")
                )
        );

    }

}

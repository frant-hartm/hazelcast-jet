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
    public void name() {
        String name = randomName();
        sqlService.execute(javaSerializableMapDdl(name, Integer.class, String.class));

        sqlService.execute("SINK INTO " + name + " (__key, this) VALUES (1, 'Alice')");

        sqlService.execute("CREATE FUNCTION customFn(VARCHAR) RETURNS varchar AS ' ' LANGUAGE KOTLIN");

        registerCustomFn("customFn", (param) -> "Custom: " + param);

        assertRowsAnyOrder(
                "SELECT customFn(this) FROM " + name,
                asList(
                        new Row("Custom: Alice")
                )
        );

    }

    private static void registerCustomFn(String name, FunctionEx<String, String> functionEx) {
        RelDataTypeFactory typeFactory = HazelcastTypeFactory.INSTANCE;

        List<RelDataType> types = new ArrayList<>();
        List<SqlTypeFamily> families = new ArrayList<>();

        CallFunction function = new CallFunction(functionEx);
        for (FunctionParameter parameter : function.getParameters()) {
            RelDataType type = parameter.getType(typeFactory);
            assert type.getSqlTypeName().getFamily() == CHARACTER;

            types.add(type);
            families.add(Util.first(type.getSqlTypeName().getFamily(), SqlTypeFamily.ANY));
        }

        FamilyOperandTypeChecker typeChecker = OperandTypes.family(families, index -> true);
        SqlUserDefinedFunction fn = new SqlUserDefinedFunction(new SqlIdentifier(name, SqlParserPos.ZERO),
                ReturnTypes.VARCHAR_2000,
                InferTypes.explicit(Collections.emptyList()),
                typeChecker,
                types,
                function
        );

        HazelcastSqlOperatorTable.instance().register(
                fn
        );

        UnsupportedOperationVisitor.SUPPORTED_OPERATORS.add(fn);
        UnsupportedOperationVisitor.SUPPORTED_KINDS.add(fn.getKind());
    }

}

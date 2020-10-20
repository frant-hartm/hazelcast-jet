package com.hazelcast.jet.sql.impl.parse;

import org.apache.calcite.sql.SqlCreate;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.util.ImmutableNullableList;

import java.util.Collections;
import java.util.List;

public class SqlCreateFunction extends SqlCreate {

    private static final SqlSpecialOperator OPERATOR =
            new SqlSpecialOperator("CREATE FUNCTION", SqlKind.CREATE_TABLE);
    private final SqlIdentifier name;
    private final SqlDataTypeSpec inputType;
    private final SqlDataTypeSpec type;
    private final SqlNode script;
    private final SqlNode language;

    public SqlCreateFunction(
            SqlIdentifier name,
            SqlDataTypeSpec inputType,
            SqlDataTypeSpec type,
            SqlNode script,
            SqlNode language
    ) {
        super(OPERATOR, SqlParserPos.ZERO, false, true);
        this.name = name;
        this.inputType = inputType;
        this.type = type;
        this.script = script;
        this.language = language;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableNullableList.of(name, inputType, type, script, language);
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        super.unparse(writer, leftPrec, rightPrec);
    }

    public SqlIdentifier getName() {
        return name;
    }

    public SqlDataTypeSpec getInputType() {
        return inputType;
    }

    public SqlDataTypeSpec getType() {
        return type;
    }

    public SqlNode getScript() {
        return script;
    }

    public SqlNode getLanguage() {
        return language;
    }

    @Override
    public void validate(SqlValidator validator, SqlValidatorScope scope) {
    }
}

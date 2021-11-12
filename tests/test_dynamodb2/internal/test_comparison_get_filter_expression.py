import moto.dynamodb2.comparisons
import moto.dynamodb2.models


def test_filter_expression():
    row1 = moto.dynamodb2.models.Item(
        None,
        None,
        None,
        None,
        {
            "Id": {"N": "8"},
            "Subs": {"N": "5"},
            "Desc": {"S": "Some description"},
            "KV": {"SS": ["test1", "test2"]},
        },
    )
    row2 = moto.dynamodb2.models.Item(
        None,
        None,
        None,
        None,
        {
            "Id": {"N": "8"},
            "Subs": {"N": "10"},
            "Desc": {"S": "A description"},
            "KV": {"SS": ["test3", "test4"]},
        },
    )

    # NOT test 1
    filter_expr = moto.dynamodb2.comparisons.get_filter_expression(
        "NOT attribute_not_exists(Id)", {}, {}
    )
    filter_expr.expr(row1).should.be(True)

    # NOT test 2
    filter_expr = moto.dynamodb2.comparisons.get_filter_expression(
        "NOT (Id = :v0)", {}, {":v0": {"N": "8"}}
    )
    filter_expr.expr(row1).should.be(False)  # Id = 8 so should be false

    # AND test
    filter_expr = moto.dynamodb2.comparisons.get_filter_expression(
        "Id > :v0 AND Subs < :v1", {}, {":v0": {"N": "5"}, ":v1": {"N": "7"}}
    )
    filter_expr.expr(row1).should.be(True)
    filter_expr.expr(row2).should.be(False)

    # lowercase AND test
    filter_expr = moto.dynamodb2.comparisons.get_filter_expression(
        "Id > :v0 and Subs < :v1", {}, {":v0": {"N": "5"}, ":v1": {"N": "7"}}
    )
    filter_expr.expr(row1).should.be(True)
    filter_expr.expr(row2).should.be(False)

    # OR test
    filter_expr = moto.dynamodb2.comparisons.get_filter_expression(
        "Id = :v0 OR Id=:v1", {}, {":v0": {"N": "5"}, ":v1": {"N": "8"}}
    )
    filter_expr.expr(row1).should.be(True)

    # BETWEEN test
    filter_expr = moto.dynamodb2.comparisons.get_filter_expression(
        "Id BETWEEN :v0 AND :v1", {}, {":v0": {"N": "5"}, ":v1": {"N": "10"}}
    )
    filter_expr.expr(row1).should.be(True)

    # PAREN test
    filter_expr = moto.dynamodb2.comparisons.get_filter_expression(
        "Id = :v0 AND (Subs = :v0 OR Subs = :v1)",
        {},
        {":v0": {"N": "8"}, ":v1": {"N": "5"}},
    )
    filter_expr.expr(row1).should.be(True)

    # IN test
    filter_expr = moto.dynamodb2.comparisons.get_filter_expression(
        "Id IN (:v0, :v1, :v2)",
        {},
        {":v0": {"N": "7"}, ":v1": {"N": "8"}, ":v2": {"N": "9"}},
    )
    filter_expr.expr(row1).should.be(True)

    # attribute function tests (with extra spaces)
    filter_expr = moto.dynamodb2.comparisons.get_filter_expression(
        "attribute_exists(Id) AND attribute_not_exists (User)", {}, {}
    )
    filter_expr.expr(row1).should.be(True)

    filter_expr = moto.dynamodb2.comparisons.get_filter_expression(
        "attribute_type(Id, :v0)", {}, {":v0": {"S": "N"}}
    )
    filter_expr.expr(row1).should.be(True)

    # beginswith function test
    filter_expr = moto.dynamodb2.comparisons.get_filter_expression(
        "begins_with(Desc, :v0)", {}, {":v0": {"S": "Some"}}
    )
    filter_expr.expr(row1).should.be(True)
    filter_expr.expr(row2).should.be(False)

    # contains function test
    filter_expr = moto.dynamodb2.comparisons.get_filter_expression(
        "contains(KV, :v0)", {}, {":v0": {"S": "test1"}}
    )
    filter_expr.expr(row1).should.be(True)
    filter_expr.expr(row2).should.be(False)

    # size function test
    filter_expr = moto.dynamodb2.comparisons.get_filter_expression(
        "size(Desc) > size(KV)", {}, {}
    )
    filter_expr.expr(row1).should.be(True)

    # Expression from @batkuip
    filter_expr = moto.dynamodb2.comparisons.get_filter_expression(
        "(#n0 < :v0 AND attribute_not_exists(#n1))",
        {"#n0": "Subs", "#n1": "fanout_ts"},
        {":v0": {"N": "7"}},
    )
    filter_expr.expr(row1).should.be(True)
    # Expression from to check contains on string value
    filter_expr = moto.dynamodb2.comparisons.get_filter_expression(
        "contains(#n0, :v0)", {"#n0": "Desc"}, {":v0": {"S": "Some"}}
    )
    filter_expr.expr(row1).should.be(True)
    filter_expr.expr(row2).should.be(False)

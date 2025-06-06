import os
import re
import string
from datetime import datetime, timedelta, timezone
from time import sleep
from unittest import SkipTest
from uuid import uuid4

import boto3
import pytest
from botocore.exceptions import ClientError
from dateutil.tz import tzlocal
from freezegun import freeze_time

from moto import mock_aws, settings
from moto.core import DEFAULT_ACCOUNT_ID as ACCOUNT_ID
from moto.secretsmanager.models import secretsmanager_backends
from moto.secretsmanager.utils import SecretsManagerSecretIdentifier
from moto.utilities.id_generator import TAG_KEY_CUSTOM_ID
from tests import allow_aws_request
from tests.test_awslambda import lambda_aws_verified
from tests.test_awslambda.utilities import _process_lambda
from tests.test_dynamodb import dynamodb_aws_verified

from .. import DEFAULT_ACCOUNT_ID
from . import secretsmanager_aws_verified

DEFAULT_SECRET_NAME = "test-secret7"


@mock_aws
def test_get_secret_value():
    conn = boto3.client("secretsmanager", region_name="us-west-2")

    conn.create_secret(Name="java-util-test-password", SecretString="foosecret")
    result = conn.get_secret_value(SecretId="java-util-test-password")
    assert result["SecretString"] == "foosecret"


@mock_aws
def test_secret_arn():
    region = "us-west-2"
    conn = boto3.client("secretsmanager", region_name=region)

    create_dict = conn.create_secret(
        Name=DEFAULT_SECRET_NAME,
        SecretString="secret_string",
    )
    assert re.match(
        f"arn:aws:secretsmanager:{region}:{ACCOUNT_ID}:secret:{DEFAULT_SECRET_NAME}-"
        + r"\w{6}",
        create_dict["ARN"],
    )


@mock_aws
def test_create_secret_with_client_request_token():
    conn = boto3.client("secretsmanager", region_name="us-west-2")

    version_id = "eb41453f-25bb-4025-b7f4-850cfca0ce71"
    create_dict = conn.create_secret(
        Name=DEFAULT_SECRET_NAME,
        SecretString="secret_string",
        ClientRequestToken=version_id,
    )
    assert create_dict
    assert create_dict["VersionId"] == version_id


@mock_aws
def test_get_secret_value_by_arn():
    conn = boto3.client("secretsmanager", region_name="us-west-2")

    name = "java-util-test-password"
    secret_value = "test_get_secret_value_by_arn"
    result = conn.create_secret(Name=name, SecretString=secret_value)
    arn = result["ARN"]
    assert re.match(
        f"^arn:aws:secretsmanager:us-west-2:{ACCOUNT_ID}:secret:{name}", arn
    )

    result = conn.get_secret_value(SecretId=arn)
    assert result["SecretString"] == secret_value


@mock_aws
def test_get_secret_value_binary():
    conn = boto3.client("secretsmanager", region_name="us-west-2")

    conn.create_secret(Name="java-util-test-password", SecretBinary=b"foosecret")
    result = conn.get_secret_value(SecretId="java-util-test-password")
    assert result["SecretBinary"] == b"foosecret"


@mock_aws
def test_get_secret_that_does_not_exist():
    conn = boto3.client("secretsmanager", region_name="us-west-2")

    with pytest.raises(ClientError) as cm:
        conn.get_secret_value(SecretId="i-dont-exist")

    assert (
        "Secrets Manager can't find the specified secret."
        == cm.value.response["Error"]["Message"]
    )


@mock_aws
def test_get_secret_that_does_not_match():
    conn = boto3.client("secretsmanager", region_name="us-west-2")
    conn.create_secret(Name="java-util-test-password", SecretString="foosecret")

    with pytest.raises(ClientError) as cm:
        conn.get_secret_value(SecretId="i-dont-match")

    assert (
        "Secrets Manager can't find the specified secret."
        == cm.value.response["Error"]["Message"]
    )


@mock_aws
def test_get_secret_value_that_is_marked_deleted():
    conn = boto3.client("secretsmanager", region_name="us-west-2")

    conn.create_secret(Name="test-secret", SecretString="foosecret")

    conn.delete_secret(SecretId="test-secret")

    with pytest.raises(ClientError):
        conn.get_secret_value(SecretId="test-secret")


@mock_aws
def test_get_secret_that_has_no_value():
    conn = boto3.client("secretsmanager", region_name="us-west-2")

    conn.create_secret(Name="secret-no-value")

    with pytest.raises(ClientError) as cm:
        conn.get_secret_value(SecretId="secret-no-value")

    assert (
        "Secrets Manager can't find the specified secret value for staging label: AWSCURRENT"
        == cm.value.response["Error"]["Message"]
    )


@mock_aws
def test_get_secret_version_that_does_not_exist():
    conn = boto3.client("secretsmanager", region_name="us-west-2")

    result = conn.create_secret(Name="java-util-test-password", SecretString="v")
    secret_arn = result["ARN"]
    missing_version_id = "00000000-0000-0000-0000-000000000000"

    with pytest.raises(ClientError) as cm:
        conn.get_secret_value(SecretId=secret_arn, VersionId=missing_version_id)

    assert (
        "Secrets Manager can't find the specified "
        "secret value for VersionId: 00000000-0000-0000-0000-000000000000"
    ) == cm.value.response["Error"]["Message"]


@mock_aws
def test_get_secret_version_stage_mismatch():
    conn = boto3.client("secretsmanager", region_name="us-west-2")

    result = conn.create_secret(Name="test-secret", SecretString="secret")
    secret_arn = result["ARN"]

    rotated_secret = conn.rotate_secret(
        SecretId=secret_arn, RotationRules={"AutomaticallyAfterDays": 42}
    )

    desc_secret = conn.describe_secret(SecretId=secret_arn)
    versions_to_stages = desc_secret["VersionIdsToStages"]
    version_for_test = rotated_secret["VersionId"]
    stages_for_version = versions_to_stages[version_for_test]

    assert "AWSPENDING" not in stages_for_version
    with pytest.raises(ClientError) as cm:
        conn.get_secret_value(
            SecretId=secret_arn, VersionId=version_for_test, VersionStage="AWSPENDING"
        )

    assert (
        "You provided a VersionStage that is not associated to the provided VersionId."
    ) == cm.value.response["Error"]["Message"]


@mock_aws
def test_batch_get_secret_value_for_secret_id_list_with_matches():
    conn = boto3.client("secretsmanager", region_name="us-west-2")

    secret_a = conn.create_secret(Name="test-secret-a", SecretString="secret")
    secret_b = conn.create_secret(Name="test-secret-b", SecretString="secret")

    secrets_batch = conn.batch_get_secret_value(
        SecretIdList=["test-secret-a", "test-secret-b"]
    )
    matched = [
        secret
        for secret in secrets_batch["SecretValues"]
        if secret["ARN"] in [secret_a["ARN"], secret_b["ARN"]]
    ]

    assert len(matched) == len(secrets_batch["SecretValues"]) == 2


@mock_aws
def test_batch_get_secret_value_for_secret_id_list_without_matches():
    conn = boto3.client("secretsmanager", region_name="us-west-2")

    conn.create_secret(Name="test-secret-a", SecretString="secret")

    secrets_batch = conn.batch_get_secret_value(
        SecretIdList=["test-secret-b", "test-secret-c"]
    )
    assert len(secrets_batch["SecretValues"]) == 0


@mock_aws
def test_batch_get_secret_value_with_filters():
    conn = boto3.client("secretsmanager", region_name="us-west-2")

    secret_a = conn.create_secret(Name="test-secret-a", SecretString="secret")
    secret_b = conn.create_secret(Name="test-secret-b", SecretString="secret")
    conn.create_secret(Name="test-secret-c", SecretString="secret")

    secrets_batch = conn.batch_get_secret_value(
        Filters=[{"Key": "name", "Values": [secret_a["Name"], secret_b["Name"]]}]
    )

    assert [sec["ARN"] for sec in secrets_batch["SecretValues"]] == [
        secret_a["ARN"],
        secret_b["ARN"],
    ]


@mock_aws
def test_batch_get_secret_value_with_both_secret_id_list_and_filters():
    conn = boto3.client("secretsmanager", region_name="us-west-2")

    with pytest.raises(ClientError) as exc:
        conn.batch_get_secret_value(
            Filters=[{"Key": "name", "Values": ["test-secret-a", "test-secret-b"]}],
            SecretIdList=["foo", "bar"],
        )

    err = exc.value.response["Error"]
    assert err["Code"] == "InvalidParameterException"
    assert (
        "Either 'SecretIdList' or 'Filters' must be provided, but not both."
        in err["Message"]
    )


@mock_aws
def test_batch_get_secret_value_with_max_results_and_no_filters():
    conn = boto3.client("secretsmanager", region_name="us-west-2")

    conn = boto3.client("secretsmanager", region_name="us-west-2")
    with pytest.raises(ClientError) as exc:
        conn.batch_get_secret_value(MaxResults=10, SecretIdList=["foo", "bar"])

    err = exc.value.response["Error"]
    assert err["Code"] == "InvalidParameterException"
    assert (
        "'Filters' not specified. 'Filters' must also be specified when 'MaxResults' is provided."
        in err["Message"]
    )


@mock_aws
def test_batch_get_secret_value_binary():
    conn = boto3.client("secretsmanager", region_name="us-west-2")

    secret_a = conn.create_secret(Name="test-secret-a", SecretBinary="secretA")
    secret_b = conn.create_secret(Name="test-secret-b", SecretBinary="secretB")
    conn.create_secret(Name="test-secret-c", SecretBinary="secretC")

    secrets_batch = conn.batch_get_secret_value(
        Filters=[{"Key": "name", "Values": [secret_a["Name"], secret_b["Name"]]}]
    )
    matched = [
        secret
        for secret in secrets_batch["SecretValues"]
        if secret["ARN"] == secret_a["ARN"]
        and secret["SecretBinary"] == b"secretA"
        or secret["ARN"] == secret_b["ARN"]
        and secret["SecretBinary"] == b"secretB"
    ]
    assert len(matched) == 2


@mock_aws
def test_batch_get_secret_value_missing_value():
    conn = boto3.client("secretsmanager", region_name="us-east-2")

    secret_a = conn.create_secret(Name="test-secret-a")
    secret_b = conn.create_secret(Name="test-secret-b")

    with pytest.raises(ClientError) as exc:
        conn.batch_get_secret_value(
            Filters=[{"Key": "name", "Values": [secret_a["Name"], secret_b["Name"]]}]
        )

    err = exc.value.response["Error"]
    assert err["Code"] == "ResourceNotFoundException"


@mock_aws
def test_create_secret():
    conn = boto3.client("secretsmanager", region_name="us-east-1")

    result = conn.create_secret(Name="test-secret", SecretString="foosecret")
    assert result["ARN"]
    assert result["Name"] == "test-secret"
    secret = conn.get_secret_value(SecretId="test-secret")
    assert secret["SecretString"] == "foosecret"


@mock_aws
def test_create_secret_with_tags():
    conn = boto3.client("secretsmanager", region_name="us-east-1")
    secret_name = "test-secret-with-tags"

    result = conn.create_secret(
        Name=secret_name,
        SecretString="foosecret",
        Tags=[{"Key": "Foo", "Value": "Bar"}, {"Key": "Mykey", "Value": "Myvalue"}],
    )
    assert result["ARN"]
    assert result["Name"] == secret_name
    secret_value = conn.get_secret_value(SecretId=secret_name)
    assert secret_value["SecretString"] == "foosecret"
    secret_details = conn.describe_secret(SecretId=secret_name)
    assert secret_details["Tags"] == [
        {"Key": "Foo", "Value": "Bar"},
        {"Key": "Mykey", "Value": "Myvalue"},
    ]


@mock_aws
def test_create_secret_with_description():
    conn = boto3.client("secretsmanager", region_name="us-east-1")
    secret_name = "test-secret-with-tags"

    result = conn.create_secret(
        Name=secret_name, SecretString="foosecret", Description="desc"
    )
    assert result["ARN"]
    assert result["Name"] == secret_name
    secret_value = conn.get_secret_value(SecretId=secret_name)
    assert secret_value["SecretString"] == "foosecret"
    secret_details = conn.describe_secret(SecretId=secret_name)
    assert secret_details["Description"] == "desc"


@mock_aws
def test_create_secret_with_tags_and_description():
    conn = boto3.client("secretsmanager", region_name="us-east-1")
    secret_name = "test-secret-with-tags"

    result = conn.create_secret(
        Name=secret_name,
        SecretString="foosecret",
        Description="desc",
        Tags=[{"Key": "Foo", "Value": "Bar"}, {"Key": "Mykey", "Value": "Myvalue"}],
    )
    assert result["ARN"]
    assert result["Name"] == secret_name
    secret_value = conn.get_secret_value(SecretId=secret_name)
    assert secret_value["SecretString"] == "foosecret"
    secret_details = conn.describe_secret(SecretId=secret_name)
    assert secret_details["Tags"] == [
        {"Key": "Foo", "Value": "Bar"},
        {"Key": "Mykey", "Value": "Myvalue"},
    ]
    assert secret_details["Description"] == "desc"


@mock_aws
def test_create_secret_without_value():
    conn = boto3.client("secretsmanager", region_name="us-east-2")
    secret_name = f"secret-{str(uuid4())[0:6]}"

    create = conn.create_secret(Name=secret_name)
    assert set(create.keys()) == {"ARN", "Name", "ResponseMetadata"}

    describe = conn.describe_secret(SecretId=secret_name)
    assert set(describe.keys()) == {
        "ARN",
        "Name",
        "LastChangedDate",
        "CreatedDate",
        "ResponseMetadata",
    }

    with pytest.raises(ClientError) as exc:
        conn.get_secret_value(SecretId=secret_name)
    err = exc.value.response["Error"]
    assert err["Code"] == "ResourceNotFoundException"

    updated = conn.update_secret(
        SecretId=secret_name,
        Description="new desc",
    )
    assert set(updated.keys()) == {"ARN", "Name", "ResponseMetadata"}

    describe = conn.describe_secret(SecretId=secret_name)
    assert set(describe.keys()) == {
        "ARN",
        "Name",
        "Description",
        "LastChangedDate",
        "CreatedDate",
        "ResponseMetadata",
    }

    deleted = conn.delete_secret(SecretId=secret_name)
    assert set(deleted.keys()) == {"ARN", "Name", "DeletionDate", "ResponseMetadata"}


@mock_aws
def test_create_secret_that_has_no_value_and_then_update():
    conn = boto3.client("secretsmanager", region_name="us-west-2")

    conn.create_secret(Name="secret-no-value")

    conn.update_secret(
        SecretId="secret-no-value",
        SecretString="barsecret",
        Description="desc",
    )

    secret = conn.get_secret_value(SecretId="secret-no-value")
    assert secret["SecretString"] == "barsecret"


@mock_aws
def test_update_secret_without_value():
    conn = boto3.client("secretsmanager", region_name="us-east-2")
    secret_name = f"secret-{str(uuid4())[0:6]}"

    create = conn.create_secret(Name=secret_name, SecretString="foosecret")
    assert set(create.keys()) == {"ARN", "Name", "VersionId", "ResponseMetadata"}
    version_id = create["VersionId"]

    describe1 = conn.describe_secret(SecretId=secret_name)
    assert set(describe1.keys()) == {
        "ARN",
        "Name",
        "LastChangedDate",
        "VersionIdsToStages",
        "CreatedDate",
        "ResponseMetadata",
    }

    conn.get_secret_value(SecretId=secret_name)

    updated = conn.update_secret(SecretId=secret_name, Description="desc")
    assert set(updated.keys()) == {"ARN", "Name", "ResponseMetadata"}

    describe2 = conn.describe_secret(SecretId=secret_name)
    # AWS also includes 'LastAccessedDate'
    assert set(describe2.keys()) == {
        "ARN",
        "Name",
        "Description",
        "LastChangedDate",
        "VersionIdsToStages",
        "CreatedDate",
        "ResponseMetadata",
    }
    assert describe1["VersionIdsToStages"] == describe2["VersionIdsToStages"]

    value = conn.get_secret_value(SecretId=secret_name)
    assert value["SecretString"] == "foosecret"
    assert value["VersionId"] == version_id

    conn.delete_secret(SecretId=secret_name)


@mock_aws
def test_delete_secret():
    conn = boto3.client("secretsmanager", region_name="us-west-2")

    conn.create_secret(Name="test-secret", SecretString="foosecret")

    deleted_secret = conn.delete_secret(SecretId="test-secret")

    assert deleted_secret["ARN"]
    assert deleted_secret["Name"] == "test-secret"
    assert deleted_secret["DeletionDate"] > datetime.fromtimestamp(1, timezone.utc)

    secret_details = conn.describe_secret(SecretId="test-secret")

    assert secret_details["ARN"]
    assert secret_details["Name"] == "test-secret"
    assert secret_details["DeletedDate"] > datetime.fromtimestamp(1, timezone.utc)


@mock_aws
def test_delete_secret_by_arn():
    conn = boto3.client("secretsmanager", region_name="us-west-2")

    secret = conn.create_secret(Name="test-secret", SecretString="foosecret")

    deleted_secret = conn.delete_secret(SecretId=secret["ARN"])

    assert deleted_secret["ARN"] == secret["ARN"]
    assert deleted_secret["Name"] == "test-secret"
    assert deleted_secret["DeletionDate"] > datetime.fromtimestamp(1, timezone.utc)

    secret_details = conn.describe_secret(SecretId="test-secret")

    assert secret_details["ARN"] == secret["ARN"]
    assert secret_details["Name"] == "test-secret"
    assert secret_details["DeletedDate"] > datetime.fromtimestamp(1, timezone.utc)


@mock_aws
def test_delete_secret_force():
    conn = boto3.client("secretsmanager", region_name="us-west-2")

    conn.create_secret(Name="test-secret", SecretString="foosecret")

    result = conn.delete_secret(SecretId="test-secret", ForceDeleteWithoutRecovery=True)

    assert result["ARN"]
    assert result["DeletionDate"] > datetime.fromtimestamp(1, timezone.utc)
    assert result["Name"] == "test-secret"

    with pytest.raises(ClientError):
        conn.get_secret_value(SecretId="test-secret")


@mock_aws
def test_delete_secret_force_no_such_secret():
    conn = boto3.client("secretsmanager", region_name="us-west-2")

    deleted_secret = conn.delete_secret(
        SecretId=DEFAULT_SECRET_NAME, ForceDeleteWithoutRecovery=True
    )
    assert deleted_secret
    assert deleted_secret["Name"] == DEFAULT_SECRET_NAME


@mock_aws
def test_delete_secret_force_with_arn():
    conn = boto3.client("secretsmanager", region_name="us-west-2")

    create_secret = conn.create_secret(Name="test-secret", SecretString="foosecret")

    result = conn.delete_secret(
        SecretId=create_secret["ARN"], ForceDeleteWithoutRecovery=True
    )

    assert result["ARN"]
    assert result["DeletionDate"] > datetime.fromtimestamp(1, timezone.utc)
    assert result["Name"] == "test-secret"

    with pytest.raises(ClientError):
        conn.get_secret_value(SecretId="test-secret")


@mock_aws
def test_delete_secret_that_does_not_exist():
    conn = boto3.client("secretsmanager", region_name="us-west-2")

    with pytest.raises(ClientError):
        conn.delete_secret(SecretId="i-dont-exist")


@mock_aws
def test_delete_secret_fails_with_both_force_delete_flag_and_recovery_window_flag():
    conn = boto3.client("secretsmanager", region_name="us-west-2")

    conn.create_secret(Name="test-secret", SecretString="foosecret")

    with pytest.raises(ClientError):
        conn.delete_secret(
            SecretId="test-secret",
            RecoveryWindowInDays=1,
            ForceDeleteWithoutRecovery=True,
        )


@mock_aws
def test_delete_secret_recovery_window_invalid_values():
    conn = boto3.client("secretsmanager", region_name="us-west-2")

    conn.create_secret(Name="test-secret", SecretString="foosecret")

    for nr in [0, 2, 6, 31, 100]:
        with pytest.raises(ClientError) as exc:
            conn.delete_secret(SecretId="test-secret", RecoveryWindowInDays=nr)
        err = exc.value.response["Error"]
        assert err["Code"] == "InvalidParameterException"
        assert (
            "RecoveryWindowInDays value must be between 7 and 30 days (inclusive)"
            in err["Message"]
        )


@mock_aws
def test_delete_secret_force_no_such_secret_with_invalid_recovery_window():
    conn = boto3.client("secretsmanager", region_name="us-west-2")

    for nr in [0, 2, 6, 31, 100]:
        with pytest.raises(ClientError) as exc:
            conn.delete_secret(
                SecretId="test-secret",
                RecoveryWindowInDays=nr,
                ForceDeleteWithoutRecovery=True,
            )
        err = exc.value.response["Error"]
        assert err["Code"] == "InvalidParameterException"
        assert (
            "RecoveryWindowInDays value must be between 7 and 30 days (inclusive)"
            in err["Message"]
        )


@mock_aws
def test_delete_secret_that_is_marked_deleted():
    conn = boto3.client("secretsmanager", region_name="us-west-2")

    conn.create_secret(Name="test-secret", SecretString="foosecret")

    conn.delete_secret(SecretId="test-secret")

    with pytest.raises(ClientError):
        conn.delete_secret(SecretId="test-secret")


@mock_aws
def test_force_delete_secret_that_is_marked_deleted():
    conn = boto3.client("secretsmanager", region_name="us-west-2")

    conn.create_secret(Name="test-secret", SecretString="foosecret")

    conn.delete_secret(SecretId="test-secret")

    conn.delete_secret(SecretId="test-secret", ForceDeleteWithoutRecovery=True)


@mock_aws
def test_get_random_password_default_length():
    conn = boto3.client("secretsmanager", region_name="us-west-2")

    random_password = conn.get_random_password()
    assert len(random_password["RandomPassword"]) == 32


@mock_aws
def test_get_random_password_default_requirements():
    # When require_each_included_type, default true
    conn = boto3.client("secretsmanager", region_name="us-west-2")

    random_password = conn.get_random_password()
    # Should contain lowercase, upppercase, digit, special character
    assert any(c.islower() for c in random_password["RandomPassword"])
    assert any(c.isupper() for c in random_password["RandomPassword"])
    assert any(c.isdigit() for c in random_password["RandomPassword"])
    assert any(c in string.punctuation for c in random_password["RandomPassword"])


@mock_aws
def test_get_random_password_custom_length():
    conn = boto3.client("secretsmanager", region_name="us-west-2")

    random_password = conn.get_random_password(PasswordLength=50)
    assert len(random_password["RandomPassword"]) == 50


@mock_aws
def test_get_random_exclude_lowercase():
    conn = boto3.client("secretsmanager", region_name="us-west-2")

    random_password = conn.get_random_password(PasswordLength=55, ExcludeLowercase=True)
    assert not any(c.islower() for c in random_password["RandomPassword"])


@mock_aws
def test_get_random_exclude_uppercase():
    conn = boto3.client("secretsmanager", region_name="us-west-2")

    random_password = conn.get_random_password(PasswordLength=55, ExcludeUppercase=True)
    assert not any(c.isupper() for c in random_password["RandomPassword"])


@mock_aws
def test_get_random_exclude_characters_and_symbols():
    conn = boto3.client("secretsmanager", region_name="us-west-2")

    random_password = conn.get_random_password(
        PasswordLength=20, ExcludeCharacters="xyzDje@?!."
    )
    assert not any(c in "xyzDje@?!." for c in random_password["RandomPassword"])
    assert len(random_password["RandomPassword"]) == 20


@mock_aws
def test_get_random_exclude_numbers():
    conn = boto3.client("secretsmanager", region_name="us-west-2")

    random_password = conn.get_random_password(PasswordLength=100, ExcludeNumbers=True)
    assert not any(c.isdigit() for c in random_password["RandomPassword"])


@mock_aws
def test_get_random_exclude_punctuation():
    conn = boto3.client("secretsmanager", region_name="us-west-2")

    random_password = conn.get_random_password(
        PasswordLength=100, ExcludePunctuation=True
    )
    assert not any(c in string.punctuation for c in random_password["RandomPassword"])


@mock_aws
def test_get_random_include_space_false():
    conn = boto3.client("secretsmanager", region_name="us-west-2")

    random_password = conn.get_random_password(PasswordLength=300)
    assert not any(c.isspace() for c in random_password["RandomPassword"])


@mock_aws
def test_get_random_include_space_true():
    conn = boto3.client("secretsmanager", region_name="us-west-2")

    random_password = conn.get_random_password(PasswordLength=4, IncludeSpace=True)
    assert any(c.isspace() for c in random_password["RandomPassword"])


@mock_aws
def test_get_random_require_each_included_type():
    conn = boto3.client("secretsmanager", region_name="us-west-2")

    random_password = conn.get_random_password(
        PasswordLength=4, RequireEachIncludedType=True
    )
    assert any(c in string.punctuation for c in random_password["RandomPassword"])
    assert any(c in string.ascii_lowercase for c in random_password["RandomPassword"])
    assert any(c in string.ascii_uppercase for c in random_password["RandomPassword"])
    assert any(c in string.digits for c in random_password["RandomPassword"])


@mock_aws
def test_get_random_too_short_password():
    conn = boto3.client("secretsmanager", region_name="us-west-2")

    with pytest.raises(ClientError):
        conn.get_random_password(PasswordLength=3)


@mock_aws
def test_get_random_too_long_password():
    conn = boto3.client("secretsmanager", region_name="us-west-2")

    with pytest.raises(Exception):
        conn.get_random_password(PasswordLength=5555)


@mock_aws
def test_describe_secret():
    conn = boto3.client("secretsmanager", region_name="us-west-2")
    conn.create_secret(Name="test-secret", SecretString="foosecret")

    conn.create_secret(Name="test-secret-2", SecretString="barsecret")

    secret_description = conn.describe_secret(SecretId="test-secret")
    secret_description_2 = conn.describe_secret(SecretId="test-secret-2")

    assert secret_description  # Returned dict is not empty
    assert secret_description["Name"] == ("test-secret")
    assert secret_description["ARN"] != ""  # Test arn not empty
    assert secret_description_2["Name"] == ("test-secret-2")
    assert secret_description_2["ARN"] != ""  # Test arn not empty
    assert secret_description["CreatedDate"] <= datetime.now(tz=tzlocal())
    assert secret_description["CreatedDate"] > datetime.fromtimestamp(1, timezone.utc)
    assert secret_description_2["CreatedDate"] <= datetime.now(tz=tzlocal())
    assert secret_description_2["CreatedDate"] > datetime.fromtimestamp(1, timezone.utc)
    assert secret_description["LastChangedDate"] <= datetime.now(tz=tzlocal())
    assert secret_description["LastChangedDate"] > datetime.fromtimestamp(
        1, timezone.utc
    )
    assert secret_description_2["LastChangedDate"] <= datetime.now(tz=tzlocal())
    assert secret_description_2["LastChangedDate"] > datetime.fromtimestamp(
        1, timezone.utc
    )


@mock_aws
@pytest.mark.parametrize("name", ["testsecret", "test-secret"])
def test_describe_secret_with_arn(name):
    conn = boto3.client("secretsmanager", region_name="us-west-2")
    results = conn.create_secret(Name=name, SecretString="foosecret")

    secret_description = conn.describe_secret(SecretId=results["ARN"])

    assert secret_description  # Returned dict is not empty
    assert secret_description["Name"] == name
    assert secret_description["ARN"] == results["ARN"]
    assert conn.list_secrets()["SecretList"][0]["ARN"] == results["ARN"]

    # We can also supply a partial ARN
    partial_arn = f"arn:aws:secretsmanager:us-west-2:{ACCOUNT_ID}:secret:{name}"
    resp = conn.get_secret_value(SecretId=partial_arn)
    assert resp["Name"] == name

    resp = conn.describe_secret(SecretId=partial_arn)
    assert resp["Name"] == name


@mock_aws
def test_describe_secret_with_KmsKeyId():
    conn = boto3.client("secretsmanager", region_name="us-west-2")
    results = conn.create_secret(
        Name="test-secret", SecretString="foosecret", KmsKeyId="dummy_arn"
    )

    secret_description = conn.describe_secret(SecretId=results["ARN"])

    assert secret_description["KmsKeyId"] == "dummy_arn"
    assert (
        conn.list_secrets()["SecretList"][0]["KmsKeyId"]
        == (secret_description["KmsKeyId"])
    )


@mock_aws
def test_describe_secret_that_does_not_exist():
    conn = boto3.client("secretsmanager", region_name="us-west-2")

    with pytest.raises(ClientError):
        conn.get_secret_value(SecretId="i-dont-exist")


@mock_aws
def test_describe_secret_that_does_not_match():
    conn = boto3.client("secretsmanager", region_name="us-west-2")
    conn.create_secret(Name="test-secret", SecretString="foosecret")

    with pytest.raises(ClientError):
        conn.get_secret_value(SecretId="i-dont-match")


@mock_aws
def test_restore_secret():
    conn = boto3.client("secretsmanager", region_name="us-west-2")

    conn.create_secret(Name="test-secret", SecretString="foosecret")

    conn.delete_secret(SecretId="test-secret")

    described_secret_before = conn.describe_secret(SecretId="test-secret")
    assert described_secret_before["DeletedDate"] > datetime.fromtimestamp(
        1, timezone.utc
    )

    restored_secret = conn.restore_secret(SecretId="test-secret")
    assert restored_secret["ARN"]
    assert restored_secret["Name"] == "test-secret"

    described_secret_after = conn.describe_secret(SecretId="test-secret")
    assert "DeletedDate" not in described_secret_after


@mock_aws
def test_restore_secret_that_is_not_deleted():
    conn = boto3.client("secretsmanager", region_name="us-west-2")

    conn.create_secret(Name="test-secret", SecretString="foosecret")

    restored_secret = conn.restore_secret(SecretId="test-secret")
    assert restored_secret["ARN"]
    assert restored_secret["Name"] == "test-secret"


@mock_aws
def test_restore_secret_that_does_not_exist():
    conn = boto3.client("secretsmanager", region_name="us-west-2")

    with pytest.raises(ClientError):
        conn.restore_secret(SecretId="i-dont-exist")


@mock_aws
def test_cancel_rotate_secret_with_invalid_secret_id():
    conn = boto3.client("secretsmanager", region_name="us-east-1")
    with pytest.raises(ClientError):
        conn.cancel_rotate_secret(SecretId="invalid_id")


@mock_aws
def test_cancel_rotate_secret_after_delete():
    conn = boto3.client("secretsmanager", region_name="us-east-1")
    conn.create_secret(
        Name=DEFAULT_SECRET_NAME, SecretString="foosecret", Description="foodescription"
    )
    conn.delete_secret(
        SecretId=DEFAULT_SECRET_NAME,
        RecoveryWindowInDays=7,
        ForceDeleteWithoutRecovery=False,
    )
    with pytest.raises(ClientError):
        conn.cancel_rotate_secret(SecretId=DEFAULT_SECRET_NAME)


@mock_aws
def test_cancel_rotate_secret_before_enable():
    conn = boto3.client("secretsmanager", region_name="us-east-1")
    conn.create_secret(
        Name=DEFAULT_SECRET_NAME, SecretString="foosecret", Description="foodescription"
    )
    with pytest.raises(ClientError):
        conn.cancel_rotate_secret(SecretId=DEFAULT_SECRET_NAME)


@mock_aws
def test_cancel_rotate_secret():
    if not settings.TEST_SERVER_MODE:
        raise SkipTest("rotation requires a server to be running")
    from tests.test_awslambda.utilities import get_role_name

    lambda_conn = boto3.client(
        "lambda", region_name="us-east-1", endpoint_url="http://localhost:5000"
    )
    func = lambda_conn.create_function(
        FunctionName="testFunction",
        Runtime="python3.11",
        Role=get_role_name(),
        Handler="lambda_function.lambda_handler",
        Code={"ZipFile": get_rotation_zip_file()},
        Description="Secret rotator",
        Timeout=3,
        MemorySize=128,
        Publish=True,
    )
    secrets_conn = boto3.client("secretsmanager", region_name="us-east-1")
    secrets_conn.create_secret(
        Name=DEFAULT_SECRET_NAME, SecretString="foosecret", Description="foodescription"
    )
    secrets_conn.rotate_secret(
        SecretId=DEFAULT_SECRET_NAME,
        RotationLambdaARN=func["FunctionArn"],
        RotationRules={"AutomaticallyAfterDays": 30},
    )
    secrets_conn.cancel_rotate_secret(SecretId=DEFAULT_SECRET_NAME)
    cancelled_rotation = secrets_conn.describe_secret(SecretId=DEFAULT_SECRET_NAME)

    assert not cancelled_rotation["RotationEnabled"]
    # The function config should be preserved
    assert cancelled_rotation["RotationLambdaARN"]


@mock_aws
def test_rotate_secret():
    # Setup
    frozen_time = datetime(2023, 5, 20, 10, 20, 30, tzinfo=tzlocal())
    rotate_after_days = 10
    with freeze_time(frozen_time):
        conn = boto3.client("secretsmanager", region_name="us-west-2")
        conn.create_secret(
            Name=DEFAULT_SECRET_NAME,
            SecretString="foosecret",
            Description="foodescription",
        )

        # Execute
        rotated_secret = conn.rotate_secret(
            SecretId=DEFAULT_SECRET_NAME,
            RotationRules={"AutomaticallyAfterDays": rotate_after_days},
        )
        describe_secret = conn.describe_secret(SecretId=DEFAULT_SECRET_NAME)

        # Verify
        assert rotated_secret
        assert rotated_secret["ARN"] != ""  # Test arn not empty
        assert rotated_secret["Name"] == DEFAULT_SECRET_NAME
        assert rotated_secret["VersionId"] != ""
        assert describe_secret["Description"] == "foodescription"
        assert "NextRotationDate" in describe_secret
        assert "LastRotatedDate" in describe_secret

        # can't do freeze time tests in servermode tests
        if settings.TEST_SERVER_MODE:
            return

        assert describe_secret["LastChangedDate"] == frozen_time
        assert describe_secret["NextRotationDate"] == frozen_time + timedelta(
            days=rotate_after_days
        )


@mock_aws
def test_rotate_secret_without_secretstring():
    # This test just verifies that Moto does not fail
    conn = boto3.client("secretsmanager", region_name="us-east-2")
    conn.create_secret(Name=DEFAULT_SECRET_NAME, Description="foodescription")

    # AWS will always require a Lambda ARN to do the actual rotating
    rotated_secret = conn.rotate_secret(SecretId=DEFAULT_SECRET_NAME)
    assert rotated_secret["Name"] == DEFAULT_SECRET_NAME

    # Without secret-value, and without actual rotating, we can't verify much
    # Just that the secret exists/can be described
    # We cannot verify any versions info (as that is not created without a secret-value)
    describe_secret = conn.describe_secret(SecretId=DEFAULT_SECRET_NAME)
    assert describe_secret["Description"] == "foodescription"


@mock_aws
def test_rotate_secret_enable_rotation():
    conn = boto3.client("secretsmanager", region_name="us-west-2")
    conn.create_secret(Name=DEFAULT_SECRET_NAME, SecretString="foosecret")

    initial_description = conn.describe_secret(SecretId=DEFAULT_SECRET_NAME)
    assert "RotationEnabled" not in initial_description

    conn.rotate_secret(
        SecretId=DEFAULT_SECRET_NAME, RotationRules={"AutomaticallyAfterDays": 42}
    )

    rotated_description = conn.describe_secret(SecretId=DEFAULT_SECRET_NAME)
    assert rotated_description
    assert rotated_description["RotationEnabled"] is True
    assert rotated_description["RotationRules"]["AutomaticallyAfterDays"] == 42


@mock_aws
def test_rotate_secret_that_is_marked_deleted():
    conn = boto3.client("secretsmanager", region_name="us-west-2")

    conn.create_secret(Name="test-secret", SecretString="foosecret")

    conn.delete_secret(SecretId="test-secret")

    with pytest.raises(ClientError):
        conn.rotate_secret(SecretId="test-secret")


@mock_aws
def test_rotate_secret_that_does_not_exist():
    conn = boto3.client("secretsmanager", "us-west-2")

    with pytest.raises(ClientError):
        conn.rotate_secret(SecretId="i-dont-exist")


@mock_aws
def test_rotate_secret_that_does_not_match():
    conn = boto3.client("secretsmanager", region_name="us-west-2")
    conn.create_secret(Name="test-secret", SecretString="foosecret")

    with pytest.raises(ClientError):
        conn.rotate_secret(SecretId="i-dont-match")


@mock_aws
def test_rotate_secret_client_request_token_too_short():
    from botocore.config import Config

    conn = boto3.client(
        "secretsmanager",
        region_name="us-west-2",
        config=Config(parameter_validation=False),
    )
    conn.create_secret(Name=DEFAULT_SECRET_NAME, SecretString="foosecret")
    client_request_token = "TOO-SHORT"
    with pytest.raises(ClientError) as exc_info:
        conn.rotate_secret(
            SecretId=DEFAULT_SECRET_NAME, ClientRequestToken=client_request_token
        )
    error = exc_info.value.response["Error"]
    assert error["Message"] == "ClientRequestToken must be 32-64 characters long."
    assert error["Code"] == "InvalidParameterException"


@mock_aws
def test_rotate_secret_client_request_token_too_long():
    conn = boto3.client("secretsmanager", region_name="us-west-2")
    conn.create_secret(Name=DEFAULT_SECRET_NAME, SecretString="foosecret")

    client_request_token = (
        "ED9F8B6C-85B7-446A-B7E4-38F2A3BEB13C-ED9F8B6C-85B7-446A-B7E4-38F2A3BEB13C"
    )
    with pytest.raises(ClientError):
        conn.rotate_secret(
            SecretId=DEFAULT_SECRET_NAME, ClientRequestToken=client_request_token
        )


@mock_aws
def test_rotate_secret_rotation_lambda_arn_too_long():
    conn = boto3.client("secretsmanager", region_name="us-west-2")
    conn.create_secret(Name=DEFAULT_SECRET_NAME, SecretString="foosecret")

    rotation_lambda_arn = "85B7-446A-B7E4" * 147  # == 2058 characters
    with pytest.raises(ClientError):
        conn.rotate_secret(
            SecretId=DEFAULT_SECRET_NAME, RotationLambdaARN=rotation_lambda_arn
        )


@mock_aws
@pytest.mark.parametrize(
    "days",
    [
        pytest.param(0, id="below min"),
        pytest.param(1001, id="above max"),
    ],
)
def test_rotate_secret_rotation_period_validation(days):
    from botocore.config import Config

    conn = boto3.client(
        "secretsmanager",
        region_name="us-west-2",
        config=Config(parameter_validation=False),
    )
    conn.create_secret(Name=DEFAULT_SECRET_NAME, SecretString="foosecret")
    with pytest.raises(ClientError) as exc_info:
        conn.rotate_secret(
            SecretId=DEFAULT_SECRET_NAME, RotationRules={"AutomaticallyAfterDays": days}
        )
    error = exc_info.value.response["Error"]
    assert (
        error["Message"]
        == "RotationRules.AutomaticallyAfterDays must be within 1-1000."
    )
    assert error["Code"] == "InvalidParameterException"


@mock_aws
def test_rotate_secret_rotation_period_too_long():
    conn = boto3.client("secretsmanager", region_name="us-west-2")
    conn.create_secret(Name=DEFAULT_SECRET_NAME, SecretString="foosecret")

    rotation_rules = {"AutomaticallyAfterDays": 1001}
    with pytest.raises(ClientError):
        conn.rotate_secret(SecretId=DEFAULT_SECRET_NAME, RotationRules=rotation_rules)


def get_rotation_zip_file():
    endpoint = "" if allow_aws_request() else 'endpoint_url="http://motoserver:5000"'

    func_str = (
        """
import boto3
import json
import os

def lambda_handler(event, context):
    arn = event['SecretId']
    token = event['ClientRequestToken']
    step = event['Step']

    client = boto3.client("secretsmanager", region_name="us-east-1", """
        + endpoint
        + """)
    metadata = client.describe_secret(SecretId=arn)
    metadata.pop('LastChangedDate', None)
    metadata.pop('LastAccessedDate', None)
    metadata.pop('NextRotationDate', None)
    metadata.pop('CreatedDate')
    metadata.pop('ResponseMetadata')
    print(metadata)
    versions = client.list_secret_version_ids(SecretId=arn, IncludeDeprecated=True)["Versions"]
    for v in versions:
        v.pop('LastAccessedDate', None)
        v.pop('CreatedDate', None)
    print(versions)
    try:
        pending_value = client.get_secret_value(SecretId=arn, VersionId=token, VersionStage='AWSPENDING')
        pending_value.pop('CreatedDate', None)
        pending_value.pop('ResponseMetadata')
    except Exception as e:
        pending_value = str(e)
    print(pending_value)
    
    dynamodb = boto3.resource("dynamodb", region_name="us-east-1", """
        + endpoint
        + """)
    table = dynamodb.Table(os.environ["table_name"])
    table.put_item(Item={"pk": step, "token": token, "metadata": metadata, "versions": versions, "pending_value": pending_value})

    if not metadata['RotationEnabled']:
        print("Secret %s is not enabled for rotation." % arn)
        raise ValueError("Secret %s is not enabled for rotation." % arn)
    versions = metadata['VersionIdsToStages']
    if token not in versions:
        print("Secret version %s has no stage for rotation of secret %s." % (token, arn))
        raise ValueError("Secret version %s has no stage for rotation of secret %s." % (token, arn))
    if "AWSCURRENT" in versions[token]:
        print("Secret version %s already set as AWSCURRENT for secret %s." % (token, arn))
        return
    elif "AWSPENDING" not in versions[token]:
        print("Secret version %s not set as AWSPENDING for rotation of secret %s." % (token, arn))
        raise ValueError("Secret version %s not set as AWSPENDING for rotation of secret %s." % (token, arn))

    if step == 'createSecret':
        client.put_secret_value(
            SecretId=arn,
            ClientRequestToken=token,
            SecretString='UpdatedValue',
            VersionStages=['AWSPENDING']
        )

    if step == 'setSecret':
        # This method should set the AWSPENDING secret in the service that the secret belongs to.
        # For example, if the secret is a database credential,
        # this method should take the value of the AWSPENDING secret and set the user's password to this value in the database.
        pass

    elif step == 'finishSecret':
        current_version = next(
            version
            for version, stages in metadata['VersionIdsToStages'].items()
            if 'AWSCURRENT' in stages
        )
        print("current: %s new: %s" % (current_version, token))
        client.update_secret_version_stage(
            SecretId=arn,
            VersionStage='AWSCURRENT',
            MoveToVersionId=token,
            RemoveFromVersionId=current_version
        )
        client.update_secret_version_stage(
            SecretId=arn,
            VersionStage='AWSPENDING',
            RemoveFromVersionId=token
        )
    """
    )
    return _process_lambda(func_str)


@pytest.mark.aws_verified
@dynamodb_aws_verified()
@lambda_aws_verified
@secretsmanager_aws_verified
def test_rotate_secret_using_lambda(secret=None, iam_role_arn=None, table_name=None):
    role_name = iam_role_arn.split("/")[-1]
    if not allow_aws_request() and not settings.TEST_SERVER_MODE:
        raise SkipTest("Can only test this in ServerMode")

    iam = boto3.client("iam", "us-east-1")
    if allow_aws_request():
        iam.attach_role_policy(
            PolicyArn="arn:aws:iam::aws:policy/SecretsManagerReadWrite",
            RoleName=role_name,
        )
        # Testing this against AWS itself is a bit of pain
        # Uncomment this to get more insights into what is happening during execution of the Lambda
        # iam.attach_role_policy(
        #    PolicyArn="arn:aws:iam::aws:policy/CloudWatchLogsFullAccess",
        #    RoleName=role_name,
        # )
        iam.attach_role_policy(
            PolicyArn="arn:aws:iam::aws:policy/AmazonDynamoDBFullAccess",
            RoleName=role_name,
        )

    function_name = "moto_test_" + str(uuid4())[0:6]

    # Passing a `RotationLambdaARN` value to `rotate_secret` should invoke lambda
    lambda_conn = boto3.client("lambda", region_name="us-east-1")
    func = lambda_conn.create_function(
        FunctionName=function_name,
        Runtime="python3.11",
        Role=iam_role_arn,
        Handler="lambda_function.lambda_handler",
        Code={"ZipFile": get_rotation_zip_file()},
        Publish=True,
        Environment={"Variables": {"table_name": table_name}},
    )
    lambda_conn.add_permission(
        FunctionName=function_name,
        StatementId="allow_secrets_manager",
        Action="lambda:InvokeFunction",
        Principal="secretsmanager.amazonaws.com",
    )
    lambda_conn.get_waiter("function_active_v2").wait(FunctionName=function_name)

    secrets_conn = boto3.client("secretsmanager", region_name="us-east-1")

    initial_version = secret["VersionId"]

    rotated_secret = secrets_conn.rotate_secret(
        SecretId=secret["ARN"],
        RotationLambdaARN=func["FunctionArn"],
        RotationRules={"AutomaticallyAfterDays": 30},
        RotateImmediately=True,
    )

    # Ensure we received an updated VersionId from `rotate_secret`
    assert rotated_secret["VersionId"] != initial_version

    secret_not_updated = True
    while secret_not_updated:
        updated_secret = secrets_conn.get_secret_value(
            SecretId=secret["ARN"], VersionStage="AWSCURRENT"
        )
        if updated_secret["SecretString"] == "UpdatedValue":
            secret_not_updated = False
        else:
            sleep(5)
    rotated_version = updated_secret["VersionId"]

    assert initial_version != rotated_version

    u2 = secrets_conn.get_secret_value(SecretId=secret["ARN"])
    assert u2["SecretString"] == "UpdatedValue"
    assert u2["VersionId"] == rotated_version

    metadata = secrets_conn.describe_secret(SecretId=secret["ARN"])
    assert metadata["VersionIdsToStages"][initial_version] == ["AWSPREVIOUS"]
    assert metadata["VersionIdsToStages"][rotated_version] == ["AWSCURRENT"]
    assert updated_secret["SecretString"] == "UpdatedValue"

    lambda_conn.delete_function(FunctionName=function_name)

    dynamodb = boto3.resource("dynamodb", region_name="us-east-1")
    items = dynamodb.Table(table_name).scan()["Items"]

    create_secret = [i for i in items if i["pk"] == "createSecret"][0]
    assert "(ResourceNotFoundException)" in create_secret["pending_value"]
    assert create_secret["versions"][0]["VersionStages"] == ["AWSCURRENT"]

    finish_secret = [i for i in items if i["pk"] == "finishSecret"][0]
    assert finish_secret["pending_value"]["SecretString"] == "UpdatedValue"
    assert finish_secret["pending_value"]["VersionStages"] == ["AWSPENDING"]


@pytest.mark.aws_verified
@dynamodb_aws_verified()
@lambda_aws_verified
@secretsmanager_aws_verified
def test_rotate_secret_using_lambda_dont_rotate_immediately(
    secret=None, iam_role_arn=None, table_name=None
):
    role_name = iam_role_arn.split("/")[-1]
    if not allow_aws_request() and not settings.TEST_SERVER_MODE:
        raise SkipTest("Can only test this in ServerMode")

    iam = boto3.client("iam", "us-east-1")
    if allow_aws_request():
        iam.attach_role_policy(
            PolicyArn="arn:aws:iam::aws:policy/SecretsManagerReadWrite",
            RoleName=role_name,
        )
        # Testing this against AWS itself is a bit of pain
        # Uncomment this to get more insights into what is happening during execution of the Lambda
        # iam.attach_role_policy(
        #    PolicyArn="arn:aws:iam::aws:policy/CloudWatchLogsFullAccess",
        #    RoleName=role_name,
        # )
        iam.attach_role_policy(
            PolicyArn="arn:aws:iam::aws:policy/AmazonDynamoDBFullAccess",
            RoleName=role_name,
        )

    function_name = "moto_test_" + str(uuid4())[0:6]

    # Passing a `RotationLambdaARN` value to `rotate_secret` should invoke lambda
    lambda_conn = boto3.client("lambda", region_name="us-east-1")
    func = lambda_conn.create_function(
        FunctionName=function_name,
        Runtime="python3.11",
        Role=iam_role_arn,
        Handler="lambda_function.lambda_handler",
        Code={"ZipFile": get_rotation_zip_file()},
        Publish=True,
        Environment={"Variables": {"table_name": table_name}},
    )
    lambda_conn.add_permission(
        FunctionName=function_name,
        StatementId="allow_secrets_manager",
        Action="lambda:InvokeFunction",
        Principal="secretsmanager.amazonaws.com",
    )
    lambda_conn.get_waiter("function_active_v2").wait(FunctionName=function_name)

    secrets_conn = boto3.client("secretsmanager", region_name="us-east-1")

    initial_version = secret["VersionId"]

    secrets_conn.rotate_secret(
        SecretId=secret["ARN"],
        RotationLambdaARN=func["FunctionArn"],
        RotationRules={"AutomaticallyAfterDays": 30},
        RotateImmediately=False,
    )

    lambda_conn.delete_function(FunctionName=function_name)

    current_secret = secrets_conn.get_secret_value(
        SecretId=secret["ARN"], VersionStage="AWSCURRENT"
    )
    assert current_secret["SecretString"] == "old_secret"
    assert current_secret["VersionId"] == initial_version

    secret = secrets_conn.get_secret_value(SecretId=secret["ARN"])
    assert secret["SecretString"] == "old_secret"
    assert secret["VersionId"] == initial_version

    dynamodb = boto3.resource("dynamodb", region_name="us-east-1")
    items = dynamodb.Table(table_name).scan()["Items"]

    attempts = 0
    while not items and attempts < 10:
        sleep(5)
        items = dynamodb.Table(table_name).scan()["Items"]
        attempts += 1

    assert items[0]["pending_value"]["VersionStages"] == ["AWSPENDING"]
    assert items[0]["pending_value"]["SecretString"] == "old_secret"
    assert items[0]["pending_value"]["VersionId"] != initial_version


@mock_aws
def test_put_secret_value_on_non_existing_secret():
    conn = boto3.client("secretsmanager", region_name="us-west-2")
    with pytest.raises(ClientError) as cm:
        conn.put_secret_value(
            SecretId=DEFAULT_SECRET_NAME,
            SecretString="foosecret",
            VersionStages=["AWSCURRENT"],
        )

    assert cm.value.response["Error"]["Message"] == (
        "Secrets Manager can't find the specified secret."
    )


@mock_aws
def test_put_secret_value_puts_new_secret():
    conn = boto3.client("secretsmanager", region_name="us-west-2")
    conn.create_secret(Name=DEFAULT_SECRET_NAME, SecretBinary=b"foosecret")
    put_secret_value_dict = conn.put_secret_value(
        SecretId=DEFAULT_SECRET_NAME,
        SecretString="foosecret",
        VersionStages=["AWSCURRENT"],
    )
    version_id = put_secret_value_dict["VersionId"]

    get_secret_value_dict = conn.get_secret_value(
        SecretId=DEFAULT_SECRET_NAME, VersionId=version_id, VersionStage="AWSCURRENT"
    )

    assert get_secret_value_dict
    assert get_secret_value_dict["SecretString"] == "foosecret"


@mock_aws
def test_put_secret_binary_value_puts_new_secret():
    conn = boto3.client("secretsmanager", region_name="us-west-2")
    conn.create_secret(Name=DEFAULT_SECRET_NAME, SecretBinary=b"foosecret")
    put_secret_value_dict = conn.put_secret_value(
        SecretId=DEFAULT_SECRET_NAME,
        SecretBinary=b"foosecret",
        VersionStages=["AWSCURRENT"],
    )
    version_id = put_secret_value_dict["VersionId"]

    get_secret_value_dict = conn.get_secret_value(
        SecretId=DEFAULT_SECRET_NAME, VersionId=version_id, VersionStage="AWSCURRENT"
    )

    assert get_secret_value_dict
    assert get_secret_value_dict["SecretBinary"] == b"foosecret"


@mock_aws
def test_create_and_put_secret_binary_value_puts_new_secret():
    conn = boto3.client("secretsmanager", region_name="us-west-2")
    conn.create_secret(Name=DEFAULT_SECRET_NAME, SecretBinary=b"foosecret")
    conn.put_secret_value(
        SecretId=DEFAULT_SECRET_NAME, SecretBinary=b"foosecret_update"
    )

    latest_secret = conn.get_secret_value(SecretId=DEFAULT_SECRET_NAME)

    assert latest_secret
    assert latest_secret["SecretBinary"] == b"foosecret_update"


@mock_aws
def test_put_secret_binary_requires_either_string_or_binary():
    conn = boto3.client("secretsmanager", region_name="us-west-2")
    with pytest.raises(ClientError) as ire:
        conn.put_secret_value(SecretId=DEFAULT_SECRET_NAME)

    assert ire.value.response["Error"]["Code"] == "InvalidRequestException"
    assert ire.value.response["Error"]["Message"] == (
        "You must provide either SecretString or SecretBinary."
    )


@mock_aws
def test_put_secret_value_can_get_first_version_if_put_twice():
    conn = boto3.client("secretsmanager", region_name="us-west-2")
    conn.create_secret(Name=DEFAULT_SECRET_NAME, SecretBinary=b"foosecret")
    put_secret_value_dict = conn.put_secret_value(
        SecretId=DEFAULT_SECRET_NAME,
        SecretString="first_secret",
        VersionStages=["AWSCURRENT"],
    )
    first_version_id = put_secret_value_dict["VersionId"]
    conn.put_secret_value(
        SecretId=DEFAULT_SECRET_NAME,
        SecretString="second_secret",
        VersionStages=["AWSCURRENT"],
    )

    first_secret_value_dict = conn.get_secret_value(
        SecretId=DEFAULT_SECRET_NAME, VersionId=first_version_id
    )
    first_secret_value = first_secret_value_dict["SecretString"]

    assert first_secret_value == "first_secret"


@mock_aws
def test_put_secret_value_versions_differ_if_same_secret_put_twice():
    conn = boto3.client("secretsmanager", region_name="us-west-2")
    conn.create_secret(Name=DEFAULT_SECRET_NAME, SecretBinary="foosecret")
    put_secret_value_dict = conn.put_secret_value(
        SecretId=DEFAULT_SECRET_NAME,
        SecretString="dupe_secret",
        VersionStages=["AWSCURRENT"],
    )
    first_version_id = put_secret_value_dict["VersionId"]
    put_secret_value_dict = conn.put_secret_value(
        SecretId=DEFAULT_SECRET_NAME,
        SecretString="dupe_secret",
        VersionStages=["AWSCURRENT"],
    )
    second_version_id = put_secret_value_dict["VersionId"]

    assert first_version_id != second_version_id


@mock_aws
def test_put_secret_value_maintains_description_and_tags():
    conn = boto3.client("secretsmanager", region_name="us-west-2")

    previous_response = conn.create_secret(
        Name=DEFAULT_SECRET_NAME,
        SecretString="foosecret",
        Description="desc",
        Tags=[{"Key": "Foo", "Value": "Bar"}, {"Key": "Mykey", "Value": "Myvalue"}],
    )
    previous_version_id = previous_response["VersionId"]

    conn = boto3.client("secretsmanager", region_name="us-west-2")
    current_response = conn.put_secret_value(
        SecretId=DEFAULT_SECRET_NAME,
        SecretString="dupe_secret",
        VersionStages=["AWSCURRENT"],
    )
    current_version_id = current_response["VersionId"]

    secret_details = conn.describe_secret(SecretId=DEFAULT_SECRET_NAME)
    assert secret_details["Tags"] == [
        {"Key": "Foo", "Value": "Bar"},
        {"Key": "Mykey", "Value": "Myvalue"},
    ]
    assert secret_details["Description"] == "desc"
    assert secret_details["VersionIdsToStages"] is not None
    assert previous_version_id in secret_details["VersionIdsToStages"]
    assert current_version_id in secret_details["VersionIdsToStages"]
    assert secret_details["VersionIdsToStages"][previous_version_id] == ["AWSPREVIOUS"]
    assert secret_details["VersionIdsToStages"][current_version_id] == ["AWSCURRENT"]


@mock_aws
def test_can_list_secret_version_ids():
    conn = boto3.client("secretsmanager", region_name="us-west-2")
    conn.create_secret(Name=DEFAULT_SECRET_NAME, SecretBinary="foosecret")
    put_secret_value_dict = conn.put_secret_value(
        SecretId=DEFAULT_SECRET_NAME,
        SecretString="dupe_secret",
        VersionStages=["AWSCURRENT"],
    )
    first_version_id = put_secret_value_dict["VersionId"]
    put_secret_value_dict = conn.put_secret_value(
        SecretId=DEFAULT_SECRET_NAME,
        SecretString="dupe_secret",
        VersionStages=["AWSCURRENT"],
    )
    second_version_id = put_secret_value_dict["VersionId"]

    versions_list = conn.list_secret_version_ids(SecretId=DEFAULT_SECRET_NAME)

    returned_version_ids = [v["VersionId"] for v in versions_list["Versions"]]

    assert [first_version_id, second_version_id].sort() == returned_version_ids.sort()


@mock_aws
def test_put_secret_value_version_stages_response():
    conn = boto3.client("secretsmanager", region_name="us-west-2")

    # Creation.
    first_version_id = "eb41453f-25bb-4025-b7f4-850cfca0ce71"
    conn.create_secret(
        Name=DEFAULT_SECRET_NAME,
        SecretString="first_secret_string",
        ClientRequestToken=first_version_id,
    )

    # Use PutSecretValue to push a new version with new version stages.
    second_version_id = "eb41453f-25bb-4025-b7f4-850cfca0ce72"
    second_version_stages = ["SAMPLESTAGE1", "SAMPLESTAGE0"]
    second_put_res_dict = conn.put_secret_value(
        SecretId=DEFAULT_SECRET_NAME,
        SecretString="second_secret_string",
        VersionStages=second_version_stages,
        ClientRequestToken=second_version_id,
    )
    assert second_put_res_dict
    assert second_put_res_dict["VersionId"] == second_version_id
    assert second_put_res_dict["VersionStages"] == second_version_stages


@mock_aws
def test_put_secret_value_version_stages_pending_response():
    conn = boto3.client("secretsmanager", region_name="us-west-2")

    # Creation.
    first_version_id = "eb41453f-25bb-4025-b7f4-850cfca0ce71"
    conn.create_secret(
        Name=DEFAULT_SECRET_NAME,
        SecretString="first_secret_string",
        ClientRequestToken=first_version_id,
    )

    # Use PutSecretValue to push a new version with new version stages.
    second_version_id = "eb41453f-25bb-4025-b7f4-850cfca0ce72"
    second_version_stages = ["AWSPENDING"]
    second_put_res_dict = conn.put_secret_value(
        SecretId=DEFAULT_SECRET_NAME,
        SecretString="second_secret_string",
        VersionStages=second_version_stages,
        ClientRequestToken=second_version_id,
    )
    assert second_put_res_dict
    assert second_put_res_dict["VersionId"] == second_version_id
    assert second_put_res_dict["VersionStages"] == second_version_stages


@mock_aws
def test_after_put_secret_value_version_stages_can_get_current():
    conn = boto3.client("secretsmanager", region_name="us-west-2")

    # Creation.
    first_version_id = "eb41453f-25bb-4025-b7f4-850cfca0ce71"
    first_secret_string = "first_secret_string"
    conn.create_secret(
        Name=DEFAULT_SECRET_NAME,
        SecretString=first_secret_string,
        ClientRequestToken=first_version_id,
    )

    # Use PutSecretValue to push a new version with new version stages.
    second_version_id = "eb41453f-25bb-4025-b7f4-850cfca0ce72"
    conn.put_secret_value(
        SecretId=DEFAULT_SECRET_NAME,
        SecretString="second_secret_string",
        VersionStages=["SAMPLESTAGE1", "SAMPLESTAGE0"],
        ClientRequestToken=second_version_id,
    )

    # Get current.
    get_dict = conn.get_secret_value(SecretId=DEFAULT_SECRET_NAME)
    assert get_dict
    assert get_dict["VersionId"] == first_version_id
    assert get_dict["SecretString"] == first_secret_string
    assert get_dict["VersionStages"] == ["AWSCURRENT"]


@mock_aws
def test_after_put_secret_value_version_stages_can_get_current_with_custom_version_stage():
    conn = boto3.client("secretsmanager", region_name="us-west-2")

    # Creation.
    first_version_id = "eb41453f-25bb-4025-b7f4-850cfca0ce71"
    first_secret_string = "first_secret_string"
    conn.create_secret(
        Name=DEFAULT_SECRET_NAME,
        SecretString=first_secret_string,
        ClientRequestToken=first_version_id,
    )

    # Use PutSecretValue to push a new version with new version stages.
    second_version_id = "eb41453f-25bb-4025-b7f4-850cfca0ce72"
    conn.put_secret_value(
        SecretId=DEFAULT_SECRET_NAME,
        SecretString="second_secret_string",
        VersionStages=["SAMPLESTAGE1", "SAMPLESTAGE0"],
        ClientRequestToken=second_version_id,
    )
    # Create a third version with one of the old stages
    third_version_id = "eb41453f-25bb-4025-b7f4-850cfca0ce73"
    third_secret_string = "third_secret_string"
    conn.put_secret_value(
        SecretId=DEFAULT_SECRET_NAME,
        SecretString=third_secret_string,
        VersionStages=["SAMPLESTAGE1"],
        ClientRequestToken=third_version_id,
    )

    # Get current with the stage label of the third version.
    get_dict = conn.get_secret_value(
        SecretId=DEFAULT_SECRET_NAME, VersionStage="SAMPLESTAGE1"
    )
    versions = conn.list_secret_version_ids(SecretId=DEFAULT_SECRET_NAME)["Versions"]
    versions_by_key = {version["VersionId"]: version for version in versions}
    # Check if indeed the third version is returned
    assert get_dict
    assert get_dict["VersionId"] == third_version_id
    assert get_dict["SecretString"] == third_secret_string
    assert get_dict["VersionStages"] == ["SAMPLESTAGE1"]
    # Check if all the versions have the proper labels
    assert versions_by_key[first_version_id]["VersionStages"] == ["AWSCURRENT"]
    assert versions_by_key[second_version_id]["VersionStages"] == ["SAMPLESTAGE0"]
    assert versions_by_key[third_version_id]["VersionStages"] == ["SAMPLESTAGE1"]


@mock_aws
def test_after_put_secret_value_version_stages_pending_can_get_current():
    conn = boto3.client("secretsmanager", region_name="us-west-2")

    # Creation.
    first_version_id = "eb41453f-25bb-4025-b7f4-850cfca0ce71"
    first_secret_string = "first_secret_string"
    conn.create_secret(
        Name=DEFAULT_SECRET_NAME,
        SecretString=first_secret_string,
        ClientRequestToken=first_version_id,
    )

    # Use PutSecretValue to push a new version with new version stages.
    pending_version_id = "eb41453f-25bb-4025-b7f4-850cfca0ce72"
    conn.put_secret_value(
        SecretId=DEFAULT_SECRET_NAME,
        SecretString="second_secret_string",
        VersionStages=["AWSPENDING"],
        ClientRequestToken=pending_version_id,
    )

    # Get current.
    get_dict = conn.get_secret_value(SecretId=DEFAULT_SECRET_NAME)
    assert get_dict
    assert get_dict["VersionId"] == first_version_id
    assert get_dict["SecretString"] == first_secret_string
    assert get_dict["VersionStages"] == ["AWSCURRENT"]


@mock_aws
@pytest.mark.parametrize("pass_arn", [True, False])
def test_update_secret(pass_arn):
    conn = boto3.client("secretsmanager", region_name="us-west-2")

    created_secret = conn.create_secret(Name="test-secret", SecretString="foosecret")

    assert created_secret["ARN"]
    assert created_secret["Name"] == "test-secret"
    assert created_secret["VersionId"] != ""

    secret_id = created_secret["ARN"] if pass_arn else "test-secret"

    secret = conn.get_secret_value(SecretId=secret_id)
    assert secret["SecretString"] == "foosecret"

    updated_secret = conn.update_secret(
        SecretId=secret_id,
        SecretString="barsecret",
        Description="new desc",
    )

    assert updated_secret["ARN"]
    assert updated_secret["Name"] == "test-secret"
    assert updated_secret["VersionId"] != ""

    secret = conn.get_secret_value(SecretId=secret_id)
    assert secret["SecretString"] == "barsecret"
    assert created_secret["VersionId"] != updated_secret["VersionId"]

    assert conn.describe_secret(SecretId=secret_id)["Description"] == "new desc"


@mock_aws
@pytest.mark.parametrize("pass_arn", [True, False])
def test_update_secret_updates_last_changed_dates(pass_arn):
    conn = boto3.client("secretsmanager", region_name="us-west-2")

    # create a secret
    created_secret = conn.create_secret(Name="test-secret", SecretString="foosecret")
    secret_id = created_secret["ARN"] if pass_arn else "test-secret"

    # save details for secret before modification
    secret_details_1 = conn.describe_secret(SecretId=secret_id)
    # check if only LastChangedDate changed, CreatedDate should stay the same
    with freeze_time(timedelta(minutes=1)):
        conn.update_secret(SecretId="test-secret", Description="new-desc")
        secret_details_2 = conn.describe_secret(SecretId=secret_id)
        assert secret_details_1["CreatedDate"] == secret_details_2["CreatedDate"]
        if os.environ.get("TEST_SERVER_MODE", "false").lower() == "false":
            assert (
                secret_details_1["LastChangedDate"]
                < secret_details_2["LastChangedDate"]
            )
        else:
            # Can't manipulate time in server mode, so use weaker constraints here
            assert (
                secret_details_1["LastChangedDate"]
                <= secret_details_2["LastChangedDate"]
            )


@mock_aws
def test_update_secret_with_tags_and_description():
    conn = boto3.client("secretsmanager", region_name="us-west-2")

    created_secret = conn.create_secret(
        Name="test-secret",
        SecretString="foosecret",
        Description="desc",
        Tags=[{"Key": "Foo", "Value": "Bar"}, {"Key": "Mykey", "Value": "Myvalue"}],
    )

    assert created_secret["ARN"]
    assert created_secret["Name"] == "test-secret"
    assert created_secret["VersionId"] != ""

    secret = conn.get_secret_value(SecretId="test-secret")
    assert secret["SecretString"] == "foosecret"

    updated_secret = conn.update_secret(
        SecretId="test-secret", SecretString="barsecret"
    )

    assert updated_secret["ARN"]
    assert updated_secret["Name"] == "test-secret"
    assert updated_secret["VersionId"] != ""

    secret = conn.get_secret_value(SecretId="test-secret")
    assert secret["SecretString"] == "barsecret"
    assert created_secret["VersionId"] != updated_secret["VersionId"]
    secret_details = conn.describe_secret(SecretId="test-secret")
    assert secret_details["Tags"] == [
        {"Key": "Foo", "Value": "Bar"},
        {"Key": "Mykey", "Value": "Myvalue"},
    ]
    assert secret_details["Description"] == "desc"


@mock_aws
def test_update_secret_with_KmsKeyId():
    conn = boto3.client("secretsmanager", region_name="us-west-2")

    created_secret = conn.create_secret(
        Name="test-secret", SecretString="foosecret", KmsKeyId="foo_arn"
    )

    assert created_secret["ARN"]
    assert created_secret["Name"] == "test-secret"
    assert created_secret["VersionId"] != ""

    secret = conn.get_secret_value(SecretId="test-secret")
    assert secret["SecretString"] == "foosecret"

    secret_details = conn.describe_secret(SecretId="test-secret")
    assert secret_details["KmsKeyId"] == "foo_arn"

    updated_secret = conn.update_secret(
        SecretId="test-secret", SecretString="barsecret", KmsKeyId="bar_arn"
    )

    assert updated_secret["ARN"]
    assert updated_secret["Name"] == "test-secret"
    assert updated_secret["VersionId"] != ""

    secret = conn.get_secret_value(SecretId="test-secret")
    assert secret["SecretString"] == "barsecret"
    assert created_secret["VersionId"] != updated_secret["VersionId"]

    secret_details = conn.describe_secret(SecretId="test-secret")
    assert secret_details["KmsKeyId"] == "bar_arn"


@mock_aws
def test_update_secret_which_does_not_exit():
    conn = boto3.client("secretsmanager", region_name="us-west-2")

    with pytest.raises(ClientError) as cm:
        conn.update_secret(SecretId="test-secret", SecretString="barsecret")

    assert (
        "Secrets Manager can't find the specified secret."
        == cm.value.response["Error"]["Message"]
    )


@mock_aws
def test_update_secret_marked_as_deleted():
    conn = boto3.client("secretsmanager", region_name="us-west-2")

    conn.create_secret(Name="test-secret", SecretString="foosecret")
    conn.delete_secret(SecretId="test-secret")

    with pytest.raises(ClientError) as cm:
        conn.update_secret(SecretId="test-secret", SecretString="barsecret")

    assert (
        "because it was marked for deletion." in cm.value.response["Error"]["Message"]
    )


@mock_aws
def test_update_secret_marked_as_deleted_after_restoring():
    conn = boto3.client("secretsmanager", region_name="us-west-2")

    conn.create_secret(Name="test-secret", SecretString="foosecret")
    conn.delete_secret(SecretId="test-secret")
    conn.restore_secret(SecretId="test-secret")

    updated_secret = conn.update_secret(
        SecretId="test-secret", SecretString="barsecret"
    )

    assert updated_secret["ARN"]
    assert updated_secret["Name"] == "test-secret"
    assert updated_secret["VersionId"] != ""


@mock_aws
@pytest.mark.parametrize("pass_arn", [True, False])
def test_tag_resource(pass_arn):
    conn = boto3.client("secretsmanager", region_name="us-west-2")
    created_secret = conn.create_secret(Name="test-secret", SecretString="foosecret")
    secret_id = created_secret["ARN"] if pass_arn else "test-secret"

    response = conn.describe_secret(SecretId=secret_id)
    assert "Tags" not in response

    conn.tag_resource(
        SecretId=secret_id, Tags=[{"Key": "FirstTag", "Value": "SomeValue"}]
    )
    conn.tag_resource(
        SecretId="test-secret", Tags=[{"Key": "FirstTag", "Value": "SomeOtherValue"}]
    )
    conn.tag_resource(
        SecretId=secret_id, Tags=[{"Key": "SecondTag", "Value": "AnotherValue"}]
    )

    secrets = conn.list_secrets()
    assert secrets["SecretList"][0].get("Tags") == [
        {"Key": "FirstTag", "Value": "SomeOtherValue"},
        {"Key": "SecondTag", "Value": "AnotherValue"},
    ]

    with pytest.raises(ClientError) as cm:
        conn.tag_resource(
            SecretId="dummy-test-secret",
            Tags=[{"Key": "FirstTag", "Value": "SomeValue"}],
        )

    assert (
        "Secrets Manager can't find the specified secret."
        == cm.value.response["Error"]["Message"]
    )


@mock_aws
@pytest.mark.parametrize("pass_arn", [True, False])
def test_untag_resource(pass_arn):
    conn = boto3.client("secretsmanager", region_name="us-west-2")
    created_secret = conn.create_secret(Name="test-secret", SecretString="foosecret")
    secret_id = created_secret["ARN"] if pass_arn else "test-secret"
    conn.tag_resource(
        SecretId=secret_id,
        Tags=[
            {"Key": "FirstTag", "Value": "SomeValue"},
            {"Key": "SecondTag", "Value": "SomeValue"},
        ],
    )

    conn.untag_resource(SecretId=secret_id, TagKeys=["FirstTag"])
    secrets = conn.list_secrets()
    assert secrets["SecretList"][0].get("Tags") == [
        {"Key": "SecondTag", "Value": "SomeValue"},
    ]

    with pytest.raises(ClientError) as cm:
        conn.untag_resource(SecretId="dummy-test-secret", TagKeys=["FirstTag"])

    assert (
        "Secrets Manager can't find the specified secret."
        == cm.value.response["Error"]["Message"]
    )

    conn.tag_resource(
        SecretId=secret_id, Tags=[{"Key": "FirstTag", "Value": "SomeValue"}]
    )
    conn.untag_resource(SecretId=secret_id, TagKeys=["FirstTag", "SecondTag"])
    response = conn.describe_secret(SecretId=secret_id)
    assert "Tags" in response
    assert response["Tags"] == []


@mock_aws
def test_secret_versions_to_stages_attribute_discrepancy():
    client = boto3.client("secretsmanager", region_name="us-west-2")

    resp = client.create_secret(Name=DEFAULT_SECRET_NAME, SecretString="foosecret")
    previous_version_id = resp["VersionId"]

    resp = client.put_secret_value(
        SecretId=DEFAULT_SECRET_NAME,
        SecretString="dupe_secret",
        VersionStages=["AWSCURRENT"],
    )
    current_version_id = resp["VersionId"]

    secret = client.describe_secret(SecretId=DEFAULT_SECRET_NAME)
    describe_vtos = secret["VersionIdsToStages"]
    assert describe_vtos[current_version_id] == ["AWSCURRENT"]
    assert describe_vtos[previous_version_id] == ["AWSPREVIOUS"]

    secret = client.list_secrets(
        Filters=[{"Key": "name", "Values": [DEFAULT_SECRET_NAME]}]
    ).get("SecretList")[0]
    list_vtos = secret["SecretVersionsToStages"]
    assert list_vtos[current_version_id] == ["AWSCURRENT"]
    assert list_vtos[previous_version_id] == ["AWSPREVIOUS"]

    assert describe_vtos == list_vtos


@mock_aws
def test_update_secret_with_client_request_token():
    client = boto3.client("secretsmanager", region_name="us-west-2")
    secret_name = "test-secret"
    client_request_token = str(uuid4())

    client.create_secret(Name=secret_name, SecretString="first-secret")
    updated_secret = client.update_secret(
        SecretId=secret_name,
        SecretString="second-secret",
        ClientRequestToken=client_request_token,
    )
    assert client_request_token == updated_secret["VersionId"]
    updated_secret = client.update_secret(
        SecretId=secret_name, SecretString="third-secret"
    )
    assert client_request_token != updated_secret["VersionId"]


@secretsmanager_aws_verified
@pytest.mark.aws_verified
def test_update_secret_version_stage_manually(secret=None):
    sm_client = boto3.client("secretsmanager", "us-east-1")
    current_version = sm_client.put_secret_value(
        SecretId=secret["ARN"],
        SecretString="previous_secret",
        VersionStages=["AWSCURRENT"],
    )["VersionId"]

    initial_secret = sm_client.get_secret_value(
        SecretId=secret["ARN"], VersionStage="AWSCURRENT"
    )
    assert initial_secret["VersionStages"] == ["AWSCURRENT"]
    assert initial_secret["SecretString"] == "previous_secret"

    token = str(uuid4())
    sm_client.put_secret_value(
        SecretId=secret["ARN"],
        ClientRequestToken=token,
        SecretString="new_secret",
        VersionStages=["AWSPENDING"],
    )

    pending_secret = sm_client.get_secret_value(
        SecretId=secret["ARN"], VersionStage="AWSPENDING"
    )
    assert pending_secret["VersionStages"] == ["AWSPENDING"]
    assert pending_secret["SecretString"] == "new_secret"

    sm_client.update_secret_version_stage(
        SecretId=secret["ARN"],
        VersionStage="AWSCURRENT",
        MoveToVersionId=token,
        RemoveFromVersionId=current_version,
    )

    current_secret = sm_client.get_secret_value(
        SecretId=secret["ARN"], VersionStage="AWSCURRENT"
    )
    assert list(sorted(current_secret["VersionStages"])) == ["AWSCURRENT", "AWSPENDING"]
    assert current_secret["SecretString"] == "new_secret"

    previous_secret = sm_client.get_secret_value(
        SecretId=secret["ARN"], VersionStage="AWSPREVIOUS"
    )
    assert previous_secret["VersionStages"] == ["AWSPREVIOUS"]
    assert previous_secret["SecretString"] == "previous_secret"


@secretsmanager_aws_verified
@pytest.mark.aws_verified
def test_update_secret_version_stage_dont_specify_current_stage(secret=None):
    sm_client = boto3.client("secretsmanager", "us-east-1")
    current_version = sm_client.put_secret_value(
        SecretId=secret["ARN"],
        SecretString="previous_secret",
        VersionStages=["AWSCURRENT"],
    )["VersionId"]

    token = str(uuid4())
    sm_client.put_secret_value(
        SecretId=secret["ARN"],
        ClientRequestToken=token,
        SecretString="new_secret",
        VersionStages=["AWSPENDING"],
    )

    # Without specifying version that currently has stage AWSCURRENT
    with pytest.raises(ClientError) as exc:
        sm_client.update_secret_version_stage(
            SecretId=secret["ARN"], VersionStage="AWSCURRENT", MoveToVersionId=token
        )
    err = exc.value.response["Error"]
    assert err["Code"] == "InvalidParameterException"
    assert (
        err["Message"]
        == f"The parameter RemoveFromVersionId can't be empty. Staging label AWSCURRENT is currently attached to version {current_version}, so you must explicitly reference that version in RemoveFromVersionId."
    )


@mock_aws
@pytest.mark.skipif(
    not settings.TEST_DECORATOR_MODE, reason="Can't access the id manager in proxy mode"
)
def test_create_secret_custom_id(set_custom_id):
    secret_suffix = "randomSuffix"
    secret_name = "secret-name"
    region_name = "us-east-1"

    client = boto3.client("secretsmanager", region_name=region_name)

    set_custom_id(
        SecretsManagerSecretIdentifier(DEFAULT_ACCOUNT_ID, region_name, secret_name),
        secret_suffix,
    )
    secret = client.create_secret(Name=secret_name, SecretString="my secret")

    assert secret["ARN"].split(":")[-1] == f"{secret_name}-{secret_suffix}"


@mock_aws
def test_create_secret_with_tag_custom_id(set_custom_id):
    secret_suffix = "randomSuffix"
    secret_name = "secret-name"

    client = boto3.client("secretsmanager", "us-east-1")

    secret = client.create_secret(
        Name=secret_name,
        SecretString="my secret",
        Tags=[{"Key": TAG_KEY_CUSTOM_ID, "Value": secret_suffix}],
    )

    assert secret["ARN"].split(":")[-1] == f"{secret_name}-{secret_suffix}"


@mock_aws
@pytest.mark.skipif(
    not settings.TEST_DECORATOR_MODE,
    reason="Can't modify backend directly if not in decorator mode.",
)
def test_aws_managed_secret(set_custom_id):
    # We have to poke directly at the backend because technically this secret
    # would be managed by RDS, and wouldn't be able to be created via the
    # public API (due to the restricted 'aws' tag prefix).
    backend = secretsmanager_backends[DEFAULT_ACCOUNT_ID]["us-east-1"]
    secret = backend.create_managed_secret("rds", "secret-managed-by-rds", "53cr3t")
    assert secret.kms_key_id == "alias/aws/secretsmanager"
    client = boto3.client("secretsmanager", region_name="us-east-1")
    resp = client.describe_secret(SecretId=secret.arn)
    assert "KmsKeyId" not in resp
    assert resp["OwningService"] == "rds"
    owning_service_filter = {"Key": "owning-service", "Values": ["rds"]}
    resp = client.list_secrets(Filters=[owning_service_filter])
    assert len(resp["SecretList"]) == 1
    assert resp["SecretList"][0]["ARN"] == secret.arn
    assert resp["SecretList"][0]["OwningService"] == secret.owning_service
    resp = client.batch_get_secret_value(Filters=[owning_service_filter])
    assert len(resp["SecretValues"]) == 1
    assert resp["SecretValues"][0]["ARN"] == secret.arn
    assert resp["SecretValues"][0]["SecretString"] == "53cr3t"

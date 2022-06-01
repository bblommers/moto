import boto3
import pytest
import sure  # noqa # pylint: disable=unused-import
import unittest

from botocore.exceptions import ClientError
from moto import mock_ec2, mock_kinesis, mock_s3, settings
from unittest import SkipTest

"""
Test the different ways that the decorator can be used
"""


@mock_ec2
def test_basic_decorator():
    client = boto3.client("ec2", region_name="us-west-1")
    client.describe_addresses()["Addresses"].should.equal([])


@pytest.fixture
def aws_credentials(monkeypatch):
    """Mocked AWS Credentials for moto."""
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
    monkeypatch.setenv("AWS_SECURITY_TOKEN", "testing")
    monkeypatch.setenv("AWS_SESSION_TOKEN", "testing")


@pytest.mark.network
def test_context_manager(aws_credentials):  # pylint: disable=unused-argument
    client = boto3.client("ec2", region_name="us-west-1")
    with pytest.raises(ClientError) as exc:
        client.describe_addresses()
    err = exc.value.response["Error"]
    err["Code"].should.equal("AuthFailure")
    err["Message"].should.equal(
        "AWS was not able to validate the provided access credentials"
    )

    with mock_ec2():
        client = boto3.client("ec2", region_name="us-west-1")
        client.describe_addresses()["Addresses"].should.equal([])


@pytest.mark.network
def test_decorator_start_and_stop():
    if settings.TEST_SERVER_MODE:
        raise SkipTest("Authentication always works in ServerMode")
    mock = mock_ec2()
    mock.start()
    client = boto3.client("ec2", region_name="us-west-1")
    client.describe_addresses()["Addresses"].should.equal([])
    mock.stop()

    with pytest.raises(ClientError) as exc:
        client.describe_addresses()
    err = exc.value.response["Error"]
    err["Code"].should.equal("AuthFailure")
    err["Message"].should.equal(
        "AWS was not able to validate the provided access credentials"
    )


@mock_ec2
def test_decorater_wrapped_gets_set():
    """
    Moto decorator's __wrapped__ should get set to the tests function
    """
    test_decorater_wrapped_gets_set.__wrapped__.__name__.should.equal(
        "test_decorater_wrapped_gets_set"
    )


@mock_ec2
class Tester(object):
    def test_the_class(self):
        client = boto3.client("ec2", region_name="us-west-1")
        client.describe_addresses()["Addresses"].should.equal([])

    def test_still_the_same(self):
        client = boto3.client("ec2", region_name="us-west-1")
        client.describe_addresses()["Addresses"].should.equal([])


@mock_s3
class TesterWithSetup(unittest.TestCase):
    def setUp(self):
        self.client = boto3.client("s3")
        self.client.create_bucket(Bucket="mybucket")

    def test_still_the_same(self):
        buckets = self.client.list_buckets()["Buckets"]
        bucket_names = [b["Name"] for b in buckets]
        bucket_names.should.equal(["mybucket"])


@mock_s3
class TesterWithStaticmethod(object):
    @staticmethod
    def static(*args):
        assert not args or not isinstance(args[0], TesterWithStaticmethod)

    def test_no_instance_sent_to_staticmethod(self):
        self.static()


@mock_s3
class TestWithSetup_UppercaseU(unittest.TestCase):
    def setUp(self):
        # This method will be executed automatically, provided we extend the TestCase-class
        s3 = boto3.client("s3", region_name="us-east-1")
        s3.create_bucket(Bucket="mybucket")

    def test_should_find_bucket(self):
        s3 = boto3.client("s3", region_name="us-east-1")
        self.assertIsNotNone(s3.head_bucket(Bucket="mybucket"))

    def test_should_not_find_unknown_bucket(self):
        s3 = boto3.client("s3", region_name="us-east-1")
        with pytest.raises(ClientError):
            s3.head_bucket(Bucket="unknown_bucket")


@mock_s3
class TestWithSetup_LowercaseU:
    def setup(self, *args):  # pylint: disable=unused-argument
        # This method will be executed automatically using pytest
        s3 = boto3.client("s3", region_name="us-east-1")
        s3.create_bucket(Bucket="mybucket")

    def test_should_find_bucket(self):
        s3 = boto3.client("s3", region_name="us-east-1")
        assert s3.head_bucket(Bucket="mybucket") is not None

    def test_should_not_find_unknown_bucket(self):
        s3 = boto3.client("s3", region_name="us-east-1")
        with pytest.raises(ClientError):
            s3.head_bucket(Bucket="unknown_bucket")


@mock_s3
class TestWithSetupMethod:
    # Xunit style setup-method
    def setup_method(self, *args):  # pylint: disable=unused-argument
        # This method will be executed automatically using pytest
        s3 = boto3.client("s3", region_name="us-east-1")
        s3.create_bucket(Bucket="mybucket")

    def test_should_find_bucket(self):
        s3 = boto3.client("s3", region_name="us-east-1")
        assert s3.head_bucket(Bucket="mybucket") is not None

    def test_should_not_find_unknown_bucket(self):
        s3 = boto3.client("s3", region_name="us-east-1")
        with pytest.raises(ClientError):
            s3.head_bucket(Bucket="unknown_bucket")


@mock_kinesis
class TestKinesisUsingSetupMethod:
    def setup_method(self, *args):  # pylint: disable=unused-argument
        self.stream_name = "test_stream"
        print(f"setup on {self}")
        #self.boto3_kinesis_client = boto3.client("kinesis", region_name="us-east-1")
        #self.boto3_kinesis_client.create_stream(
        #    StreamName=self.stream_name, ShardCount=1
        #)

    def test_stream_creation(self):
        self.stream_name.should.equal("test_stream")

    def test_stream_recreation(self):
        # The setup-method will run again for this test
        # The fact that it passes, means the state was reset
        # Otherwise it would complain about a stream already existing
        pass


@mock_s3
class TestWithInvalidSetupMethod:
    def setupmethod(self):
        s3 = boto3.client("s3", region_name="us-east-1")
        s3.create_bucket(Bucket="mybucket")

    def test_should_not_find_bucket(self):
        # Name of setupmethod is not recognized, so it will not be executed
        s3 = boto3.client("s3", region_name="us-east-1")
        with pytest.raises(ClientError):
            s3.head_bucket(Bucket="mybucket")


@mock_s3
class TestWithPublicMethod(unittest.TestCase):
    def ensure_bucket_exists(self):
        s3 = boto3.client("s3", region_name="us-east-1")
        s3.create_bucket(Bucket="mybucket")

    def test_should_find_bucket(self):
        self.ensure_bucket_exists()

        s3 = boto3.client("s3", region_name="us-east-1")
        s3.head_bucket(Bucket="mybucket").shouldnt.equal(None)

    def test_should_not_find_bucket(self):
        s3 = boto3.client("s3", region_name="us-east-1")
        with pytest.raises(ClientError):
            s3.head_bucket(Bucket="mybucket")


@mock_s3
class TestWithPseudoPrivateMethod(unittest.TestCase):
    def _ensure_bucket_exists(self):
        s3 = boto3.client("s3", region_name="us-east-1")
        s3.create_bucket(Bucket="mybucket")

    def test_should_find_bucket(self):
        self._ensure_bucket_exists()
        s3 = boto3.client("s3", region_name="us-east-1")
        s3.head_bucket(Bucket="mybucket").shouldnt.equal(None)

    def test_should_not_find_bucket(self):
        s3 = boto3.client("s3", region_name="us-east-1")
        with pytest.raises(ClientError):
            s3.head_bucket(Bucket="mybucket")


@mock_s3
class Baseclass(unittest.TestCase):
    def setUp(self):
        self.s3 = boto3.resource("s3", region_name="us-east-1")
        self.client = boto3.client("s3", region_name="us-east-1")
        self.test_bucket = self.s3.Bucket("testbucket")
        self.test_bucket.create()

    def tearDown(self):
        # The bucket will still exist at this point
        self.test_bucket.delete()


@mock_s3
class TestSetUpInBaseClass(Baseclass):
    def test_a_thing(self):
        # Verify that we can 'see' the setUp-method in the parent class
        self.client.head_bucket(Bucket="testbucket").shouldnt.equal(None)


@mock_s3
class TestWithNestedClasses:
    class NestedClass(unittest.TestCase):
        def _ensure_bucket_exists(self):
            s3 = boto3.client("s3", region_name="us-east-1")
            s3.create_bucket(Bucket="bucketclass1")

        def test_should_find_bucket(self):
            self._ensure_bucket_exists()
            s3 = boto3.client("s3", region_name="us-east-1")
            s3.head_bucket(Bucket="bucketclass1")

    class NestedClass2(unittest.TestCase):
        def _ensure_bucket_exists(self):
            s3 = boto3.client("s3", region_name="us-east-1")
            s3.create_bucket(Bucket="bucketclass2")

        def test_should_find_bucket(self):
            self._ensure_bucket_exists()
            s3 = boto3.client("s3", region_name="us-east-1")
            s3.head_bucket(Bucket="bucketclass2")

        def test_should_not_find_bucket_from_different_class(self):
            s3 = boto3.client("s3", region_name="us-east-1")
            with pytest.raises(ClientError):
                s3.head_bucket(Bucket="bucketclass1")

    class TestWithSetup(unittest.TestCase):
        def setUp(self):
            s3 = boto3.client("s3", region_name="us-east-1")
            s3.create_bucket(Bucket="mybucket")

        def test_should_find_bucket(self):
            s3 = boto3.client("s3", region_name="us-east-1")
            s3.head_bucket(Bucket="mybucket")

            s3.create_bucket(Bucket="bucketinsidetest")

        def test_should_not_find_bucket_from_test_method(self):
            s3 = boto3.client("s3", region_name="us-east-1")
            s3.head_bucket(Bucket="mybucket")

            with pytest.raises(ClientError):
                s3.head_bucket(Bucket="bucketinsidetest")

    class TestThatDoesNotExtendUnitTest:
        def test_should_not_have_initial_bucket(self):
            list(boto3.resource("s3").buckets.all()).should.have.length_of(0)

        def test_bucket_can_be_created(self):
            s3 = boto3.client("s3", region_name="us-east-1")
            s3.create_bucket(Bucket="test_bucket")
            list(boto3.resource("s3").buckets.all()).should.have.length_of(1)


@mock_s3
class BaseClassWithSetupButDoesNotExtendTestCase:
    def setUp(self):
        self.client = boto3.client("s3")
        self.client.create_bucket(Bucket="test_bucket")

        list(boto3.resource("s3").buckets.all()).should.have.length_of(1)


class TestClassExtendingMotoAndUnit(
    BaseClassWithSetupButDoesNotExtendTestCase, unittest.TestCase
):

    # setUp is executed first, because we extend TestCase
    # Moto should recognize this, and reset the state before the setUp-method
    # (Even though, when declaring the decorator, Moto doesn't know yet that we will extend TestCase)
    def test_bucket_created_in_parent_setup_exists(self):
        list(boto3.resource("s3").buckets.all()).should.have.length_of(1)

    def test_second_bucket_can_be_created(self):
        self.client.create_bucket(Bucket="test_bucket2")
        list(boto3.resource("s3").buckets.all()).should.have.length_of(2)


@mock_s3
class TestWithTearDown_LowerCase:

    def setup(self):
        self.client = boto3.client("s3")

    def test_teardown_is_mocked(self):
        self.client.create_bucket(Bucket="test_bucket")

    def teardown(self):
        self.client.list_buckets()["Buckets"].should.have.length_of(1)


@mock_s3
class TestWithTearDown_Underscore:

    def setup_method(self, *args):  # pylint: disable=unused-argument
        print(self)
        print("hi")
        self.client = boto3.client("s3")
        print(self.client)

    def test_teardown_is_mocked(self):
        print(self)
        print("hi2")
        print(self.client)
        #self.client.create_bucket(Bucket="test_bucket")

    #def teardown_method(self, *args):
    #    print("hi3")
    #    self.client.list_buckets()["Buckets"].should.have.length_of(1)

# TODO: test teardown behaviour
# TODO: test static setup methods
# TODO: only decorate() around setup/test-/teardown methods, to improve performance

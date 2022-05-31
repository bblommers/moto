import sure  # noqa # pylint: disable=unused-import
import boto3
from moto import mock_ec2, mock_sqs, settings
from moto.core.models import botocore_stubber
from tests import EXAMPLE_AMI_ID


def test_context_manager_returns_mock():
    with mock_sqs() as sqs_mock:
        conn = boto3.client("sqs", region_name="us-west-1")
        conn.create_queue(QueueName="queue1")

        if not settings.TEST_SERVER_MODE:
            list(sqs_mock.backends["us-west-1"].queues.keys()).should.equal(["queue1"])


class TestCaseUsingMultipleMocks:
    def test_resets_using_multiple_contextmanagers(self):
        with mock_ec2():
            client = boto3.client("ec2")
            resp = client.run_instances(ImageId=EXAMPLE_AMI_ID, MinCount=1, MaxCount=1)
            my_id1 = resp["Instances"][0]["InstanceId"]

            instances = client.describe_instances()["Reservations"][0]["Instances"]
            instance_ids = [i["InstanceId"] for i in instances]
            instance_ids.should.contain(my_id1)

            with mock_ec2():
                # Starting another mock made us lose context
                client.describe_instances()["Reservations"].should.equal([])

                # We can create another
                resp = client.run_instances(
                    ImageId=EXAMPLE_AMI_ID, MinCount=1, MaxCount=1
                )
                my_id2 = resp["Instances"][0]["InstanceId"]

            # We only remember the last Instance at this point
            instances = client.describe_instances()["Reservations"][0]["Instances"]
            instance_ids = [i["InstanceId"] for i in instances]
            instance_ids.should.contain(my_id2)

    def test_number_of_registered_mocks(self):
        # The stubber should be disabled by default
        botocore_stubber.enabled.should.equal(False)
        botocore_stubber.methods.should.equal({})

        with mock_ec2():
            # Within a mock, the stubber should be enabled and mock a number of URL's
            botocore_stubber.enabled.should.equal(True)
            botocore_stubber.methods.should.have.length_of(7)
            for response_per_http_method in botocore_stubber.methods.values():
                response_per_http_method.should.have.length_of(16)

            with mock_ec2():
                # Mocking it twice make no difference
                botocore_stubber.enabled.should.equal(True)
                botocore_stubber.methods.should.have.length_of(7)
                for response_per_http_method in botocore_stubber.methods.values():
                    response_per_http_method.should.have.length_of(16)

            with mock_sqs():
                # Mocking another service should add more methods
                botocore_stubber.enabled.should.equal(True)
                botocore_stubber.methods.should.have.length_of(7)
                for response_per_http_method in botocore_stubber.methods.values():
                    response_per_http_method.should.have.length_of(20)

            # Exiting the second context should not break the mock of the initial stubber
            botocore_stubber.enabled.should.equal(True)
            botocore_stubber.methods.should.have.length_of(7)
            for response_per_http_method in botocore_stubber.methods.values():
                response_per_http_method.should.have.length_of(20)

        # Only outside both mocks should we have zero methods
        botocore_stubber.enabled.should.equal(False)
        botocore_stubber.methods.should.equal({})

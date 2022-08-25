import json
import re
import sure  # noqa # pylint: disable=unused-import

import moto.server as server

"""
Test the different server responses
"""


def test_cloudformation_server_get():
    backend = server.create_backend_app("cloudformation")
    stack_name = "test stack"
    test_client = backend.test_client()
    template_body = {"Resources": {}}
    create_stack_resp = test_client.action_data(
        "CreateStack", StackName=stack_name, TemplateBody=json.dumps(template_body)
    )
    create_stack_resp.should.match(
        r"<CreateStackResponse>.*<CreateStackResult>.*<StackId>.*</StackId>.*</CreateStackResult>.*</CreateStackResponse>",
        re.DOTALL,
    )
    stack_id_from_create_response = re.search(
        "<StackId>(.*)</StackId>", create_stack_resp
    ).groups()[0]

    list_stacks_resp = test_client.action_data("ListStacks")
    list_stacks_resp.should.contain(
        f"<StackId>{stack_id_from_create_response}</StackId>"
    )

def create_method_integration(client, api_id, httpMethod="GET"):
    resources = client.get_resources(restApiId=api_id)
    root_id = [resource for resource in resources["items"] if resource["path"] == "/"][
        0
    ]["id"]
    client.put_method(
        restApiId=api_id,
        resourceId=root_id,
        httpMethod=httpMethod,
        authorizationType="NONE",
    )
    client.put_method_response(
        restApiId=api_id, resourceId=root_id, httpMethod=httpMethod, statusCode="200"
    )
    client.put_integration(
        restApiId=api_id,
        resourceId=root_id,
        httpMethod=httpMethod,
        type="HTTP",
        uri="http://httpbin.org/robots.txt",
        integrationHttpMethod="POST",
    )
    client.put_integration_response(
        restApiId=api_id,
        resourceId=root_id,
        httpMethod=httpMethod,
        statusCode="200",
        responseTemplates={},
    )
    return root_id

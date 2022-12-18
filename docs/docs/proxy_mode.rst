.. _proxy_mode:

.. role:: bash(code)
   :language: bash

.. role:: raw-html(raw)
    :format: html

================================
Proxy Mode
================================

Moto has can be run as a proxy. All the official AWS SDK's can be configured to use this proxy.  :raw-html:`<br />`
Instead of sending requests through to AWS, Moto will mock them.

Installation
-------------

Install the required dependencies using:

.. code:: bash

    pip install moto[proxy]


You can then start the proxy like this:

.. code:: bash

    $ moto_proxy


Quick usage
--------------
The help command shows a quick-guide on how to connect to the proxy.
.. code-block:: bash

    $ moto_proxy --help


Extended Configuration
------------------------

To use the MotoProxy while running your tests, the AWS SDK needs to know two things:

 - The proxy endpoint
 - How to deal with SSL

To set the proxy endpoint, most SDK's allow you to set a `HTTPS_PROXY`-environment variable.

Because the proxy does not have an approved SSL certificate, the SDK will not trust the proxy by default. This means that the SDK has to be configured to either

1. Accept the proxy's custom certificate, by setting the `AWS_CA_BUNDLE`-environment variable
2. Allow unverified SSL certificates

The `AWS_CA_BUNDLE` needs to point to the location of the CA certificate that comes with Moto.  :raw-html:`<br />`
You can run `moto_proxy --help` to get the exact location of this certificate, depending on where Moto is installed.

Alternatively, you can download the certificate from Github: `TODO:: LINK`


Python Configuration
--------------------------

When running tests using the boto3-SDK, a custom environment variable is exposed that configures everything automatically:

.. code-block:: bash

    TEST_PROXY_MODE=true pytest

To configure this manually:

.. code-block:: python

    from botocore.config import Config

    config = Config(proxies={"https": "http://localhost:5005"})
    client = boto3.client("s3", config=config, verify=False)

AWS CLI Configuration:
------------------------------

.. code-block:: bash

    export HTTPS_PROXY=http://localhost:5005
    aws cloudformation list-stacks --no-verify-ssl

Or by configuring the AWS_CA_BUNDLE:

.. code-block:: bash

    export HTTPS_PROXY=http://localhost:5005
    export AWS_CA_BUNDLE=/location/of/moto/ca/cert.crt
    aws cloudformation list-stacks


Terraform Configuration
------------------------------

.. code-block::

    provider "aws" {
        region                      = "us-east-1"
        http_proxy                  = "http://localhost:5005"
        custom_ca_bundle            = "/location/of/moto/ca/cert.crt"
        # OR
        insecure                    = true
    }

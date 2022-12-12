Done:

 - Proxy server, can be started on port 5005 with `moto_proxy`
 - Proxy<-->Server Certificates are included, so calls to AWS can be intercepted
 - Intercept calls to AWS, and redirect to Moto instead
 - Logs all requests
 - Contains a TEST_PROXY_MODE-env so we can run our internal tests using this proxy

Notes: 

 - The proxy runs on HTTP, so does not return it's own SSL certificate
   With AWS CLI, you have to ignore SSL verification because of this: `aws cloudformation list-stacks --no-verify-ssl`

TODO:
 - Change all `if TEST_SERVER_MODE` in our tests to `if DECORATOR_MODE`
 - Change all `if not TEST_SERVER_MODE` to `if not TEST_SERVER_MODE and not TEST_PROXY_MODE`
 - Test if we can set ports
 - Determine what to do with incoming requests not intended for AWS: let them through? Warning? Error?
 - Determine how to run the proxy on HTTPS, so we don't need '-no-verify-ssl'
 - Add env variables so that calls from within a Lambda are redirected to our proxy
 - Figure out how to set `-no-verify-ssl` within a Lambda
 - Create the certificates on-demand, as the certicates currently included are only valid for 10 years (?) See `setup_https_intercept.py`
 - Test Windows/Mac, as certificate generation depends on OpenSSL

Cleanup:
 - Remove connect_intercept

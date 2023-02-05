Done:

 - Proxy server, can be started on port 5005 with `moto_proxy`
 - Proxy<-->Server Certificates are included, so calls to AWS can be intercepted
 - Intercept calls to AWS, and redirect to Moto instead
 - Logs all requests
 - Contains a TEST_PROXY_MODE-env so we can run our internal tests using this proxy
 - Documents how to set the AWS_CA_BUNDLE environment variable, avoiding the need for `--no-verify-ssl`

TODO:
 - Change all `if TEST_SERVER_MODE` in our tests to `if DECORATOR_MODE`
 - Do not intercept calls inside our normal decorator if `HTTPS_PROXY` is set
 - Set HTTPS_PROXY when MotoProxy is running, so that outgoing requests are forwarded to itself
 - Change all `if not TEST_SERVER_MODE` to `if not TEST_SERVER_MODE and not TEST_PROXY_MODE`
 - Determine what to do with incoming requests not intended for AWS: let them through? Warning? Error?
 - Add env variables so that calls from within a Lambda are redirected to our proxy
 - Figure out how to set `-no-verify-ssl` within a Lambda
 - Create the certificates on-demand, as the certicates currently included are only valid for 10 years (?) See `setup_https_intercept.py`
 - Test Windows/Mac, as certificate generation depends on OpenSSL

Cleanup:
 - Remove connect_intercept

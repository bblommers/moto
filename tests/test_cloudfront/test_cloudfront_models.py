from moto.cloudfront.models import ForwardedValues, CustomOriginConfig, Methods


class TestForwardedValues:
    def test_whitelisted_name_no_values(self):
        config = {'Cookies': {'WhitelistedNames': {'Items': None}}}
        values = ForwardedValues(config)
        values.whitelisted_names.should.equal([])

    def test_whitelisted_name_single_value(self):
        config = {'Cookies': {'WhitelistedNames': {'Items': {'Name': 'x-amz-target'}}}}
        values = ForwardedValues(config)
        values.whitelisted_names.should.equal(["x-amz-target"])

    def test_whitelisted_name_multiple_values(self):
        config = {'Cookies': {'WhitelistedNames': {'Items': {'Name': ['x-amz-target', 'x-custom']}}}}
        values = ForwardedValues(config)
        values.whitelisted_names.should.equal(["x-amz-target", "x-custom"])


class TestCustomOriginConfig:

    def test_properties(self):
        config = {'HTTPSPort': '443', 'OriginKeepaliveTimeout': '5', 'OriginProtocolPolicy': 'https-only',
                  'OriginReadTimeout': '30', 'HTTPPort': '80'}
        origin = CustomOriginConfig(config)
        origin.http_port.should.equal("80")
        origin.https_port.should.equal("443")
        origin.origin_read_timeout.should.equal("30")
        origin.origin_keepalive_timeout.should.equal("5")

    def test_single_protocol(self):
        config = {'OriginSslProtocols': {'Items': {'SslProtocol': 'TLSv1.2'}}}
        origin = CustomOriginConfig(config)
        origin.origin_ssl_protocols.should.equal(["TLSv1.2"])

    def test_multiple_protocols(self):
        config = {'OriginSslProtocols': {'Items': {'SslProtocol': ['TLSv1.1', 'TLSv1.2']}}}
        origin = CustomOriginConfig(config)
        origin.origin_ssl_protocols.should.equal(["TLSv1.1", "TLSv1.2"])

    def test_empty_config(self):
        origin = CustomOriginConfig({})
        origin.http_port.should.equal("")
        origin.https_port.should.equal("")
        origin.origin_read_timeout.should.equal("")
        origin.origin_keepalive_timeout.should.equal("")
        origin.origin_ssl_protocols.should.equal([])


class TestMethods:

    def test_default(self):
        m = Methods(None)
        m.names.should.equal(["GET", "HEAD"])

    def test_single_method(self):
        config = {'Items': {'Method': ['GET', 'HEAD', 'POST']}}
        m = Methods(config)
        m.names.should.equal(["GET", "HEAD", "POST"])

    def test_multiple_methods(self):
        config = {'Items': {'Method': 'GET'}}
        m = Methods(config)
        m.names.should.equal(["GET"])

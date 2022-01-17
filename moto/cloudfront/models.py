import random
import string

from moto.core import ACCOUNT_ID, BaseBackend, BaseModel
from moto.utilities.tagging_service import TaggingService
from uuid import uuid4

from .exceptions import (
    OriginDoesNotExist,
    InvalidOriginServer,
    DomainNameNotAnS3Bucket,
    DistributionAlreadyExists,
    InvalidIfMatchVersion,
    NoSuchDistribution,
)


class ActiveTrustedSigners:
    def __init__(self):
        self.enabled = False
        self.quantity = 0
        self.signers = []


class ActiveTrustedKeyGroups:
    def __init__(self):
        self.enabled = False
        self.quantity = 0
        self.kg_key_pair_ids = []


class LambdaFunctionAssociation:
    def __init__(self):
        self.arn = ""
        self.event_type = ""
        self.include_body = False


class ForwardedValues:
    def __init__(self, config):
        cookies = config.get("Cookies", {})
        self.forward = cookies.get("Forward", "")
        self.query_string = str(config.get("QueryString", False)).lower() == "true"
        names = cookies.get("WhitelistedNames") or {}
        names = names.get("Items") or {}
        names = names.get("Name") or []
        if isinstance(names, list):
            self.whitelisted_names = names
        else:
            self.whitelisted_names = [names]  # Single name is represented as a string - manually convert back to a list
        self.headers = config.get("Headers", {}).get("Items") or []
        self.query_string_cache_keys = config.get("QueryStringCacheKeys", {}).get("Items") or []


class Methods:
    def __init__(self, config):
        if config and "Items" in config:
            items = config.get("Items") or {}
            self.names = items.get("Method") or []

            if not isinstance(self.names, list):
                self.names = [self.names]
        else:
            # Default methods
            self.names = ["GET", "HEAD"]


class DefaultCacheBehaviour:
    def __init__(self, config):
        self.target_origin_id = config.get("TargetOriginId")
        self.trusted_signers_enabled = False
        self.trusted_signers = ActiveTrustedSigners()
        self.trusted_key_groups_enabled = False
        self.trusted_key_groups = []
        self.viewer_protocol_policy = config.get("ViewerProtocolPolicy")
        allowed_method_config = config.get("AllowedMethods") or {}
        cached_config = allowed_method_config.get("CachedMethods") or {}
        self.allowed_methods = Methods(allowed_method_config)
        self.cached_methods = Methods(cached_config)
        self.smooth_streaming = config.get("SmoothStreaming", "True").lower() == "true"
        self.compress = config.get("Compress", "True").lower() == "true"
        self.lambda_function_associations = []
        self.function_associations = []
        self.field_level_encryption_id = ""
        self.forwarded_values = ForwardedValues(config.get("ForwardedValues", {}))
        self.min_ttl = config.get("MinTTL", 0)
        self.default_ttl = config.get("DefaultTTL", 0)
        self.max_ttl = config.get("MaxTTL", 0)


class Logging:
    def __init__(self):
        self.enabled = False
        self.include_cookies = False


class ViewerCertificate:
    def __init__(self):
        self.cloud_front_default_certificate = True
        self.min_protocol_version = "TLSv1"
        self.certificate_source = "cloudfront"


class CustomOriginConfig:
    def __init__(self, config):
        self.http_port = config.get("HTTPPort", "")
        self.https_port = config.get("HTTPSPort", "")
        self.origin_read_timeout = config.get("OriginReadTimeout", "")
        self.origin_keepalive_timeout = config.get("OriginKeepaliveTimeout", "")
        self.protocol_policy = config.get("OriginProtocolPolicy", "")

        protocols = config.get("OriginSslProtocols") or {}
        protocols = protocols.get("Items") or {}
        protocol_names = protocols.get("SslProtocol") or []
        if isinstance(protocol_names, list):
            self.origin_ssl_protocols = protocol_names
        else:
            self.origin_ssl_protocols = [protocol_names]


class Origin:
    def __init__(self, origin):
        self.id = origin["Id"]
        self.domain_name = origin["DomainName"]
        self.custom_headers = []
        self.s3_access_identity = ""
        self.origin_shield = None
        self.connection_attempts = 3
        self.connection_timeout = 10
        self.is_s3_config = "S3OriginConfig" in origin
        self.is_custom_config = "CustomOriginConfig" in origin
        self.custom_origin = CustomOriginConfig(origin.get("CustomOriginConfig") or {})

        if not self.is_s3_config and not self.is_custom_config:
            raise InvalidOriginServer

        if self.is_s3_config:
            # Very rough validation
            if not self.domain_name.endswith("amazonaws.com"):
                raise DomainNameNotAnS3Bucket
            self.s3_access_identity = origin["S3OriginConfig"]["OriginAccessIdentity"]


class DistributionConfig:
    def __init__(self, config):
        self.config = config
        self.default_cache_behavior = DefaultCacheBehaviour(
            config["DefaultCacheBehavior"]
        )
        self.cache_behaviors = []
        self.custom_error_responses = []
        self.logging = Logging()
        self.enabled = str(config.get("Enabled", "False")).lower() == "true"
        self.viewer_certificate = ViewerCertificate()

        restrictions = config.get("Restrictions") or {}
        geo = restrictions.get("GeoRestriction") or {}
        self.geo_restriction_type = geo.get("RestrictionType") or "none"
        locations = geo.get("Items") or {}
        self.geo_restrictions = locations.get("Location") or []

        self.caller_reference = config.get("CallerReference", str(uuid4()))
        self.origins = config["Origins"]["Items"]["Origin"]
        if not isinstance(self.origins, list):
            self.origins = [self.origins]

        # This check happens before any other Origins-validation
        if self.default_cache_behavior.target_origin_id not in [
            o["Id"] for o in self.origins
        ]:
            raise OriginDoesNotExist

        self.origins = [Origin(o) for o in self.origins]
        self.price_class = config.get("PriceClass", "PriceClass_All")
        self.http_version = config.get("HttpVersion", "http2")
        self.is_ipv6_enabled = config.get("IsIPV6Enabled", "true").lower() == "true"
        self.default_root_object = config.get("DefaultRootObject") or ""


class Distribution(BaseModel):
    @staticmethod
    def random_id(uppercase=True):
        ascii_set = string.ascii_uppercase if uppercase else string.ascii_lowercase
        chars = list(range(10)) + list(ascii_set)
        resource_id = random.choice(ascii_set) + "".join(
            str(random.choice(chars)) for _ in range(12)
        )
        return resource_id

    def __init__(self, config):
        self.distribution_id = Distribution.random_id()
        self.etag = Distribution.random_id()
        self.arn = (
            f"arn:aws:cloudfront:{ACCOUNT_ID}:distribution/{self.distribution_id}"
        )
        self.distribution_config = DistributionConfig(config)
        self.active_trusted_signers = ActiveTrustedSigners()
        self.active_trusted_key_groups = ActiveTrustedKeyGroups()
        self.aliases = []
        self.origin_groups = []
        self.alias_icp_recordals = []
        self.last_modified_time = "2021-11-27T10:34:26.802Z"
        self.in_progress_invalidation_batches = 0
        self.has_active_trusted_key_groups = False
        self.status = "InProgress"
        self.domain_name = f"{Distribution.random_id(uppercase=False)}.cloudfront.net"

    def advance(self):
        """
        Advance the status of this Distribution, to mimick AWS' behaviour
        """
        if self.status == "InProgress":
            self.status = "Deployed"

    @property
    def location(self):
        return f"https://cloudfront.amazonaws.com/2020-05-31/distribution/{self.distribution_id}"


class CloudFrontBackend(BaseBackend):
    def __init__(self):
        self.distributions = dict()
        self.tagger = TaggingService()

    def create_distribution(self, distribution_config):
        """
        This has been tested against an S3-distribution with the simplest possible configuration.
        Please raise an issue if we're not persisting/returning the correct attributes for your use-case.
        """
        dist = Distribution(distribution_config)
        caller_reference = dist.distribution_config.caller_reference
        existing_dist = self._distribution_with_caller_reference(caller_reference)
        if existing_dist:
            raise DistributionAlreadyExists(existing_dist.distribution_id)
        self.distributions[dist.distribution_id] = dist
        return dist, dist.location, dist.etag

    def create_distribution_with_tags(self, config):
        dist_config = config["DistributionConfig"]
        tag_config = config.get("Tags") or {}
        items = tag_config.get("Items") or {}
        tags = items.get("Tag") or []

        dist, location, etag = self.create_distribution(dist_config)
        self.tagger.tag_resource(dist.arn, tags)
        return dist, location, etag

    def get_distribution(self, distribution_id):
        if distribution_id not in self.distributions:
            raise NoSuchDistribution
        dist = self.distributions[distribution_id]
        dist.advance()
        return dist, dist.etag

    def update_distribution(self, dist_id, if_match, config):
        self.distributions[dist_id] = Distribution(config)
        dist = self.distributions[dist_id]
        return dist, dist.etag

    def delete_distribution(self, distribution_id, if_match):
        """
        The IfMatch-value is ignored - any value is considered valid.
        Calling this function without a value is invalid, per AWS' behaviour
        """
        if not if_match:
            raise InvalidIfMatchVersion
        if distribution_id not in self.distributions:
            raise NoSuchDistribution
        del self.distributions[distribution_id]

    def list_distributions(self):
        """
        Pagination is not supported yet.
        """
        for dist in self.distributions.values():
            dist.advance()
        return self.distributions.values()

    def list_tags_for_resource(self, resource):
        return self.tagger.get_tag_dict_for_resource(resource)

    def _distribution_with_caller_reference(self, reference):
        for dist in self.distributions.values():
            config = dist.distribution_config
            if config.caller_reference == reference:
                return dist
        return False


cloudfront_backend = CloudFrontBackend()

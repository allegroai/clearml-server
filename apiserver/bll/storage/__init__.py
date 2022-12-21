from copy import copy

from boltons.cacheutils import cachedproperty
from clearml.backend_config.bucket_config import (
    S3BucketConfigurations,
    AzureContainerConfigurations,
    GSBucketConfigurations,
)

from apiserver.config_repo import config


log = config.logger(__file__)


class StorageBLL:
    default_aws_configs: S3BucketConfigurations = None
    conf = config.get("services.storage_credentials")

    @cachedproperty
    def _default_aws_configs(self) -> S3BucketConfigurations:
        return S3BucketConfigurations.from_config(self.conf.get("aws.s3"))

    @cachedproperty
    def _default_azure_configs(self) -> AzureContainerConfigurations:
        return AzureContainerConfigurations.from_config(self.conf.get("azure.storage"))

    @cachedproperty
    def _default_gs_configs(self) -> GSBucketConfigurations:
        return GSBucketConfigurations.from_config(self.conf.get("google.storage"))

    def get_azure_settings_for_company(
        self,
        company_id: str,
    ) -> AzureContainerConfigurations:
        return copy(self._default_azure_configs)

    def get_gs_settings_for_company(
        self,
        company_id: str,
    ) -> GSBucketConfigurations:
        return copy(self._default_gs_configs)

    def get_aws_settings_for_company(
        self,
        company_id: str,
    ) -> S3BucketConfigurations:
        return copy(self._default_aws_configs)

from typing import Union

from aws_cdk.core import CfnParameter

from pcluster.config.cluster_config import SlurmClusterConfig, AwsBatchClusterConfig
from pcluster.constants import CW_LOGS_CFN_PARAM_NAME


class ClusterStackMixin:
    """
    Mixin for CDK properties that are common across multiple CDK Stacks
    """

    def add_common_parameters(self, config: Union[SlurmClusterConfig, AwsBatchClusterConfig]):
        if self.config.is_cw_logging_enabled:
            return CfnParameter(
                self,
                CW_LOGS_CFN_PARAM_NAME,
                description="CloudWatch Log Group associated to the cluster",
                default=self.log_group_name,
            )

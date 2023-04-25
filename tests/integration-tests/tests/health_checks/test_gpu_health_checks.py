import logging

import pytest

from remote_command_executor import RemoteCommandExecutor
from tests.common.assertions import assert_head_node_is_running, assert_lines_in_logs


@pytest.mark.usefixtures("instance", "os", "scheduler")
def test_cluster_with_gpu_health_checks(
    region, pcluster_config_reader, s3_bucket_factory, clusters_factory, test_datadir, scheduler_commands_factory
):
    """Test cluster creation fail with cluster creation timeout."""
    cluster_config = pcluster_config_reader()
    cluster = clusters_factory(cluster_config)
    assert_head_node_is_running(region, cluster)
    remote_command_executor = RemoteCommandExecutor(cluster)
    scheduler_commands = scheduler_commands_factory(remote_command_executor)
    job_id = scheduler_commands.submit_command_and_assert_job_accepted(
        submit_command_args={"command": "sleep 1", "host": "queue-0-dy-compute-resource-1-1"}
    )
    scheduler_commands.wait_job_completed(job_id)
    logging.info("Verifying error in logs")
    # assert_lines_in_logs(
    #     remote_command_executor,
    #     ["/var/log/chef-client.log"],
    #     [r"Error executing action `mount` on resource 'manage_ebs\[add ebs\]'"],
    # )

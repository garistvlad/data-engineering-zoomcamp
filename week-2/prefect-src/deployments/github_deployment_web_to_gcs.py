from pathlib import Path
import sys
from typing import Coroutine, Union

from prefect.deployments import Deployment
from prefect.filesystems import GitHub

PREFECT_SRC_DIR = Path(__file__).parent.absolute().parent
sys.path.append(str(PREFECT_SRC_DIR))

from flows.etl_web_to_gcs import etl_web_to_gcs

GITHUB_BLOCK_NAME = "github-de-zoomcamp"


def create_github_deployment(github_block_name: str) -> Union[Deployment, Coroutine]:
    """Create GitHub deployment"""
    github_block = GitHub.load(github_block_name)
    github_deployment = Deployment.build_from_flow(
        flow=etl_web_to_gcs,
        name="github-web-to-gcs",
        work_queue_name="github",
        storage=github_block,
        entrypoint=f"week-2/prefect-src/flows/etl_web_to_gcs.py:etl_web_to_gcs",
    )
    return github_deployment


def main():
    github_deployment = create_github_deployment(GITHUB_BLOCK_NAME)
    github_deployment.apply()


if __name__ == "__main__":
    main()

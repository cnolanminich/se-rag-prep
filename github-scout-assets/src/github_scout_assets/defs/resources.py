"""Shared resources for all components."""

import dagster as dg
from github_scout_assets.resources.github_resource import GitHubResource
from github_scout_assets.resources.notion_resource import NotionResource
from github_scout_assets.resources.scoutos_resource import ScoutosResource


defs = dg.Definitions(
    resources={
        "github": GitHubResource(github_token=dg.EnvVar("GITHUB_TOKEN")),
        "notion": NotionResource(notion_token=dg.EnvVar("NOTION_TOKEN")),
        "scoutos": ScoutosResource(api_key=dg.EnvVar("SCOUTOS_API_KEY")),
    }
)
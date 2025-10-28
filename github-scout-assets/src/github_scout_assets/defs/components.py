"""Component configurations for GitHub and Notion Scout integration."""

from github_scout_assets.components.github_repositories import GitHubRepositoriesComponent, GitHubRepoConfig, GitHubGistConfig
# from github_scout_assets.components.notion_content import NotionContentComponent, NotionPageConfig, NotionDatabaseConfig


# GitHub repositories component configuration
# github_repos_component = GitHubRepositoriesComponent(
#     repositories=[
#         # Official Dagster Examples
#         GitHubRepoConfig(owner="dagster-io", repo="hooli-data-eng-pipelines", category="official", description="Official Dagster demo data engineering pipelines"),
#         GitHubRepoConfig(owner="dagster-io", repo="knowledge-repo", category="official", description="Official Dagster knowledge repository"),
#         GitHubRepoConfig(owner="dagster-io", repo="dagster-open-platform", category="official", description="Dagster open platform components and examples"),
#         GitHubRepoConfig(owner="dagster-io", repo="awesome-dagster", category="official", description="Curated list of awesome Dagster resources"),
#         GitHubRepoConfig(owner="dagster-io", repo="community-integrations", category="community", description="Community-contributed Dagster integrations"),
#         
#         # User Examples - cnolanminich
#         GitHubRepoConfig(owner="cnolanminich", repo="azure_demo_20241003", category="user_examples", description="Azure integration demo"),
#         GitHubRepoConfig(owner="cnolanminich", repo="30_days_of_dagster", category="user_examples", description="30 days of Dagster learning examples"),
#         GitHubRepoConfig(owner="cnolanminich", repo="dagster-and-r", category="user_examples", description="Dagster integration with R programming"),
#         GitHubRepoConfig(owner="cnolanminich", repo="dbt-multiple-code-locations", category="user_examples", description="Multiple dbt code locations example"),
#         GitHubRepoConfig(owner="cnolanminich", repo="dagster-airbyte-dbt-demo", category="user_examples", description="Airbyte and dbt integration demo"),
#         GitHubRepoConfig(owner="cnolanminich", repo="pipes-non-pipes-demo", category="user_examples", description="Pipes vs non-pipes comparison demo"),
#         
#         # User Examples - slopp
#         GitHubRepoConfig(owner="slopp", repo="freshness_guide", category="user_examples", description="Data freshness monitoring guide"),
#         GitHubRepoConfig(owner="slopp", repo="dagster_kafka_demo", category="user_examples", description="Kafka integration with Dagster"),
#         GitHubRepoConfig(owner="slopp", repo="snowreport", category="user_examples", description="Snow report data pipeline example"),
#         GitHubRepoConfig(owner="slopp", repo="dagteam", category="user_examples", description="Team collaboration patterns"),
#         GitHubRepoConfig(owner="slopp", repo="dagster-dynamic-partitions", category="user_examples", description="Dynamic partitions example"),
#     ],
#     gists=[
#         GitHubGistConfig(id="74f96fbfaf9e2a72f37c536ba21d597a", name="GitHub Action - Release Based Deployment", description="GitHub Actions workflow for release-based deployment"),
#         GitHubGistConfig(id="07b00b3402c7e22589ee03b39063d8a9", name="OSS Dagster - Standard Credit Query", description="SQL query for standard credit usage in OSS Dagster"),
#         GitHubGistConfig(id="23739d22f310eb71d8a24ab9e639febd", name="OSS Dagster - Concurrency Model", description="Concurrency model analysis for OSS Dagster"),
#         GitHubGistConfig(id="0376d9653f1a8b0a8dfe7f4120055bd9", name="OSS Dagster - High Velocity Credits", description="High velocity credits usage analysis"),
#         GitHubGistConfig(id="b3cdc8fec9f9442069efbf86cfc30204", name="Dagster and SageMaker", description="Integration example with AWS SageMaker"),
#         GitHubGistConfig(id="554dc45ae164b36e6a0cfa08906a9af1", name="Run Status Sensor - Wait for Two Jobs", description="Run status sensor waiting for multiple jobs"),
#         GitHubGistConfig(id="ab981bae677a75bcad15c352c3d42507", name="Automation Conditions - Update Once Per Period", description="Automation conditions for periodic updates"),
#         GitHubGistConfig(id="8dd440c754f133113f2f668fbe92c6e9", name="Dagster - Controlling Parallelism within a Run", description="Techniques for controlling parallelism in Dagster"),
#         GitHubGistConfig(id="bb1d8fd7738dbc6c4f1c320486a378fb", name="Dagster+ High Velocity and Standard Credits Query", description="Credits usage queries for Dagster+"),
#     ],
#     collection_name="SEbot Dagster Github Repos and Gists",
#     collection_description="GitHub repositories, gists, and code files from Dagster community resources",
#     table_name="GitHub Repositories and Files",
# )


# # Notion content component configuration  
# # Note: Update these with your actual Notion page/database IDs
# notion_content_component = NotionContentComponent(
#     pages=[
#         NotionPageConfig(
#             id="your_page_id_here",
#             name="Example Page", 
#             description="Example Notion page",
#         ),
#         # Add more pages here
#     ],
#     databases=[
#         NotionDatabaseConfig(
#             id="your_database_id_here",
#             name="Example Database",
#             description="Example Notion database",
#         ),
#         # Add more databases here
#     ],
#     include_search=True,  # Include automatic page discovery
#     collection_name="Notion Content",
#     collection_description="Notion pages and database entries for knowledge management",
#     table_name="Notion Pages and Database Entries",
# )
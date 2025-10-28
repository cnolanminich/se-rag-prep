import os
from typing import Sequence, Dict, Any, List

import dagster as dg
from pydantic import BaseModel, Field

from github_scout_assets.resources.github_resource import GitHubResource
from github_scout_assets.resources.scoutos_resource import ScoutosResource


class GitHubRepoConfig(dg.Model):
    """Configuration for a GitHub repository."""
    
    owner: str
    repo: str
    category: str = "user_examples"
    description: str = ""


class GitHubGistConfig(dg.Model):
    """Configuration for a GitHub gist."""
    
    id: str
    name: str
    description: str = ""


class GitHubRepositoriesComponent(dg.Component, dg.Model, dg.Resolvable):
    """Component for processing multiple GitHub repositories and gists into Scout."""

    repositories: List[GitHubRepoConfig] = []
    gists: List[GitHubGistConfig] = []
    collection_name: str = "SEbot Dagster Github Repos and Gists"
    collection_description: str = "GitHub repositories, gists, and code files from Dagster community"
    table_name: str = "GitHub Repositories and Files"
    force_recreate_table: bool = False
    

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        """Build Dagster definitions for GitHub repositories processing."""
        
        # Create setup assets
        @dg.asset(
            name="github_scout_collection",
            group_name="github_scout_setup",
            kinds={"scout", "setup"},
            owners=["team:data"],
        )
        def scout_collection(
            context: dg.AssetExecutionContext,
            scoutos: ScoutosResource,
        ) -> dg.MaterializeResult:
            """Create or verify the GitHub collection in Scout."""
            
            # Check if collection already exists
            try:
                collections = scoutos.get_collections()
                existing_collection = None
                
                for collection in collections:
                    if collection.get("name") == self.collection_name:
                        existing_collection = collection
                        break
                
                if existing_collection:
                    collection_id = existing_collection.get("id")
                    context.log.info(f"Using existing collection: {self.collection_name} (ID: {collection_id})")
                    return dg.MaterializeResult(metadata={"collection_id": collection_id, "status": "existing"})
                else:
                    # Create new collection
                    result = scoutos.create_collection(
                        name=self.collection_name,
                        description=self.collection_description
                    )
                    collection_id = result.get("id")
                    context.log.info(f"Created new collection: {self.collection_name} (ID: {collection_id})")
                    return dg.MaterializeResult(metadata={"collection_id": collection_id, "status": "created"})
                    
            except Exception as e:
                context.log.error(f"Error managing GitHub collection: {e}")
                raise

        @dg.asset(
            name="github_scout_table",
            group_name="github_scout_setup",
            kinds={"scout", "setup"}, 
            owners=["team:data"],
            deps=[scout_collection],
        )
        def scout_table(
            context: dg.AssetExecutionContext,
            scoutos: ScoutosResource,
        ) -> dg.MaterializeResult:
            """Create or verify the GitHub table in Scout."""
            
            collection_id = os.getenv("SCOUTOS_COLLECTION_ID")
            if not collection_id:
                raise ValueError("SCOUTOS_COLLECTION_ID environment variable is required")
            
            # GitHub table schema - Scout API column types
            schema = [
                {"name": "_key", "column_type": "text-short"},
                {"name": "type", "column_type": "text-short"},
                {"name": "document_type", "column_type": "text-short"},
                {"name": "title", "column_type": "text-short"},
                {"name": "description", "column_type": "text-long"},
                {"name": "owner", "column_type": "text-short"},
                {"name": "repo", "column_type": "text-short"},
                {"name": "category", "column_type": "text-short"},
                {"name": "source", "column_type": "text-short"},
                {"name": "url", "column_type": "url"},
                {"name": "created_at", "column_type": "datetime"},
                {"name": "updated_at", "column_type": "datetime"},
                {"name": "content", "column_type": "text-long"},
            ]
            
            try:
                context.log.info(f"force_recreate_table setting: {self.force_recreate_table}")
                tables = scoutos.get_tables(collection_id)
                existing_table = None
                
                for table in tables:
                    if table.get("name") == self.table_name:
                        existing_table = table
                        break
                
                context.log.info(f"Found existing table: {existing_table is not None}")
                if existing_table and not self.force_recreate_table:
                    table_id = existing_table.get("id")
                    context.log.info(f"Using existing table: {self.table_name} (ID: {table_id})")
                    return dg.MaterializeResult(metadata={"table_id": table_id, "status": "existing"})
                elif existing_table and self.force_recreate_table:
                    # Delete existing table and create new one
                    old_table_id = existing_table.get("id")
                    context.log.info(f"Deleting existing table: {self.table_name} (ID: {old_table_id}) for recreation")
                    scoutos.delete_table(collection_id, old_table_id)
                    
                    result = scoutos.create_table(collection_id=collection_id, name=self.table_name, schema=schema)
                    table_id = result.get("data", {}).get("table_id")
                    
                    # Verify the table schema was created correctly
                    table_schema = scoutos.get_table_schema(collection_id, table_id)
                    actual_schema = table_schema.get("data", {}).get("table_config", {}).get("schema", [])
                    context.log.info(f"Created table schema has {len(actual_schema)} columns")
                    for col in actual_schema:
                        col_name = col.get("column_id", "unknown")
                        col_type = col.get("column_type", "unknown")
                        context.log.info(f"Column: {col_name} (type: {col_type})")
                    
                    context.log.info(f"Recreated table: {self.table_name} (ID: {table_id})")
                    return dg.MaterializeResult(metadata={"table_id": table_id, "status": "recreated"})
                else:
                    result = scoutos.create_table(collection_id=collection_id, name=self.table_name, schema=schema)
                    table_id = result.get("data", {}).get("table_id")
                    context.log.info(f"Created new table: {self.table_name} (ID: {table_id})")
                    return dg.MaterializeResult(metadata={"table_id": table_id, "status": "created"})
                    
            except Exception as e:
                context.log.error(f"Error managing GitHub table: {e}")
                raise

        # Single asset to load all GitHub data
        @dg.asset(
            name="scout_github_table_fill",
            group_name="github_repositories", 
            kinds={"github", "scout"},
            owners=["team:data"],
            deps=[scout_table],
        )
        def github_table_fill(
            context: dg.AssetExecutionContext,
            github: GitHubResource,
            scoutos: ScoutosResource,
        ) -> dg.MaterializeResult:
            """Load all GitHub repositories and gists into Scout."""
            
            # Get Scout IDs from upstream asset metadata
            instance = context.instance
            
            # Get table ID from upstream scout_table asset  
            table_event = instance.get_latest_materialization_event(dg.AssetKey(["github_scout_table"]))
            context.log.info(f"table_event: {table_event}")
            if table_event is None:
                context.log.error("No materialization found for github_scout_table asset")
                return dg.MaterializeResult(metadata={"error": "missing_table_materialization"})
            
            context.log.info(f"table_event metadata: {table_event.asset_materialization.metadata}")
            table_id_metadata = table_event.asset_materialization.metadata.get("table_id")
            context.log.info(f"table_id metadata object: {table_id_metadata}")
            
            if table_id_metadata is None:
                context.log.error("table_id not found in metadata")
                return dg.MaterializeResult(metadata={"error": "table_id_missing_from_metadata"})
                
            table_id = table_id_metadata.value
            context.log.info(f"extracted table_id: {table_id}")

            if not table_id:
                context.log.warning("Scout table ID not set, skipping write")
                return dg.MaterializeResult(
                    metadata={
                        "documents_processed": 0,
                        "scout_write": "skipped - missing env vars",
                    }
                )
            
            all_documents = []
            repos_processed = 0
            gists_processed = 0
            context.log.info(f"repo list: {[repo for repo in self.repositories]}")
            context.log.info(f"Starting to process {len(self.repositories)} repositories and {len(self.gists)} gists")
            # Process all repositories
            for repo_config in self.repositories:
                context.log.info(f"Processing repository: {repo_config.owner}/{repo_config.repo}")
                try:
                    documents = github.get_repository_content(repo_config.owner, repo_config.repo)
                    if documents:
                        # Add metadata to documents
                        for doc in documents:
                            doc["category"] = repo_config.category
                            doc["source"] = "github_component"
                        all_documents.extend(documents)
                        repos_processed += 1
                        context.log.info(f"Added {len(documents)} documents from {repo_config.owner}/{repo_config.repo}")
                except Exception as e:
                    context.log.error(f"Failed to process {repo_config.owner}/{repo_config.repo}: {e}")
            
            # Process all gists
            for gist_config in self.gists:
                context.log.info(f"Processing gist: {gist_config.name} ({gist_config.id})")
                
                # Skip placeholder IDs
                if gist_config.id in ["your_gist_id_here", "gist_id_placeholder"]:
                    context.log.warning(f"Skipping placeholder gist ID: {gist_config.id}")
                    continue
                
                try:
                    documents = github.get_gist_content(gist_id=gist_config.id, gist_name=gist_config.name)
                    if documents:
                        # Add metadata to documents
                        for doc in documents:
                            doc["category"] = "gist"
                            doc["source"] = "github_component"
                        all_documents.extend(documents)
                        gists_processed += 1
                        context.log.info(f"Added {len(documents)} documents from gist {gist_config.name}")
                except Exception as e:
                    context.log.error(f"Failed to process gist {gist_config.id}: {e}")
            collection_id = os.getenv("SCOUTOS_COLLECTION_ID")
            # Write all documents to Scout
            if all_documents:
                try:
                    # Log a sample document structure for debugging
                    if all_documents:
                        sample_doc = all_documents[0]
                        context.log.info(f"Sample document structure: {list(sample_doc.keys())}")
                        context.log.info(f"Sample title (cmfeg8drs00wz0fs60q668chq): {sample_doc.get('cmfeg8drs00wz0fs60q668chq', 'NOT_FOUND')}")
                        context.log.info(f"Sample description (cmfeg8drs00x00fs6b44bexkp): {sample_doc.get('cmfeg8drs00x00fs6b44bexkp', 'NOT_FOUND')[:100]}...")
                    
                    scoutos.write_documents(collection_id, table_id, all_documents)
                    context.log.info(f"Successfully wrote all documents to Scout collection: {self.collection_name} (ID: {collection_id}), table: {self.table_name} (ID: {table_id})")
                    
                    return dg.MaterializeResult(
                        metadata={
                            "total_documents": len(all_documents),
                            "repositories_processed": repos_processed,
                            "gists_processed": gists_processed,
                            "total_repos": len(self.repositories),
                            "total_gists": len(self.gists),
                            "scout_write": "success",
                        }
                    )
                except Exception as e:
                    context.log.error(f"Failed to write documents to Scout: {e}")
                    return dg.MaterializeResult(
                        metadata={
                            "total_documents": len(all_documents),
                            "repositories_processed": repos_processed,
                            "gists_processed": gists_processed,
                            "scout_write": "failed",
                            "error": str(e),
                        }
                    )
            else:
                context.log.warning("No documents to write to Scout")
                return dg.MaterializeResult(
                    metadata={
                        "total_documents": 0,
                        "repositories_processed": repos_processed,
                        "gists_processed": gists_processed,
                        "scout_write": "skipped - no documents",
                    }
                )

        # Create summary asset
        @dg.asset(
            name="github_repositories_summary",
            group_name="github_repositories",
            kinds={"github", "scout", "summary"},
            owners=["team:data"],
            deps=[github_table_fill],
        )
        def repositories_summary(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
            """Summary of all GitHub repository processing. Checking component"""
            
            total_repos = len(self.repositories)
            total_gists = len(self.gists)
            categories = set(repo.category for repo in self.repositories)
            
            context.log.info(f"Processed {total_repos} repositories and {total_gists} gists across {len(categories)} categories")
            
            return dg.MaterializeResult(
                metadata={
                    "total_repositories": total_repos,
                    "total_gists": total_gists,
                    "categories": list(categories),
                    "repositories": [f"{repo.owner}/{repo.repo}" for repo in self.repositories],
                    "gists": [f"{gist.name} ({gist.id})" for gist in self.gists],
                }
            )

        return dg.Definitions(
            assets=[scout_collection, scout_table, github_table_fill, repositories_summary],
        )
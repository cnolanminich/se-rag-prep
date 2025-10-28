import os
from typing import Sequence, Dict, Any

import dagster as dg
from pydantic import BaseModel

from github_scout_assets.resources.notion_resource import NotionResource
from github_scout_assets.resources.scoutos_resource import ScoutosResource


class NotionPageConfig(BaseModel):
    """Configuration for a Notion page."""
    
    id: str
    name: str
    description: str = ""


class NotionDatabaseConfig(BaseModel):
    """Configuration for a Notion database."""
    
    id: str
    name: str
    description: str = ""


class NotionContentComponent(dg.Component, dg.Model, dg.Resolvable):
    """Component for processing Notion pages and databases into Scout."""

    pages: Sequence[NotionPageConfig] = []
    databases: Sequence[NotionDatabaseConfig] = []
    include_search: bool = True
    collection_name: str = "Notion Content"
    collection_description: str = "Notion pages and database entries for knowledge management"
    table_name: str = "Notion Pages and Database Entries"

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        """Build Dagster definitions for Notion content processing."""
        
        # Create setup assets
        @dg.asset(
            name="notion_scout_collection",
            group_name="notion_scout_setup",
            kinds={"scout", "setup"},
            owners=["team:data"],
        )
        def scout_collection(
            context: dg.AssetExecutionContext,
            scoutos: ScoutosResource,
        ) -> dg.MaterializeResult:
            """Create or verify the Notion collection in Scout."""
            
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
                    result = scoutos.create_collection(name=self.collection_name, description=self.collection_description)
                    collection_id = result.get("id")
                    context.log.info(f"Created new collection: {self.collection_name} (ID: {collection_id})")
                    return dg.MaterializeResult(metadata={"collection_id": collection_id, "status": "created"})
                    
            except Exception as e:
                context.log.error(f"Error managing Notion collection: {e}")
                raise

        @dg.asset(
            name="notion_scout_table",
            group_name="notion_scout_setup",
            kinds={"scout", "setup"},
            owners=["team:data"],
            deps=[scout_collection],
        )
        def scout_table(
            context: dg.AssetExecutionContext,
            scoutos: ScoutosResource,
        ) -> dg.MaterializeResult:
            """Create or verify the Notion table in Scout."""
            
            collection_id = os.getenv("SCOUTOS_NOTION_COLLECTION_ID")
            if not collection_id:
                raise ValueError("SCOUTOS_NOTION_COLLECTION_ID environment variable is required")
            
            # Notion table schema
            schema = {
                "type": "object",
                "properties": {
                    "_key": {"type": "string", "description": "Unique identifier"},
                    "type": {"type": "string", "description": "Document type"},
                    "content": {"type": "string", "description": "Full markdown content"},
                    "document_type": {"type": "string", "description": "notion_page or notion_database_page"},
                    "title": {"type": "string", "description": "Page or entry title"},
                    "source": {"type": "string", "description": "Source type"},
                    "page_id": {"type": "string", "description": "Notion page ID"},
                    "url": {"type": "string", "description": "Notion page URL"},
                    "database_id": {"type": "string", "description": "Database ID (optional)"},
                    "database_title": {"type": "string", "description": "Database title (optional)"},
                    "created_at": {"type": "string", "description": "Creation timestamp"},
                    "updated_at": {"type": "string", "description": "Update timestamp"},
                },
                "required": ["_key", "type", "content", "document_type", "title", "source"]
            }
            
            try:
                tables = scoutos.get_tables(collection_id)
                existing_table = None
                
                for table in tables:
                    if table.get("name") == self.table_name:
                        existing_table = table
                        break
                
                if existing_table:
                    table_id = existing_table.get("id")
                    context.log.info(f"Using existing table: {self.table_name} (ID: {table_id})")
                    return dg.MaterializeResult(metadata={"table_id": table_id, "status": "existing"})
                else:
                    result = scoutos.create_table(collection_id=collection_id, name=self.table_name, schema=schema)
                    table_id = result.get("id")
                    context.log.info(f"Created new table: {self.table_name} (ID: {table_id})")
                    return dg.MaterializeResult(metadata={"table_id": table_id, "status": "created"})
                    
            except Exception as e:
                context.log.error(f"Error managing Notion table: {e}")
                raise

        # Create database assets
        database_assets = []
        for db_config in self.databases:
            asset_name = f"notion_database_{db_config.name.lower().replace(' ', '_').replace('-', '_')}"
            
            @dg.asset(
                name=asset_name,
                group_name="notion_databases",
                kinds={"notion", "scout"},
                owners=["team:data"],
                metadata={"database_id": db_config.id, "database_name": db_config.name},
                deps=[scout_table],
            )
            def database_asset(
                context: dg.AssetExecutionContext,
                notion: NotionResource,
                scoutos: ScoutosResource,
                _db_config=db_config,  # Capture config in closure
            ) -> dg.MaterializeResult:
                """Process Notion database and load into Scout."""
                
                context.log.info(f"Processing database: {_db_config.name} ({_db_config.id})")
                
                # Skip placeholder IDs
                if _db_config.id in ["your_database_id_here", "your_actual_database_id"]:
                    context.log.warning(f"Skipping placeholder database ID: {_db_config.id}")
                    return dg.MaterializeResult(metadata={"documents_processed": 0, "scout_write": "skipped"})
                
                documents = notion.get_database_entries(database_id=_db_config.id)
                
                if documents:
                    collection_id = os.getenv("SCOUTOS_NOTION_COLLECTION_ID", "")
                    table_id = os.getenv("SCOUTOS_NOTION_TABLE_ID", "")
                    
                    if not collection_id or not table_id:
                        context.log.warning("Notion Scout IDs not set, skipping write")
                        return dg.MaterializeResult(metadata={"documents_processed": len(documents), "scout_write": "skipped"})
                    
                    for doc in documents:
                        doc["source"] = "notion_component_database"
                    
                    try:
                        result = scoutos.write_documents(collection_id, table_id, documents)
                        context.log.info(f"Successfully wrote database to Scout: {_db_config.name}")
                        return dg.MaterializeResult(metadata={"documents_processed": len(documents), "scout_write": "success"})
                    except Exception as e:
                        context.log.error(f"Failed to write to Scout: {e}")
                        return dg.MaterializeResult(metadata={"documents_processed": len(documents), "scout_write": "failed", "error": str(e)})
                else:
                    return dg.MaterializeResult(metadata={"documents_processed": 0, "scout_write": "skipped"})
            
            database_assets.append(database_asset)

        # Create page assets
        page_assets = []
        for page_config in self.pages:
            asset_name = f"notion_page_{page_config.name.lower().replace(' ', '_').replace('-', '_')}"
            
            @dg.asset(
                name=asset_name,
                group_name="notion_pages",
                kinds={"notion", "scout"},
                owners=["team:data"],
                metadata={"page_id": page_config.id, "page_name": page_config.name},
                deps=[scout_table],
            )
            def page_asset(
                context: dg.AssetExecutionContext,
                notion: NotionResource,
                scoutos: ScoutosResource,
                _page_config=page_config,  # Capture config in closure
            ) -> dg.MaterializeResult:
                """Process Notion page and load into Scout."""
                
                context.log.info(f"Processing page: {_page_config.name} ({_page_config.id})")
                
                # Skip placeholder IDs
                if _page_config.id in ["your_page_id_here", "your_actual_page_id"]:
                    context.log.warning(f"Skipping placeholder page ID: {_page_config.id}")
                    return dg.MaterializeResult(metadata={"documents_processed": 0, "scout_write": "skipped"})
                
                document = notion.get_page_document(page_id=_page_config.id)
                
                if document:
                    collection_id = os.getenv("SCOUTOS_NOTION_COLLECTION_ID", "")
                    table_id = os.getenv("SCOUTOS_NOTION_TABLE_ID", "")
                    
                    if not collection_id or not table_id:
                        context.log.warning("Notion Scout IDs not set, skipping write")
                        return dg.MaterializeResult(metadata={"documents_processed": 1, "scout_write": "skipped"})
                    
                    document["source"] = "notion_component_page"
                    
                    try:
                        result = scoutos.write_documents(collection_id, table_id, [document])
                        context.log.info(f"Successfully wrote page to Scout: {_page_config.name}")
                        return dg.MaterializeResult(metadata={"documents_processed": 1, "scout_write": "success"})
                    except Exception as e:
                        context.log.error(f"Failed to write to Scout: {e}")
                        return dg.MaterializeResult(metadata={"documents_processed": 1, "scout_write": "failed", "error": str(e)})
                else:
                    return dg.MaterializeResult(metadata={"documents_processed": 0, "scout_write": "skipped"})
            
            page_assets.append(page_asset)

        # Create search asset if enabled
        search_assets = []
        if self.include_search:
            @dg.asset(
                name="notion_search_pages",
                group_name="notion_content",
                kinds={"notion", "scout"},
                owners=["team:data"],
                deps=[scout_table],
            )
            def search_pages(
                context: dg.AssetExecutionContext,
                notion: NotionResource,
                scoutos: ScoutosResource,
            ) -> dg.MaterializeResult:
                """Search all accessible Notion pages and load into Scout."""
                
                context.log.info("Searching all accessible Notion pages...")
                documents = notion.search_pages(query="")
                
                if documents:
                    collection_id = os.getenv("SCOUTOS_NOTION_COLLECTION_ID", "")
                    table_id = os.getenv("SCOUTOS_NOTION_TABLE_ID", "")
                    
                    if not collection_id or not table_id:
                        context.log.warning("Notion Scout IDs not set, skipping write")
                        return dg.MaterializeResult(metadata={"documents_processed": len(documents), "scout_write": "skipped"})
                    
                    for doc in documents:
                        doc["source"] = "notion_component_search"
                    
                    try:
                        result = scoutos.write_documents(collection_id, table_id, documents)
                        context.log.info(f"Successfully wrote {len(documents)} search pages to Scout")
                        return dg.MaterializeResult(metadata={"documents_processed": len(documents), "scout_write": "success"})
                    except Exception as e:
                        context.log.error(f"Failed to write to Scout: {e}")
                        return dg.MaterializeResult(metadata={"documents_processed": len(documents), "scout_write": "failed", "error": str(e)})
                else:
                    return dg.MaterializeResult(metadata={"documents_processed": 0, "scout_write": "skipped"})
            
            search_assets.append(search_pages)

        # Create summary asset
        @dg.asset(
            name="notion_content_summary",
            group_name="notion_content",
            kinds={"notion", "scout", "summary"},
            owners=["team:data"],
            deps=[*database_assets, *page_assets, *search_assets],
        )
        def content_summary(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
            """Summary of all Notion content processing."""
            
            total_databases = len(self.databases)
            total_pages = len(self.pages)
            
            context.log.info(f"Processed {total_databases} databases, {total_pages} pages, search enabled: {self.include_search}")
            
            return dg.MaterializeResult(
                metadata={
                    "total_databases": total_databases,
                    "total_pages": total_pages,
                    "include_search": self.include_search,
                    "database_names": [db.name for db in self.databases],
                    "page_names": [page.name for page in self.pages],
                }
            )

        all_assets = [scout_collection, scout_table, *database_assets, *page_assets, *search_assets, content_summary]

        return dg.Definitions(
            assets=all_assets,
        )
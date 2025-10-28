import time
from typing import List, Dict, Any, Optional

from dagster import ConfigurableResource, get_dagster_logger
from notion_client import Client


class NotionResource(ConfigurableResource):
    """Resource for fetching Notion pages and databases and flattening content."""

    notion_token: str

    def get_client(self) -> Client:
        """Get Notion client."""
        return Client(auth=self.notion_token)

    def get_page_content(self, page_id: str) -> Optional[str]:
        """Get page content and convert to markdown."""
        try:
            client = self.get_client()
            
            # Get page metadata
            page = client.pages.retrieve(page_id=page_id)
            
            # Get page blocks (content)
            blocks = client.blocks.children.list(block_id=page_id)
            
            return self._convert_page_to_markdown(page, blocks)
            
        except Exception as e:
            get_dagster_logger().error(f"Error fetching page {page_id}: {e}")
            return None

    def get_database_entries(self, database_id: str) -> List[Dict[str, Any]]:
        """Get all entries from a database and convert to markdown documents."""
        documents = []
        
        try:
            client = self.get_client()
            
            # Get database metadata
            database = client.databases.retrieve(database_id=database_id)
            database_title = self._extract_title_from_rich_text(database.get("title", []))
            
            get_dagster_logger().info(f"Processing database: {database_title}")
            
            # Query all pages in database
            has_more = True
            start_cursor = None
            
            while has_more:
                query_params = {"database_id": database_id}
                if start_cursor:
                    query_params["start_cursor"] = start_cursor
                
                response = client.databases.query(**query_params)
                
                for page in response["results"]:
                    page_id = page["id"]
                    
                    # Get page content
                    page_content = self._get_database_page_content(page, database)
                    
                    if page_content:
                        doc = {
                            "_key": f"notion_db_page_{database_id}_{page_id}",
                            "type": "database_page",
                            "content": page_content,
                            "document_type": "notion_database_page",
                            "title": self._extract_page_title(page),
                            "database_title": database_title,
                            "database_id": database_id,
                            "page_id": page_id,
                            "url": page.get("url", ""),
                            "created_at": page.get("created_time", ""),
                            "updated_at": page.get("last_edited_time", ""),
                        }
                        documents.append(doc)
                
                has_more = response.get("has_more", False)
                start_cursor = response.get("next_cursor")
            
            get_dagster_logger().info(f"Processed {len(documents)} pages from database {database_title}")
            
        except Exception as e:
            get_dagster_logger().error(f"Error fetching database {database_id}: {e}")
        
        return documents

    def get_page_document(self, page_id: str) -> Optional[Dict[str, Any]]:
        """Get a single page as a Scout document."""
        try:
            client = self.get_client()
            
            # Get page metadata
            page = client.pages.retrieve(page_id=page_id)
            page_title = self._extract_page_title(page)
            
            # Get page content
            page_content = self.get_page_content(page_id)
            
            if page_content:
                return {
                    "_key": f"notion_page_{page_id}",
                    "type": "page",
                    "content": page_content,
                    "document_type": "notion_page",
                    "title": page_title,
                    "page_id": page_id,
                    "url": page.get("url", ""),
                    "created_at": page.get("created_time", ""),
                    "updated_at": page.get("last_edited_time", ""),
                }
            
        except Exception as e:
            get_dagster_logger().error(f"Error processing page {page_id}: {e}")
        
        return None

    def _get_database_page_content(self, page: Dict[str, Any], database: Dict[str, Any]) -> str:
        """Get content for a database page."""
        try:
            page_id = page["id"]
            page_title = self._extract_page_title(page)
            database_title = self._extract_title_from_rich_text(database.get("title", []))
            
            # Start with metadata
            content = [
                f"# {page_title}",
                "",
                f"**Database:** {database_title}",
                f"**Created:** {page.get('created_time', '')}",
                f"**Updated:** {page.get('last_edited_time', '')}",
                "",
            ]
            
            # Add properties
            properties = page.get("properties", {})
            if properties:
                content.append("## Properties")
                content.append("")
                
                for prop_name, prop_data in properties.items():
                    prop_value = self._extract_property_value(prop_data)
                    if prop_value:
                        content.append(f"**{prop_name}:** {prop_value}")
                
                content.append("")
            
            # Get page blocks (content)
            try:
                client = self.get_client()
                blocks = client.blocks.children.list(block_id=page_id)
                
                if blocks.get("results"):
                    content.append("## Content")
                    content.append("")
                    content.extend(self._convert_blocks_to_markdown(blocks.get("results", [])))
                
            except Exception as e:
                get_dagster_logger().warning(f"Could not fetch blocks for page {page_id}: {e}")
            
            return "\n".join(content)
            
        except Exception as e:
            get_dagster_logger().error(f"Error converting database page to markdown: {e}")
            return ""

    def _convert_page_to_markdown(self, page: Dict[str, Any], blocks: Dict[str, Any]) -> str:
        """Convert a Notion page to markdown format."""
        try:
            page_title = self._extract_page_title(page)
            
            content = [
                f"# {page_title}",
                "",
                f"**Created:** {page.get('created_time', '')}",
                f"**Updated:** {page.get('last_edited_time', '')}",
                "",
            ]
            
            # Convert blocks to markdown
            if blocks.get("results"):
                content.extend(self._convert_blocks_to_markdown(blocks.get("results", [])))
            
            return "\n".join(content)
            
        except Exception as e:
            get_dagster_logger().error(f"Error converting page to markdown: {e}")
            return ""

    def _convert_blocks_to_markdown(self, blocks: List[Dict[str, Any]]) -> List[str]:
        """Convert Notion blocks to markdown."""
        markdown_lines = []
        
        for block in blocks:
            block_type = block.get("type", "")
            
            if block_type == "paragraph":
                text = self._extract_rich_text(block.get("paragraph", {}).get("rich_text", []))
                if text:
                    markdown_lines.append(text)
                    markdown_lines.append("")
            
            elif block_type == "heading_1":
                text = self._extract_rich_text(block.get("heading_1", {}).get("rich_text", []))
                if text:
                    markdown_lines.append(f"# {text}")
                    markdown_lines.append("")
            
            elif block_type == "heading_2":
                text = self._extract_rich_text(block.get("heading_2", {}).get("rich_text", []))
                if text:
                    markdown_lines.append(f"## {text}")
                    markdown_lines.append("")
            
            elif block_type == "heading_3":
                text = self._extract_rich_text(block.get("heading_3", {}).get("rich_text", []))
                if text:
                    markdown_lines.append(f"### {text}")
                    markdown_lines.append("")
            
            elif block_type == "bulleted_list_item":
                text = self._extract_rich_text(block.get("bulleted_list_item", {}).get("rich_text", []))
                if text:
                    markdown_lines.append(f"- {text}")
            
            elif block_type == "numbered_list_item":
                text = self._extract_rich_text(block.get("numbered_list_item", {}).get("rich_text", []))
                if text:
                    markdown_lines.append(f"1. {text}")
            
            elif block_type == "quote":
                text = self._extract_rich_text(block.get("quote", {}).get("rich_text", []))
                if text:
                    markdown_lines.append(f"> {text}")
                    markdown_lines.append("")
            
            elif block_type == "code":
                code_block = block.get("code", {})
                code_text = self._extract_rich_text(code_block.get("rich_text", []))
                language = code_block.get("language", "")
                if code_text:
                    markdown_lines.append(f"```{language}")
                    markdown_lines.append(code_text)
                    markdown_lines.append("```")
                    markdown_lines.append("")
            
            elif block_type == "divider":
                markdown_lines.append("---")
                markdown_lines.append("")
            
            elif block_type == "callout":
                callout = block.get("callout", {})
                text = self._extract_rich_text(callout.get("rich_text", []))
                emoji = callout.get("icon", {}).get("emoji", "💡")
                if text:
                    markdown_lines.append(f"{emoji} **Callout:** {text}")
                    markdown_lines.append("")
            
            # Add more block types as needed
            
        return markdown_lines

    def _extract_rich_text(self, rich_text: List[Dict[str, Any]]) -> str:
        """Extract plain text from Notion rich text."""
        if not rich_text:
            return ""
        
        text_parts = []
        for text_obj in rich_text:
            plain_text = text_obj.get("plain_text", "")
            annotations = text_obj.get("annotations", {})
            
            # Apply basic markdown formatting
            if annotations.get("bold"):
                plain_text = f"**{plain_text}**"
            if annotations.get("italic"):
                plain_text = f"*{plain_text}*"
            if annotations.get("code"):
                plain_text = f"`{plain_text}`"
            if annotations.get("strikethrough"):
                plain_text = f"~~{plain_text}~~"
            
            text_parts.append(plain_text)
        
        return "".join(text_parts)

    def _extract_title_from_rich_text(self, title_rich_text: List[Dict[str, Any]]) -> str:
        """Extract title from rich text array."""
        return self._extract_rich_text(title_rich_text)

    def _extract_page_title(self, page: Dict[str, Any]) -> str:
        """Extract page title from properties."""
        properties = page.get("properties", {})
        
        # Look for title property
        for prop_name, prop_data in properties.items():
            if prop_data.get("type") == "title":
                title_rich_text = prop_data.get("title", [])
                title = self._extract_rich_text(title_rich_text)
                if title:
                    return title
        
        # Fallback to page ID if no title found
        return f"Page {page.get('id', 'Unknown')}"

    def _extract_property_value(self, prop_data: Dict[str, Any]) -> str:
        """Extract value from a property based on its type."""
        prop_type = prop_data.get("type", "")
        
        if prop_type == "title":
            return self._extract_rich_text(prop_data.get("title", []))
        elif prop_type == "rich_text":
            return self._extract_rich_text(prop_data.get("rich_text", []))
        elif prop_type == "number":
            return str(prop_data.get("number", ""))
        elif prop_type == "select":
            select_obj = prop_data.get("select")
            return select_obj.get("name", "") if select_obj else ""
        elif prop_type == "multi_select":
            multi_select = prop_data.get("multi_select", [])
            return ", ".join([item.get("name", "") for item in multi_select])
        elif prop_type == "date":
            date_obj = prop_data.get("date")
            if date_obj:
                start = date_obj.get("start", "")
                end = date_obj.get("end", "")
                return f"{start} - {end}" if end else start
            return ""
        elif prop_type == "checkbox":
            return "Yes" if prop_data.get("checkbox", False) else "No"
        elif prop_type == "url":
            return prop_data.get("url", "")
        elif prop_type == "email":
            return prop_data.get("email", "")
        elif prop_type == "phone_number":
            return prop_data.get("phone_number", "")
        else:
            return str(prop_data.get(prop_type, ""))

    def get_page_document(self, page_id: str) -> Optional[Dict[str, Any]]:
        """Get a single page as a Scout document."""
        try:
            client = self.get_client()
            
            # Get page metadata
            page = client.pages.retrieve(page_id=page_id)
            page_title = self._extract_page_title(page)
            
            # Get page content
            page_content = self.get_page_content(page_id)
            
            if page_content:
                return {
                    "_key": f"notion_page_{page_id}",
                    "type": "page",
                    "content": page_content,
                    "document_type": "notion_page",
                    "title": page_title,
                    "page_id": page_id,
                    "url": page.get("url", ""),
                    "created_at": page.get("created_time", ""),
                    "updated_at": page.get("last_edited_time", ""),
                }
            
        except Exception as e:
            get_dagster_logger().error(f"Error processing page {page_id}: {e}")
        
        return None

    def search_pages(self, query: str = "") -> List[Dict[str, Any]]:
        """Search for pages and return as Scout documents."""
        documents = []
        
        try:
            client = self.get_client()
            
            search_params = {"filter": {"property": "object", "value": "page"}}
            if query:
                search_params["query"] = query
            
            response = client.search(**search_params)
            
            for page in response.get("results", []):
                page_id = page["id"]
                doc = self.get_page_document(page_id)
                if doc:
                    documents.append(doc)
            
            get_dagster_logger().info(f"Found {len(documents)} pages matching query: {query}")
            
        except Exception as e:
            get_dagster_logger().error(f"Error searching pages: {e}")
        
        return documents
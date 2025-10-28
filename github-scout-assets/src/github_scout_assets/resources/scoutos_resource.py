import json
from typing import Any, Dict, List

import requests
from dagster import ConfigurableResource, get_dagster_logger


class ScoutosResource(ConfigurableResource):
    """Resource for interacting with the ScoutOS API."""

    api_key: str

    @property
    def headers(self):
        return {
            "Accept": "*/*",
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self.api_key}",
        }

    def write_documents(
        self, collection_id: str, table_id: str, documents: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """Writes documents to the ScoutOS API."""
        if not documents:
            get_dagster_logger().info("No documents to write")
            return {"status": "success", "message": "No documents to write"}
            
        request_url = f"https://api.scoutos.com/v2/collections/{collection_id}/tables/{table_id}/documents?await_completion=false"
        payload = json.dumps(documents)
        
        # Log first document structure for debugging
        if documents:
            sample_doc = documents[0]
            get_dagster_logger().info(f"Writing documents with sample keys: {list(sample_doc.keys())}")
            get_dagster_logger().info(f"Sample title in payload: {sample_doc.get('title', 'MISSING')}")
            get_dagster_logger().info(f"Sample description in payload: {sample_doc.get('description', 'MISSING')[:100]}...")
        
        try:
            response = requests.post(request_url, data=payload, headers=self.headers)
            response.raise_for_status()
            result = response.json()
            get_dagster_logger().info(f"Successfully wrote {len(documents)} documents to ScoutOS")
            get_dagster_logger().info(f"Write response: {result}")
            return result
        except requests.exceptions.RequestException as e:
            get_dagster_logger().error(f"Error writing documents to ScoutOS: {e}")
            if hasattr(e, 'response') and e.response is not None:
                get_dagster_logger().error(f"Response content: {e.response.text}")
            raise

    def create_collection(self, name: str, description: str = "") -> Dict[str, Any]:
        """Creates a new collection in ScoutOS."""
        request_url = "https://api.scoutos.com/v2/collections"
        payload = json.dumps({
            "name": name,
            "collection_display_name": name,
            "collection_description": description
        })
        
        try:
            response = requests.post(request_url, data=payload, headers=self.headers)
            response.raise_for_status()
            result = response.json()
            get_dagster_logger().info(f"Created collection: {name}")
            return result
        except requests.exceptions.RequestException as e:
            get_dagster_logger().error(f"Error creating collection: {e}")
            raise

    def create_table(self, collection_id: str, name: str, schema: Dict[str, Any]) -> Dict[str, Any]:
        """Creates a new table in a ScoutOS collection."""
        request_url = f"https://api.scoutos.com/v2/collections/{collection_id}/tables"
        payload = json.dumps({
            "name": name,
            "table_display_name": name,
            "schema": schema
        })
        
        try:
            response = requests.post(request_url, data=payload, headers=self.headers)
            response.raise_for_status()
            result = response.json()
            get_dagster_logger().info(f"Created table: {name} in collection {collection_id}")
            get_dagster_logger().info(f"API response structure: {result}")
            get_dagster_logger().info(f"Table ID from response: {result.get('data', {}).get('table_id', 'NOT_FOUND')}")
            return result
        except requests.exceptions.RequestException as e:
            get_dagster_logger().error(f"Error creating table: {e}")
            raise

    def get_collections(self) -> List[Dict[str, Any]]:
        """Gets all collections."""
        request_url = "https://api.scoutos.com/v2/collections"
        
        try:
            response = requests.get(request_url, headers=self.headers)
            response.raise_for_status()
            result = response.json()
            return result.get('data', []) if isinstance(result, dict) else result
        except requests.exceptions.RequestException as e:
            get_dagster_logger().error(f"Error getting collections: {e}")
            raise

    def get_tables(self, collection_id: str) -> List[Dict[str, Any]]:
        """Gets all tables in a collection."""
        request_url = f"https://api.scoutos.com/v2/collections/{collection_id}/tables"
        
        try:
            response = requests.get(request_url, headers=self.headers)
            response.raise_for_status()
            result = response.json()
            return result.get('data', []) if isinstance(result, dict) else result
        except requests.exceptions.RequestException as e:
            get_dagster_logger().error(f"Error getting tables: {e}")
            raise

    def delete_table(self, collection_id: str, table_id: str) -> Dict[str, Any]:
        """Deletes a table from a ScoutOS collection."""
        request_url = f"https://api.scoutos.com/v2/collections/{collection_id}/tables/{table_id}"
        
        try:
            response = requests.delete(request_url, headers=self.headers)
            response.raise_for_status()
            get_dagster_logger().info(f"Deleted table {table_id} from collection {collection_id}")
            return response.json() if response.content else {"status": "success"}
        except requests.exceptions.RequestException as e:
            get_dagster_logger().error(f"Error deleting table: {e}")
            raise

    def get_table_schema(self, collection_id: str, table_id: str) -> Dict[str, Any]:
        """Gets the schema of a specific table."""
        request_url = f"https://api.scoutos.com/v2/collections/{collection_id}/tables/{table_id}"
        
        try:
            response = requests.get(request_url, headers=self.headers)
            response.raise_for_status()
            result = response.json()
            get_dagster_logger().info(f"Retrieved table schema for {table_id}")
            return result
        except requests.exceptions.RequestException as e:
            get_dagster_logger().error(f"Error getting table schema: {e}")
            raise
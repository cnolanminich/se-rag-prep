import os
import subprocess
import tempfile
import time
import json
from pathlib import Path
from typing import List, Dict, Any, Optional
from urllib.parse import urlparse

import requests
from dagster import ConfigurableResource, get_dagster_logger


class GitHubResource(ConfigurableResource):
    """Resource for cloning GitHub repositories and flattening content."""

    github_token: str

    def parse_github_url(self, url: str) -> tuple[str, str | None, str | None]:
        """Parse GitHub URL and extract base repo URL, branch, and subdirectory path."""
        try:
            parsed = urlparse(url)
            path_parts = parsed.path.strip("/").split("/")
            
            if len(path_parts) < 2:
                return url, None, None
            
            # Check if URL contains tree/branch/subdirectory structure
            if len(path_parts) >= 4 and path_parts[2] == "tree":
                # URL format: github.com/owner/repo/tree/branch/path/to/subdirectory
                owner = path_parts[0]
                repo = path_parts[1]
                branch = path_parts[3]
                subdirectory = "/".join(path_parts[4:]) if len(path_parts) > 4 else None
                
                base_url = f"https://github.com/{owner}/{repo}.git"
                return base_url, branch, subdirectory
            else:
                # Regular repository URL
                return url, None, None
                
        except Exception as e:
            get_dagster_logger().error(f"Error parsing URL {url}: {e}")
            return url, None, None

    def clone_repository(self, owner: str, repo: str, target_dir: Path) -> bool:
        """Clone a git repository."""
        repo_url = f"https://github.com/{owner}/{repo}.git"
        
        try:
            get_dagster_logger().info(f"Cloning {repo_url}...")
            
            # Use token for authentication if provided
            if self.github_token:
                auth_url = f"https://{self.github_token}@github.com/{owner}/{repo}.git"
            else:
                auth_url = repo_url
            
            result = subprocess.run(
                ["git", "clone", auth_url, str(target_dir)],
                capture_output=True,
                text=True,
                timeout=300  # 5 minute timeout
            )
            
            if result.returncode == 0:
                get_dagster_logger().info(f"Successfully cloned {repo_url}")
                return True
            else:
                get_dagster_logger().error(f"Failed to clone {repo_url}: {result.stderr}")
                return False
                
        except subprocess.TimeoutExpired:
            get_dagster_logger().error(f"Timeout cloning {repo_url}")
            return False
        except subprocess.CalledProcessError as e:
            get_dagster_logger().error(f"Git error cloning {repo_url}: {e}")
            return False
        except FileNotFoundError:
            get_dagster_logger().error("Git not found. Please install git and ensure it's in your PATH")
            return False

    def flatten_repository_to_markdown(self, repo_path: Path, owner: str, repo: str) -> str | None:
        """Flatten a repository directory into a single markdown string."""
        try:
            if not repo_path.is_dir() or not any(repo_path.iterdir()):
                return None
            
            get_dagster_logger().info(f"Flattening {owner}/{repo} to markdown...")
            
            # Collect all relevant files
            code_extensions = {'.py', '.sql', '.yaml', '.yml', '.toml', '.json', '.md', '.txt', '.sh', '.js', '.ts', '.css', '.html', '.r', '.R', '.ipynb'}
            
            markdown_content = []
            
            # Add header
            markdown_content.append(f"# {owner}/{repo}")
            markdown_content.append("")
            markdown_content.append(f"Repository: https://github.com/{owner}/{repo}")
            markdown_content.append(f"Flattened: {time.strftime('%Y-%m-%d %H:%M:%S')}")
            markdown_content.append("")
            
            # Check if it's a git repository and add metadata
            git_dir = repo_path / ".git"
            if git_dir.exists():
                try:
                    # Try to get git remote info
                    result = subprocess.run(
                        ["git", "remote", "get-url", "origin"],
                        cwd=repo_path,
                        capture_output=True,
                        text=True,
                        timeout=10
                    )
                    if result.returncode == 0:
                        remote_url = result.stdout.strip()
                        markdown_content.append(f"**Git Remote:** {remote_url}")
                        markdown_content.append("")
                except Exception:
                    pass
            
            # Add README content if it exists
            readme_files = ['README.md', 'readme.md', 'README.txt', 'README']
            for readme_name in readme_files:
                readme_path = repo_path / readme_name
                if readme_path.exists():
                    try:
                        with open(readme_path, 'r', encoding='utf-8', errors='ignore') as f:
                            readme_content = f.read()
                        markdown_content.append("## README")
                        markdown_content.append("")
                        markdown_content.append(readme_content)
                        markdown_content.append("")
                        break
                    except Exception:
                        pass
            
            # Recursively collect all files
            files_processed = 0
            for file_path in sorted(repo_path.rglob("*")):
                if file_path.is_file() and file_path.suffix.lower() in code_extensions:
                    # Skip git files and other system files
                    if '.git' in file_path.parts or file_path.name.startswith('.'):
                        continue
                    
                    # Skip very large files (>1MB)
                    try:
                        if file_path.stat().st_size > 1024 * 1024:
                            continue
                    except Exception:
                        continue
                    
                    try:
                        with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                            file_content = f.read()
                        
                        # Get relative path from repo root
                        rel_path = file_path.relative_to(repo_path)
                        
                        # Add file section
                        markdown_content.append(f"## File: {rel_path}")
                        markdown_content.append("")
                        
                        # Determine language for syntax highlighting
                        ext = file_path.suffix.lower()
                        language_map = {
                            '.py': 'python',
                            '.sql': 'sql',
                            '.yaml': 'yaml',
                            '.yml': 'yaml',
                            '.toml': 'toml',
                            '.json': 'json',
                            '.sh': 'bash',
                            '.js': 'javascript',
                            '.ts': 'typescript',
                            '.css': 'css',
                            '.html': 'html',
                            '.r': 'r',
                            '.R': 'r',
                            '.ipynb': 'json'
                        }
                        language = language_map.get(ext, 'text')
                        
                        if ext == '.md':
                            # For markdown files, include content directly
                            markdown_content.append(file_content)
                        else:
                            # For code files, wrap in code blocks
                            markdown_content.append(f"```{language}")
                            markdown_content.append(file_content)
                            markdown_content.append("```")
                        
                        markdown_content.append("")
                        files_processed += 1
                        
                    except Exception as e:
                        get_dagster_logger().warning(f"Could not read {file_path}: {e}")
                        continue
            
            if files_processed == 0:
                get_dagster_logger().warning(f"No relevant files found in {owner}/{repo}")
                return None
            
            get_dagster_logger().info(f"Flattened {owner}/{repo} ({files_processed} files)")
            return '\n'.join(markdown_content)
            
        except Exception as e:
            get_dagster_logger().error(f"Error flattening {owner}/{repo}: {e}")
            return None

    def extract_description_from_content(self, content: str, owner: str, repo: str) -> str:
        """Extract a description from repository content, primarily from README."""
        try:
            # Look for README section in the flattened content
            lines = content.split('\n')
            in_readme = False
            readme_lines = []
            
            for line in lines:
                if line.strip() == "## README":
                    in_readme = True
                    continue
                elif line.startswith("## File:") and in_readme:
                    # End of README section
                    break
                elif in_readme and line.strip():
                    # Skip markdown headers that are just the repo name
                    if line.startswith('#') and (repo.lower() in line.lower() or owner.lower() in line.lower()):
                        continue
                    readme_lines.append(line.strip())
            
            if readme_lines:
                # Take the first meaningful paragraph from README
                description_parts = []
                for line in readme_lines[:10]:  # Look at first 10 lines
                    if line and not line.startswith('#') and not line.startswith('**'):
                        description_parts.append(line)
                        # Stop after getting a substantial description (around 200 chars)
                        if len(' '.join(description_parts)) > 200:
                            break
                
                if description_parts:
                    return ' '.join(description_parts)[:500]  # Limit to 500 chars
            
            # Fallback: create a basic description
            return f"Repository {owner}/{repo} containing code and documentation files."
            
        except Exception as e:
            get_dagster_logger().warning(f"Could not extract description from {owner}/{repo}: {e}")
            return f"Repository {owner}/{repo} containing code and documentation files."

    def get_repository_content(self, owner: str, repo: str) -> List[Dict[str, Any]]:
        """Clone repository and flatten content into Scout documents."""
        documents = []
        
        with tempfile.TemporaryDirectory() as temp_dir:
            repo_dir = Path(temp_dir) / repo
            
            # Clone the repository
            if not self.clone_repository(owner, repo, repo_dir):
                get_dagster_logger().error(f"Failed to clone {owner}/{repo}")
                return documents
            
            # Flatten repository content
            flattened_content = self.flatten_repository_to_markdown(repo_dir, owner, repo)
            
            if flattened_content:
                # Extract description from README or create a basic one
                description = self.extract_description_from_content(flattened_content, owner, repo)
                
                # Create comprehensive repository document using Scout's actual column IDs
                repo_doc = {
                    "_key": f"{owner}/{repo}",
                    "type": "repository",
                    "content": flattened_content,  # Content column (supports markdown)
                    "document_type": "flattened_repository",
                    "cmfeg8drs00wz0fs60q668chq": f"{owner}/{repo}",  # Title column
                    "cmfeg8drs00x00fs6b44bexkp": description,  # Description column
                    "owner": owner,
                    "repo": repo,
                    "url": f"https://github.com/{owner}/{repo}",
                    "created_at": time.strftime('%Y-%m-%dT%H:%M:%SZ'),
                    "updated_at": time.strftime('%Y-%m-%dT%H:%M:%SZ'),
                }
                documents.append(repo_doc)
                
                get_dagster_logger().info(f"Created flattened document for {owner}/{repo}")
            
        return documents

    def get_gist_content(self, gist_id: str, gist_name: str) -> List[Dict[str, Any]]:
        """Download gist and flatten content into Scout documents."""
        documents = []
        
        try:
            get_dagster_logger().info(f"Downloading gist {gist_id} ({gist_name})...")
            
            # Set up session with authentication if available
            session = requests.Session()
            if self.github_token:
                session.headers.update({"Authorization": f"token {self.github_token}"})
            
            # Get gist metadata
            response = session.get(f"https://api.github.com/gists/{gist_id}")
            response.raise_for_status()
            
            gist_data = response.json()
            
            # Create flattened markdown content
            markdown_content = []
            
            # Add header
            gist_description = gist_data.get("description", gist_name)
            owner = gist_data.get("owner", {}).get("login", "unknown")
            
            markdown_content.append(f"# Gist: {gist_description}")
            markdown_content.append("")
            markdown_content.append(f"**Gist ID:** {gist_id}")
            markdown_content.append(f"**Owner:** {owner}")
            markdown_content.append(f"**URL:** {gist_data.get('html_url', '')}")
            markdown_content.append(f"**Created:** {gist_data.get('created_at', '')}")
            markdown_content.append(f"**Updated:** {gist_data.get('updated_at', '')}")
            markdown_content.append("")
            
            if gist_description:
                markdown_content.append(f"**Description:** {gist_description}")
                markdown_content.append("")
            
            # Process each file in the gist
            files_processed = 0
            for filename, file_data in gist_data.get("files", {}).items():
                content = file_data.get("content", "")
                if content:
                    markdown_content.append(f"## File: {filename}")
                    markdown_content.append("")
                    
                    # Determine language for syntax highlighting
                    language = file_data.get("language", "").lower() or "text"
                    language_map = {
                        "python": "python",
                        "sql": "sql", 
                        "yaml": "yaml",
                        "json": "json",
                        "javascript": "javascript",
                        "typescript": "typescript",
                        "bash": "bash",
                        "shell": "bash",
                        "r": "r",
                    }
                    lang = language_map.get(language, "text")
                    
                    if filename.lower().endswith('.md'):
                        # For markdown files, include content directly
                        markdown_content.append(content)
                    else:
                        # For code files, wrap in code blocks
                        markdown_content.append(f"```{lang}")
                        markdown_content.append(content)
                        markdown_content.append("```")
                    
                    markdown_content.append("")
                    files_processed += 1
            
            if files_processed > 0:
                # Create gist document using Scout's actual column IDs
                gist_doc = {
                    "_key": f"gist_{gist_id}",
                    "type": "gist",
                    "content": '\n'.join(markdown_content),  # Content column (supports markdown)
                    "document_type": "flattened_gist",
                    "cmfeg8drs00wz0fs60q668chq": gist_description or gist_name,  # Title column
                    "cmfeg8drs00x00fs6b44bexkp": gist_description or f"GitHub Gist {gist_name} containing {files_processed} file(s)",  # Description column
                    "gist_id": gist_id,
                    "owner": owner,
                    "url": gist_data.get("html_url", ""),
                    "created_at": gist_data.get("created_at", ""),
                    "updated_at": gist_data.get("updated_at", ""),
                    "file_count": files_processed,
                    "files": list(gist_data.get("files", {}).keys()),
                }
                documents.append(gist_doc)
                
                get_dagster_logger().info(f"Created flattened gist document for {gist_id}")
            else:
                get_dagster_logger().warning(f"No files found in gist {gist_id}")
                
        except Exception as e:
            get_dagster_logger().error(f"Error processing gist {gist_id}: {e}")
        
        return documents
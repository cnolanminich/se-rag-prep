#!/usr/bin/env python3
"""
GitHub Repository and Gist Downloader
=====================================

This script downloads all GitHub repositories and gists mentioned in the 
Dagster Common Resources documentation. It organizes them into categorized 
folders and handles both git cloning and direct file downloads.

Requirements:
- git (must be installed and available in PATH)
- requests library: pip install requests
- Optional: GitHub token for higher rate limits (set GITHUB_TOKEN env var)

Usage:
    python github_downloader.py [--output-dir DIRECTORY] [--token TOKEN]
"""

import os
import sys
import json
import time
import argparse
import subprocess
from pathlib import Path
from urllib.parse import urlparse
from typing import List, Dict, Optional, Tuple

try:
    import requests
except ImportError:
    print("Error: 'requests' library not found. Install with: pip install requests")
    sys.exit(1)


class GitHubDownloader:
    def __init__(self, output_dir: str = "dagster_resources", github_token: Optional[str] = None):
        self.output_dir = Path(output_dir)
        self.github_token = github_token
        self.session = requests.Session()
        
        # Set up authentication if token provided
        if github_token:
            self.session.headers.update({"Authorization": f"token {github_token}"})
        
        # Create output directory structure
        self.setup_directories()
        
        # Rate limiting
        self.last_request_time = 0
        self.min_request_interval = 1.0  # seconds between API requests

    def setup_directories(self):
        """Create organized directory structure"""
        directories = [
            "official_examples",
            "community_integrations", 
            "user_examples/cnolanminich",
            "user_examples/slopp",
            "user_examples/other",
            "gists",
            "documentation_examples",
            "download_logs"
        ]
        
        for directory in directories:
            (self.output_dir / directory).mkdir(parents=True, exist_ok=True)
        
        print(f"📁 Created directory structure in: {self.output_dir.absolute()}")

    def rate_limit(self):
        """Implement basic rate limiting"""
        elapsed = time.time() - self.last_request_time
        if elapsed < self.min_request_interval:
            time.sleep(self.min_request_interval - elapsed)
        self.last_request_time = time.time()

    def parse_github_url(self, url: str) -> Tuple[str, Optional[str], Optional[str]]:
        """Parse GitHub URL and extract base repo URL, branch, and subdirectory path"""
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
            print(f"⚠️ Error parsing URL {url}: {e}")
            return url, None, None

    def clone_subdirectory(self, repo_url: str, branch: str, subdirectory: str, target_dir: Path) -> bool:
        """Clone only a specific subdirectory from a repository using sparse-checkout"""
        try:
            print(f"🔄 Cloning subdirectory {subdirectory} from {repo_url} (branch: {branch})...")
            
            # Create target directory
            target_dir.mkdir(parents=True, exist_ok=True)
            
            # Initialize git repository
            result = subprocess.run(
                ["git", "init"],
                cwd=target_dir,
                capture_output=True,
                text=True,
                timeout=30
            )
            if result.returncode != 0:
                print(f"❌ Failed to initialize git repository: {result.stderr}")
                return False
            
            # Add remote origin
            result = subprocess.run(
                ["git", "remote", "add", "origin", repo_url],
                cwd=target_dir,
                capture_output=True,
                text=True,
                timeout=30
            )
            if result.returncode != 0:
                print(f"❌ Failed to add remote: {result.stderr}")
                return False
            
            # Enable sparse-checkout
            result = subprocess.run(
                ["git", "config", "core.sparseCheckout", "true"],
                cwd=target_dir,
                capture_output=True,
                text=True,
                timeout=30
            )
            if result.returncode != 0:
                print(f"❌ Failed to enable sparse-checkout: {result.stderr}")
                return False
            
            # Configure sparse-checkout to include only the subdirectory
            sparse_checkout_file = target_dir / ".git" / "info" / "sparse-checkout"
            sparse_checkout_file.parent.mkdir(parents=True, exist_ok=True)
            with open(sparse_checkout_file, 'w') as f:
                f.write(f"{subdirectory}/\n")
            
            # Fetch the specific branch
            result = subprocess.run(
                ["git", "fetch", "origin", branch],
                cwd=target_dir,
                capture_output=True,
                text=True,
                timeout=300
            )
            if result.returncode != 0:
                print(f"❌ Failed to fetch branch {branch}: {result.stderr}")
                return False
            
            # Checkout the branch
            result = subprocess.run(
                ["git", "checkout", f"origin/{branch}"],
                cwd=target_dir,
                capture_output=True,
                text=True,
                timeout=60
            )
            if result.returncode != 0:
                print(f"❌ Failed to checkout branch {branch}: {result.stderr}")
                return False
            
            # Verify that the subdirectory exists
            subdirectory_path = target_dir / subdirectory
            if not subdirectory_path.exists():
                print(f"❌ Subdirectory {subdirectory} does not exist in the repository")
                return False
            
            print(f"✅ Successfully cloned subdirectory {subdirectory} from {repo_url}")
            return True
            
        except subprocess.TimeoutExpired:
            print(f"⏰ Timeout cloning subdirectory from {repo_url}")
            return False
        except Exception as e:
            print(f"❌ Error cloning subdirectory from {repo_url}: {e}")
            return False

    def clone_repository(self, repo_url: str, target_dir: Path) -> bool:
        """Clone a git repository or subdirectory"""
        try:
            # Parse the URL to determine if it's a subdirectory
            base_url, branch, subdirectory = self.parse_github_url(repo_url)
            
            if subdirectory:
                # Clone only the subdirectory
                return self.clone_subdirectory(base_url, branch, subdirectory, target_dir)
            else:
                # Clone the entire repository
                print(f"🔄 Cloning {repo_url}...")
                result = subprocess.run(
                    ["git", "clone", base_url, str(target_dir)],
                    capture_output=True,
                    text=True,
                    timeout=300  # 5 minute timeout
                )
                
                if result.returncode == 0:
                    print(f"✅ Successfully cloned {repo_url}")
                    return True
                else:
                    print(f"❌ Failed to clone {repo_url}: {result.stderr}")
                    return False
                
        except subprocess.TimeoutExpired:
            print(f"⏰ Timeout cloning {repo_url}")
            return False
        except subprocess.CalledProcessError as e:
            print(f"❌ Git error cloning {repo_url}: {e}")
            return False
        except FileNotFoundError:
            print("❌ Git not found. Please install git and ensure it's in your PATH")
            return False

    def download_gist(self, gist_id: str, target_dir: Path, description: str = "") -> bool:
        """Download a GitHub gist"""
        try:
            self.rate_limit()
            
            # Get gist metadata
            response = self.session.get(f"https://api.github.com/gists/{gist_id}")
            response.raise_for_status()
            
            gist_data = response.json()
            gist_name = description.replace(" ", "_").replace("/", "_") if description else gist_id
            gist_dir = target_dir / f"{gist_name}_{gist_id}"
            gist_dir.mkdir(parents=True, exist_ok=True)
            
            print(f"📄 Downloading gist {gist_id} ({description})...")
            
            # Download each file in the gist
            for filename, file_data in gist_data["files"].items():
                file_path = gist_dir / filename
                with open(file_path, 'w', encoding='utf-8') as f:
                    f.write(file_data["content"])
            
            # Save metadata
            metadata = {
                "id": gist_id,
                "description": gist_data.get("description", ""),
                "created_at": gist_data.get("created_at", ""),
                "updated_at": gist_data.get("updated_at", ""),
                "owner": gist_data.get("owner", {}).get("login", ""),
                "html_url": gist_data.get("html_url", ""),
                "files": list(gist_data["files"].keys())
            }
            
            with open(gist_dir / "gist_metadata.json", 'w') as f:
                json.dump(metadata, f, indent=2)
            
            print(f"✅ Downloaded gist {gist_id}")
            return True
            
        except Exception as e:
            print(f"❌ Failed to download gist {gist_id}: {e}")
            return False

    def extract_gist_id(self, gist_input: str) -> Optional[str]:
        """Extract gist ID from GitHub gist URL or return the ID if already provided"""
        try:
            # If it's already a gist ID (32 hex chars), return it
            if len(gist_input) == 32 and all(c in '0123456789abcdef' for c in gist_input.lower()):
                return gist_input
            
            # Try to extract from URL
            if 'gist.github.com' in gist_input:
                parsed = urlparse(gist_input)
                path_parts = parsed.path.strip("/").split("/")
                # URL format: gist.github.com/username/gist_id
                if len(path_parts) >= 2:
                    return path_parts[-1]  # Last part should be the gist ID
                elif len(path_parts) == 1:
                    return path_parts[0]   # Just the gist ID
            
            return None
        except Exception:
            return None

    def extract_repo_info(self, url: str) -> Optional[Tuple[str, str]]:
        """Extract owner and repo name from GitHub URL"""
        try:
            # Parse URL to get base repo info
            base_url, branch, subdirectory = self.parse_github_url(url)
            
            parsed = urlparse(base_url)
            if "github.com" not in parsed.netloc:
                return None
            
            path_parts = parsed.path.strip("/").split("/")
            if len(path_parts) >= 2:
                owner = path_parts[0]
                repo = path_parts[1].replace(".git", "")
                return owner, repo
            return None
        except Exception:
            return None

    def get_target_directory(self, owner: str, repo: str, url: str) -> Path:
        """Determine appropriate target directory based on owner, repo, and URL"""
        base_dir = None
        
        if owner == "dagster-io":
            if repo in ["hooli-data-eng-pipelines", "dagster-open-platform", "awesome-dagster"]:
                base_dir = self.output_dir / "official_examples"
            elif repo == "community-integrations":
                base_dir = self.output_dir / "community_integrations"
            else:
                base_dir = self.output_dir / "official_examples"
        elif owner == "cnolanminich":
            base_dir = self.output_dir / "user_examples" / "cnolanminich"
        elif owner == "slopp":
            base_dir = self.output_dir / "user_examples" / "slopp"
        else:
            base_dir = self.output_dir / "user_examples" / "other"
        
        # Check if this is a subdirectory URL
        base_url, branch, subdirectory = self.parse_github_url(url)
        if subdirectory:
            # For subdirectories, use the last part of the subdirectory path as the folder name
            subdirectory_name = subdirectory.split("/")[-1]
            return base_dir / subdirectory_name
        else:
            # For regular repos, use the repo name
            return base_dir / repo

    def download_repositories(self) -> Dict[str, bool]:
        """Download all repositories mentioned in the Common Resources"""
        
        # Repository URLs from the Common Resources document
        repositories = [
            # Official Dagster Examples
            "https://github.com/dagster-io/hooli-data-eng-pipelines",
            "https://github.com/dagster-io/dagster-open-platform",
            "https://github.com/dagster-io/awesome-dagster",
            "https://github.com/dagster-io/community-integrations",
            
            # Documentation Examples
            "https://github.com/dagster-io/dagster/tree/master/examples/docs_projects/project_atproto_dashboard",
            "https://github.com/dagster-io/dagster/tree/master/examples/docs_projects/project_llm_fine_tune",
            "https://github.com/dagster-io/dagster/tree/master/examples/docs_projects/project_dagster_modal_pipes",
            
            # User Examples - cnolanminich
            "https://github.com/cnolanminich/azure_demo_20241003",
            "https://github.com/cnolanminich/30_days_of_dagster", 
            "https://github.com/cnolanminich/dagster-and-r",
            "https://github.com/cnolanminich/dbt-multiple-code-locations",
            "https://github.com/cnolanminich/dbt-loom-example",
            "https://github.com/cnolanminich/dagster-airbyte-dbt-demo",
            "https://github.com/cnolanminich/dagster_dbt_lineage",
            "https://github.com/cnolanminich/dagster-argo-workflows",
            "https://github.com/cnolanminich/pipes-non-pipes-demo",
            "https://github.com/cnolanminich/dagster-sensor-partitions-example",
            "https://github.com/cnolanminich/dagster_dynamic_out_vs_paralleism",
            "https://github.com/cnolanminich/dagster_kafka_demo",
            "https://github.com/cnolanminich/dagster_example_external_source",
            "https://github.com/cnolanminich/data-contracts-example",
            "https://github.com/cnolanminich/automation-condition-testing",
            "https://github.com/cnolanminich/partition-custom-calendar",
            
            # User Examples - slopp  
            "https://github.com/slopp/freshness_guide",
            "https://github.com/slopp/dagster_kafka_demo", 
            "https://github.com/slopp/snowreport",
            "https://github.com/slopp/dagteam",
            "https://github.com/slopp/dagster-dynamic-partitions"
            "https://github.com/cnolanminich/testing-dynamic-fanout"
        ]
        
        results = {}
        
        for repo_url in repositories:
            repo_info = self.extract_repo_info(repo_url)
            if not repo_info:
                print(f"⚠️  Could not parse URL: {repo_url}")
                results[repo_url] = False
                continue
                
            owner, repo = repo_info
            target_dir = self.get_target_directory(owner, repo, repo_url)
            
            # Skip if already exists
            if target_dir.exists() and any(target_dir.iterdir()):
                print(f"⏭️  Skipping {repo_url} (already exists)")
                results[repo_url] = True
                continue
            
            # Clone the repository
            success = self.clone_repository(repo_url, target_dir)
            results[repo_url] = success
            
            # Brief pause between repos
            time.sleep(2)
        
        return results

    def download_gists(self) -> Dict[str, bool]:
        """Download all gists mentioned in the Common Resources"""
        
        # Gist IDs and descriptions from the Common Resources document
        gists = [
            ("74f96fbfaf9e2a72f37c536ba21d597a", "GitHub Action - Release Based Deployment"),
            ("07b00b3402c7e22589ee03b39063d8a9", "OSS Dagster - Standard Credit Query"),
            ("23739d22f310eb71d8a24ab9e639febd", "OSS Dagster - Concurrency Model"),
            ("0376d9653f1a8b0a8dfe7f4120055bd9", "OSS Dagster - High Velocity Credits"),
            ("b3cdc8fec9f9442069efbf86cfc30204", "Dagster and SageMaker"),
            ("554dc45ae164b36e6a0cfa08906a9af1", "Run Status Sensor - Wait for Two Jobs"),
            ("ab981bae677a75bcad15c352c3d42507", "Automation Conditions - Update Once Per Period"),
            ("8dd440c754f133113f2f668fbe92c6e9", "Dagster - Controlling Parallelism within a Run"),
            ("bb1d8fd7738dbc6c4f1c320486a378fb", "Dagster -- Dagster+ High Velocity and Standard Credits Query")
        ]
        
        results = {}
        gist_dir = self.output_dir / "gists"
        
        for gist_id, description in gists:
            success = self.download_gist(gist_id, gist_dir, description)
            results[gist_id] = success
            
            # Rate limiting pause
            time.sleep(2)
        
        return results

    def flatten_repository_to_markdown(self, repo_path: Path, output_path: Path) -> bool:
        """Flatten a repository directory into a single markdown file"""
        try:
            # Skip if it's not a directory or is empty
            if not repo_path.is_dir() or not any(repo_path.iterdir()):
                return False
            
            # Skip .DS_Store and other system files
            if repo_path.name.startswith('.'):
                return False
            
            print(f"📝 Flattening {repo_path.name} to markdown...")
            
            # Collect all relevant files
            code_extensions = {'.py', '.sql', '.yaml', '.yml', '.toml', '.json', '.md', '.txt', '.sh', '.js', '.ts', '.css', '.html', '.r', '.R', '.ipynb'}
            
            # Get repository metadata
            repo_name = repo_path.name
            markdown_content = []
            
            # Add header
            markdown_content.append(f"# {repo_name}")
            markdown_content.append("")
            markdown_content.append(f"Repository flattened from: `{repo_path}`")
            markdown_content.append(f"Generated: {time.strftime('%Y-%m-%d %H:%M:%S')}")
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
                        print(f"⚠️ Could not read {file_path}: {e}")
                        continue
            
            if files_processed == 0:
                print(f"⚠️ No relevant files found in {repo_name}")
                return False
            
            # Write the markdown file
            output_path.parent.mkdir(parents=True, exist_ok=True)
            with open(output_path, 'w', encoding='utf-8') as f:
                f.write('\n'.join(markdown_content))
            
            print(f"✅ Flattened {repo_name} ({files_processed} files) -> {output_path.name}")
            return True
            
        except Exception as e:
            print(f"❌ Error flattening {repo_path.name}: {e}")
            return False

    def flatten_all_repositories(self) -> Dict[str, bool]:
        """Flatten all downloaded repositories into markdown files"""
        print("\n" + "="*60)
        print("📝 FLATTENING REPOSITORIES TO MARKDOWN")
        print("="*60)
        
        results = {}
        
        # Create flattened output directory
        flattened_dir = self.output_dir / "flattened_repositories"
        flattened_dir.mkdir(parents=True, exist_ok=True)
        
        # Categories to process
        categories = [
            ("official_examples", "Official Dagster Examples"),
            ("community_integrations", "Community Integrations"),
            ("user_examples", "User Examples"),
            ("gists", "GitHub Gists")
        ]
        
        for category_dir, category_name in categories:
            category_path = self.output_dir / category_dir
            if not category_path.exists():
                continue
            
            print(f"\n📁 Processing {category_name}...")
            
            # Process repositories in this category
            for repo_path in category_path.iterdir():
                if repo_path.is_dir() and not repo_path.name.startswith('.'):
                    # Handle nested user examples
                    if category_dir == "user_examples":
                        # For user examples, go one level deeper
                        for user_dir in repo_path.iterdir():
                            if user_dir.is_dir() and not user_dir.name.startswith('.'):
                                output_file = flattened_dir / f"{repo_path.name}_{user_dir.name}.md"
                                success = self.flatten_repository_to_markdown(user_dir, output_file)
                                results[f"{category_dir}/{repo_path.name}/{user_dir.name}"] = success
                    else:
                        # For other categories, flatten directly
                        output_file = flattened_dir / f"{category_dir}_{repo_path.name}.md"
                        success = self.flatten_repository_to_markdown(repo_path, output_file)
                        results[f"{category_dir}/{repo_path.name}"] = success
        
        return results

    def generate_report(self, repo_results: Dict[str, bool], gist_results: Dict[str, bool], flatten_results: Optional[Dict[str, bool]] = None):
        """Generate a download report"""
        report_path = self.output_dir / "download_logs" / "download_report.md"
        
        total_repos = len(repo_results)
        successful_repos = sum(repo_results.values())
        total_gists = len(gist_results)
        successful_gists = sum(gist_results.values())
        
        report = f"""# Dagster Resources Download Report
Generated: {time.strftime('%Y-%m-%d %H:%M:%S')}

## Summary
- **Repositories**: {successful_repos}/{total_repos} successful
- **Gists**: {successful_gists}/{total_gists} successful
- **Total Success Rate**: {((successful_repos + successful_gists) / (total_repos + total_gists)) * 100:.1f}%"""

        # Add flattening results if available
        if flatten_results:
            total_flattened = len(flatten_results)
            successful_flattened = sum(flatten_results.values())
            report += f"""
- **Flattened to Markdown**: {successful_flattened}/{total_flattened} successful"""

        report += """

## Repositories

### ✅ Successfully Downloaded
"""
        
        for repo_url, success in repo_results.items():
            if success:
                report += f"- {repo_url}\n"
        
        report += "\n### ❌ Failed Downloads\n"
        for repo_url, success in repo_results.items():
            if not success:
                report += f"- {repo_url}\n"
        
        report += "\n## Gists\n\n### ✅ Successfully Downloaded\n"
        for gist_id, success in gist_results.items():
            if success:
                report += f"- {gist_id}\n"
        
        report += "\n### ❌ Failed Downloads\n"
        for gist_id, success in gist_results.items():
            if not success:
                report += f"- {gist_id}\n"
        
        # Add flattening results if available
        if flatten_results:
            report += "\n## Flattened Markdown Files\n\n### ✅ Successfully Flattened\n"
            for repo_path, success in flatten_results.items():
                if success:
                    report += f"- {repo_path}\n"
            
            report += "\n### ❌ Failed to Flatten\n"
            for repo_path, success in flatten_results.items():
                if not success:
                    report += f"- {repo_path}\n"
        
        report += f"""
## Directory Structure
```
{self.output_dir.name}/
├── official_examples/          # Dagster official repositories
├── community_integrations/     # Community integration examples  
├── user_examples/
│   ├── cnolanminich/          # Christian's examples
│   ├── slopp/                 # Sean's examples
│   └── other/                 # Other user examples
├── gists/                     # GitHub gists with code snippets
├── documentation_examples/    # Examples from docs
├── flattened_repositories/    # Markdown files with flattened content
└── download_logs/             # This report and logs
```

## Usage Notes
- Each repository contains its own README with setup instructions
- Gists include metadata files with original URLs and descriptions
- **Flattened markdown files** in `flattened_repositories/` contain all code from each repository
- Check individual repositories for their specific requirements
- Some repositories may require additional dependencies

## Next Steps
1. Review failed downloads and retry if needed
2. Check each repository's README for setup instructions
3. Install any required dependencies per repository
4. **Use flattened markdown files** for easy content search and analysis
5. Refer to the Common Resources guide for context on each example
"""
        
        with open(report_path, 'w') as f:
            f.write(report)
        
        print(f"\n📋 Download report saved to: {report_path}")
        return report

    def run(self):
        """Execute the complete download process"""
        print("🚀 Starting Dagster Resources Download...")
        print(f"📁 Output directory: {self.output_dir.absolute()}")
        
        if self.github_token:
            print("🔑 Using GitHub token for authentication")
        else:
            print("⚠️  No GitHub token provided - may hit rate limits")
            print("   Set GITHUB_TOKEN environment variable for higher limits")
        
        print("\n" + "="*60)
        print("📦 DOWNLOADING REPOSITORIES")
        print("="*60)
        
        repo_results = self.download_repositories()
        
        print("\n" + "="*60)
        print("📄 DOWNLOADING GISTS")
        print("="*60)
        
        gist_results = self.download_gists()
        
        print("\n" + "="*60)
        print("� FLATTENING REPOSITORIES TO MARKDOWN")
        print("="*60)
        
        flatten_results = self.flatten_all_repositories()
        
        print("\n" + "="*60)
        print("�📋 GENERATING REPORT")
        print("="*60)
        
        self.generate_report(repo_results, gist_results, flatten_results)
        
        print("\n🎉 Download process completed!")
        print(f"📊 Results: {sum(repo_results.values())}/{len(repo_results)} repos, {sum(gist_results.values())}/{len(gistResults)} gists, {sum(flattenResults.values())}/{len(flattenResults)} flattened")


def main():
    parser = argparse.ArgumentParser(
        description="Download GitHub repositories and gists from Dagster Common Resources",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    python github_downloader.py
    python github_downloader.py --output-dir /path/to/downloads
    python github_downloader.py --token ghp_your_token_here
    python github_downloader.py --repo-url https://github.com/user/repo
    python github_downloader.py --gist-id abc123def456
    python github_downloader.py --gist-id https://gist.github.com/user/abc123def456
    
Environment Variables:
    GITHUB_TOKEN    GitHub personal access token for higher rate limits
        """
    )
    
    parser.add_argument(
        "--output-dir", 
        default="dagster_resources",
        help="Output directory for downloads (default: dagster_resources)"
    )
    
    parser.add_argument(
        "--token",
        default=os.getenv("GITHUB_TOKEN"),
        help="GitHub personal access token (or set GITHUB_TOKEN env var)"
    )
    
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be downloaded without actually downloading"
    )
    
    parser.add_argument(
        "--repo-url",
        default=None,
        help="Download and flatten only the specified GitHub repository URL (optional)"
    )
    
    parser.add_argument(
        "--gist-id",
        default=None,
        help="Download and flatten only the specified GitHub gist ID or URL (optional)"
    )
    
    args = parser.parse_args()
    
    if args.dry_run:
        print("🔍 DRY RUN MODE - No files will be downloaded")
        # TODO: Implement dry run functionality
        return

    try:
        downloader = GitHubDownloader(
            output_dir=args.output_dir,
            github_token=args.token
        )
        if args.repo_url:
            print(f"🚀 Downloading single repository: {args.repo_url}")
            repo_info = downloader.extract_repo_info(args.repo_url)
            if not repo_info:
                print(f"❌ Could not parse repo URL: {args.repo_url}")
                sys.exit(1)
            owner, repo = repo_info
            target_dir = downloader.get_target_directory(owner, repo, args.repo_url)
            success = downloader.clone_repository(args.repo_url, target_dir)
            flatten_success = False
            if success:
                # Flatten to markdown
                flattened_dir = downloader.output_dir / "flattened_repositories"
                flattened_dir.mkdir(parents=True, exist_ok=True)
                output_file = flattened_dir / f"single_{repo}.md"
                flatten_success = downloader.flatten_repository_to_markdown(target_dir, output_file)
            # Print summary
            print("\n🎉 Single repo process completed!")
            print(f"📊 Repo download: {'Success' if success else 'Failed'} | Flatten: {'Success' if flatten_success else 'Failed'}")
        elif args.gist_id:
            print(f"🚀 Downloading single gist: {args.gist_id}")
            gist_id = downloader.extract_gist_id(args.gist_id)
            if not gist_id:
                print(f"❌ Could not parse gist ID from: {args.gist_id}")
                sys.exit(1)
            gist_dir = downloader.output_dir / "gists"
            success = downloader.download_gist(gist_id, gist_dir, "Single Gist Download")
            flatten_success = False
            if success:
                # Find the downloaded gist directory
                gist_directories = list(gist_dir.glob(f"*_{gist_id}"))
                if gist_directories:
                    gist_path = gist_directories[0]
                    # Flatten to markdown
                    flattened_dir = downloader.output_dir / "flattened_repositories"
                    flattened_dir.mkdir(parents=True, exist_ok=True)
                    output_file = flattened_dir / f"single_gist_{gist_id}.md"
                    flatten_success = downloader.flatten_repository_to_markdown(gist_path, output_file)
            # Print summary
            print("\n🎉 Single gist process completed!")
            print(f"📊 Gist download: {'Success' if success else 'Failed'} | Flatten: {'Success' if flatten_success else 'Failed'}")
        else:
            downloader.run()

    except KeyboardInterrupt:
        print("\n⏹️  Download interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Unexpected error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()

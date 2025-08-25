#!/usr/bin/env python3
"""
Test script for subdirectory cloning functionality
"""

from github_downloader import GitHubDownloader
import shutil
from pathlib import Path

def test_subdirectory_cloning():
    # Create a test downloader
    downloader = GitHubDownloader(output_dir="test_output")
    
    # Test URL with subdirectory
    test_url = "https://github.com/dagster-io/dagster/tree/master/examples/docs_projects/project_atproto_dashboard"
    
    print(f"Testing URL: {test_url}")
    
    # Parse the URL
    base_url, branch, subdirectory = downloader.parse_github_url(test_url)
    print(f"Parsed - Base: {base_url}, Branch: {branch}, Subdirectory: {subdirectory}")
    
    # Get repository info
    repo_info = downloader.extract_repo_info(test_url)
    if repo_info:
        owner, repo = repo_info
        print(f"Repository info - Owner: {owner}, Repo: {repo}")
        
        # Get target directory
        target_dir = downloader.get_target_directory(owner, repo, test_url)
        print(f"Target directory: {target_dir}")
        
        # Clean up any existing test directory
        if target_dir.exists():
            print(f"Removing existing directory: {target_dir}")
            shutil.rmtree(target_dir)
        
        # Test the clone operation
        print("Attempting to clone subdirectory...")
        success = downloader.clone_repository(test_url, target_dir)
        
        if success:
            print("✅ Subdirectory cloning successful!")
            # List contents
            if target_dir.exists():
                print("Contents of cloned subdirectory:")
                for item in target_dir.rglob("*"):
                    if item.is_file():
                        print(f"  {item.relative_to(target_dir)}")
        else:
            print("❌ Subdirectory cloning failed!")
    
    # Clean up test directory
    test_output = Path("test_output")
    if test_output.exists():
        print(f"Cleaning up test directory: {test_output}")
        shutil.rmtree(test_output)

if __name__ == "__main__":
    test_subdirectory_cloning()

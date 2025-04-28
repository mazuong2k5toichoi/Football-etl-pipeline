import requests
from bs4 import BeautifulSoup
import re
import os
import json
import concurrent.futures
import pandas as pd
from typing import Dict, List, Tuple, Optional

class ClubScraper:
    """Utility class to scrape club names from Transfermarkt"""
    
    def __init__(self, cache_dir: str = None):
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
        }
        if cache_dir is None:
            # Default to the directory containing this file
            cache_dir = os.path.dirname(os.path.abspath(__file__))
        self.cache_file = os.path.join(cache_dir, "club_names_cache.json")
        self.club_names_cache = self._load_cache()
        
    def _load_cache(self) -> Dict[str, str]:
        """Load existing club name cache"""
        if os.path.exists(self.cache_file):
            try:
                with open(self.cache_file, 'r') as f:
                    return json.load(f)
            except (json.JSONDecodeError, IOError):
                return {}
        return {}
        
    def _save_cache(self) -> None:
        """Save club names to cache file"""
        try:
            os.makedirs(os.path.dirname(self.cache_file), exist_ok=True)
            with open(self.cache_file, 'w') as f:
                json.dump(self.club_names_cache, f)
        except IOError as e:
            print(f"Warning: Could not save cache file: {e}")
    
    def get_club_name(self, club_id: int) -> Tuple[int, Optional[str]]:
        """Scrape club name from Transfermarkt for a single club ID"""
        # First check the cache
        str_id = str(club_id)
        if str_id in self.club_names_cache:
            return (club_id, self.club_names_cache[str_id])
        
        url = f"https://www.transfermarkt.us/-/datenfakten/verein/{club_id}"
        
        try:
            response = requests.get(url, headers=self.headers, timeout=10)
            
            if response.status_code == 200:
                soup = BeautifulSoup(response.text, 'html.parser')
                meta_tag = soup.find('meta', property='og:title')
                
                if meta_tag and 'content' in meta_tag.attrs:
                    title_content = meta_tag['content']
                    team_name = re.sub(r' - Facts and data$', '', title_content)
                    # Update cache
                    self.club_names_cache[str_id] = team_name
                    return (club_id, team_name)
            
            return (club_id, None)
        except Exception as e:
            print(f"Error retrieving data for club ID {club_id}: {str(e)}")
            return (club_id, None)
    
    def scrape_club_names(self, club_ids: List[int], max_workers: int = 5) -> Dict[int, str]:
        """
        Scrape club names in parallel for multiple club IDs
        
        Args:
            club_ids: List of club IDs to scrape
            max_workers: Number of concurrent workers (default: 5)
            
        Returns:
            Dictionary mapping club IDs to club names
        """
        # Filter out IDs already in cache
        ids_to_scrape = [cid for cid in club_ids if str(cid) not in self.club_names_cache]
        
        if ids_to_scrape:
            print(f"Scraping {len(ids_to_scrape)} new club names (found {len(club_ids) - len(ids_to_scrape)} in cache)")
            
            results = {}
            with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
                future_to_id = {executor.submit(self.get_club_name, cid): cid for cid in ids_to_scrape}
                
                for future in concurrent.futures.as_completed(future_to_id):
                    club_id, club_name = future.result()
                    if club_name:
                        results[club_id] = club_name
                        
            # Save updated cache
            self._save_cache()
        else:
            print(f"All {len(club_ids)} club IDs found in cache")
            
        # Return all requested club names (including from cache)
        return {cid: self.club_names_cache.get(str(cid)) for cid in club_ids if str(cid) in self.club_names_cache}
    
    def get_club_names_df(self, club_ids: List[int], max_workers: int = 5) -> pd.DataFrame:
        """
        Get club names as a DataFrame
        
        Args:
            club_ids: List of club IDs to scrape
            max_workers: Number of concurrent workers (default: 5)
            
        Returns:
            DataFrame with club_id and club_name columns
        """
        club_names_dict = self.scrape_club_names(club_ids, max_workers)
        df = pd.DataFrame(list(club_names_dict.items()), columns=['club_id', 'club_name'])
        return df
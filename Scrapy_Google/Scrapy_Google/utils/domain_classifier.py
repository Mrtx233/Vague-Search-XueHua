import json
import logging
import tldextract
from urllib.parse import urlparse
from typing import Dict, List

logger = logging.getLogger(__name__)


class DomainClassifier:
    def __init__(self, config_path: str):
        self.config_path = config_path
        self.url_class_keywords = {}
        self._load_config()

    def _load_config(self) -> None:
        try:
            with open(self.config_path, 'r', encoding='utf-8') as f:
                config_data = json.load(f)
            if not isinstance(config_data, dict):
                raise ValueError("Config root must be a dict")
            for category, keywords in config_data.items():
                if not isinstance(keywords, list) or not all(isinstance(kw, str) for kw in keywords):
                    raise ValueError(f"Invalid keywords list for category '{category}'")
            self.url_class_keywords = config_data
            total_keywords = sum(len(kws) for kws in config_data.values())
            logger.info(f"Domain config loaded: {len(config_data)} categories, {total_keywords} keywords")
        except FileNotFoundError:
            logger.error(f"Domain config not found: {self.config_path}")
            self.url_class_keywords = {}
        except json.JSONDecodeError as e:
            logger.error(f"Domain config JSON error: {str(e)}")
            self.url_class_keywords = {}
        except Exception as e:
            logger.error(f"Domain config load failed: {str(e)}")
            self.url_class_keywords = {}

    def is_config_loaded(self) -> bool:
        return bool(self.url_class_keywords)

    def extract_domain_parts(self, url: str) -> Dict[str, str]:
        try:
            parsed_url = urlparse(url)
            full_host = parsed_url.hostname or ""
            if not full_host:
                ext = tldextract.extract(url)
                full_host = f"{ext.subdomain}.{ext.domain}.{ext.suffix}".lstrip('.')
            ext = tldextract.extract(url)
            return {
                "subdomain": ext.subdomain,
                "domain": ext.domain,
                "suffix": ext.suffix,
                "registered_domain": ext.registered_domain or ext.domain + '.' + ext.suffix if ext.domain and ext.suffix else "",
                "full_host": full_host
            }
        except Exception as e:
            logger.warning(f"Domain parse failed: {url} -> {str(e)[:30]}")
            return {"full_host": "", "subdomain": "", "domain": "", "suffix": "", "registered_domain": ""}

    def determine_domain_class(self, full_host: str, suffix: str) -> str:
        if not self.url_class_keywords:
            return ""
        domain_info = f"{full_host}.{suffix}".lower() if full_host else suffix.lower()
        for domain_class, keywords in self.url_class_keywords.items():
            for keyword in keywords:
                if keyword.lower() in domain_info:
                    return domain_class
        return ""

    def classify_url(self, url: str) -> Dict[str, str]:
        domain_info = self.extract_domain_parts(url)
        domain_class = self.determine_domain_class(domain_info["full_host"], domain_info["suffix"])
        return {**domain_info, "domain_class": domain_class}

    def reload_config(self) -> bool:
        self._load_config()
        return self.is_config_loaded()

    def get_categories(self) -> List[str]:
        return list(self.url_class_keywords.keys())

    def get_keywords_for_category(self, category: str) -> List[str]:
        return self.url_class_keywords.get(category, [])

import json
from typing import Iterable, Mapping
from src.corpus.base import DatasetLoader

# ---------------------------
# External web-source loaders
# ---------------------------

class WikipediaLoader(DatasetLoader):
    """Loads Wikipedia lead summaries and specific sections (e.g., Plot).
    Cache layout:
      ./datasets/wikipedia/metadata.csv  (book_id, wiki_title, title, summary, plot_path)
      ./datasets/texts/{id}_{title}.txt  (contains extracted plot or lead text)
    Notes:
      - download() accepts either a list of wikipedia titles or a number `n`.
      - If titles is None and n is provided, this will attempt to read the first `n` titles
        from the existing global index.csv (useful to bootstrap from previously indexed books).
    """
    REST_SUMMARY = "https://{lang}.wikipedia.org/api/rest_v1/page/summary/{title}"
    ACTION_API = "https://{lang}.wikipedia.org/w/api.php"

    def __init__(self, cache_dir: str = "./datasets/wikipedia", lang: str = "en"):
        self.cache_dir = cache_dir
        self.lang = lang
        self.metadata_file = f"{cache_dir}/metadata.csv"
        os.makedirs(self.cache_dir, exist_ok=True)

    def _fetch_summary(self, wiki_title: str) -> Optional[str]:
        url = self.REST_SUMMARY.format(lang=self.lang, title=wiki_title)
        try:
            res = requests.get(url, timeout=10)
            if res.status_code == 429:
                time.sleep(1)
                res = requests.get(url, timeout=10)
            res.raise_for_status()
            data = res.json()
            return data.get("extract", "")
        except Exception as e:
            print(f"Warning: Wikipedia summary fetch failed for {wiki_title}: {e}")
            return None

    def _fetch_section_wikitext(self, wiki_title: str, section_regex: str = r"==\s*Plot\s*==") -> Optional[str]:
        """Use action=parse to fetch wikitext and extract section matching regex label."""
        params = {
            "action": "parse",
            "page": wiki_title,
            "prop": "wikitext",
            "format": "json"
        }
        url = self.ACTION_API.format(lang=self.lang)
        try:
            res = requests.get(url, params=params, timeout=15)
            if res.status_code == 429:
                time.sleep(1)
                res = requests.get(url, params=params, timeout=15)
            res.raise_for_status()
            data = res.json()
            wikitext = data.get("parse", {}).get("wikitext", {}).get("*", "")
            # Extract section by header name (Plot, Summary, Synopsis, etc.)
            m = re.search(rf"==\s*(Plot|Synopsis|Summary|Plot summary)\s*==(.+?)(^==|\Z)", wikitext, flags=re.S | re.M | re.I)
            if m:
                return m.group(2).strip()
            # fallback: return the lead paragraph if no plot section
            m2 = re.search(r"^(.+?)(\n==|\Z)", wikitext, flags=re.S)
            return m2.group(1).strip() if m2 else None
        except Exception as e:
            print(f"Warning: Wikipedia section fetch failed for {wiki_title}: {e}")
            return None

    def download(self, titles: Iterable[str] = None, n: int = None, fraction: float = None) -> None:
        """Download Wikipedia pages for a list of titles.
        @param titles  Iterable of wiki page titles (e.g., ['Pride_and_Prejudice']). If None and n provided,
                       tries to read first n titles from global index.csv.
        @param n       Number of pages to download if titles is None.
        """
        os.makedirs(self.cache_dir, exist_ok=True)

        # Resolve titles list
        resolved_titles = []
        if titles:
            resolved_titles = list(titles)
        elif n is not None:
            # Try to get titles from index.csv
            if os.path.exists(DatasetLoader.INDEX_FILE):
                idx = read_csv(DatasetLoader.INDEX_FILE)
                resolved_titles = idx['title'].dropna().astype(str).tolist()[:n]
                # Titles in index are normalized (lowercase) so convert to wiki-style (underscores)
                resolved_titles = [t.replace(" ", "_") for t in resolved_titles]
            else:
                raise ValueError("No titles provided and index.csv not found to derive titles from.")
        else:
            raise ValueError("WikipediaLoader.download requires `titles` or `n` parameter.")

        rows = []
        for raw_title in resolved_titles:
            wiki_title = raw_title  # assume already in Title_With_Underscores format
            # fetch lead summary and section
            summary = self._fetch_summary(wiki_title) or ""
            plot_wikitext = self._fetch_section_wikitext(wiki_title) or ""
            # Save combined text to a file
            book_id = self._get_next_id()
            norm_title = normalize_title(wiki_title.replace("_", " "))
            text_content = "\n\n".join([summary, plot_wikitext]).strip()
            text_path = self._save_text(book_id, norm_title, text_content)
            # Save per-record metadata
            metadata_row = {
                "book_id": book_id,
                "wiki_title": wiki_title,
                "title": norm_title,
                "summary": summary,
                "plot_path": text_path
            }
            rows.append(metadata_row)
            # Append to global index (single path for Wikipedia stored in wiki_path)
            index_row = {
                "book_id": book_id,
                "title": norm_title,
                "gutenberg_id": None,
                "text_path": text_path,
                "booksum_id": None,
                "booksum_path": None,
                "nqa_id": None,
                "nqa_path": None,
                "litbank_id": None,
                "litbank_path": None
            }
            self._append_to_index(index_row)
            print(f"Downloaded Wikipedia: {wiki_title}", end="\r")
            time.sleep(0.35)  # polite pacing

        # Persist metadata
        df = DataFrame(rows)
        df.to_csv(self.metadata_file, index=False)
        print()

    def load(self, streaming: bool = False) -> DataFrame:
        if not os.path.exists(self.metadata_file):
            raise FileNotFoundError(f"Wikipedia cache not found at {self.metadata_file}. Run download() first.")
        return read_csv(self.metadata_file)

    def get_schema(self) -> list[str]:
        return ["book_id", "wiki_title", "title", "summary", "plot_path"]


class FandomLoader(DatasetLoader):
    """Loads pages from a Fandom wiki (MediaWiki API).
    Usage:
      - supply `subdomain` (e.g., 'harrypotter', 'lotr', 'starwars') and a list of page titles.
      - download() will fetch wikitext and optionally extract a named section (Plot/Characters).
    Cache:
      ./datasets/fandom/{subdomain}/metadata.csv  (book_id, subdomain, page_title, section, page_path)
    """
    ACTION_API = "https://{subdomain}.fandom.com/api.php"

    def __init__(self, cache_dir: str = "./datasets/fandom"):
        self.cache_dir = cache_dir
        os.makedirs(self.cache_dir, exist_ok=True)

    def _fetch_wikitext(self, subdomain: str, page_title: str) -> Optional[str]:
        url = self.ACTION_API.format(subdomain=subdomain)
        params = {
            "action": "parse",
            "page": page_title,
            "prop": "wikitext",
            "format": "json"
        }
        try:
            res = requests.get(url, params=params, timeout=15)
            if res.status_code == 429:
                time.sleep(1)
                res = requests.get(url, params=params, timeout=15)
            res.raise_for_status()
            return res.json().get("parse", {}).get("wikitext", {}).get("*", "")
        except Exception as e:
            print(f"Warning: Fandom fetch failed for {subdomain}/{page_title}: {e}")
            return None

    def _fetch_html(self, subdomain: str, page_title: str) -> Optional[str]:
        url = self.ACTION_API.format(subdomain=subdomain)
        params = {"action": "parse", "page": page_title, "prop": "text", "format": "json"}
        try:
            res = requests.get(url, params=params, timeout=15)
            if res.status_code == 429:
                time.sleep(1)
                res = requests.get(url, params=params, timeout=15)
            res.raise_for_status()
            return res.json().get("parse", {}).get("text", {}).get("*", "")
        except Exception as e:
            print(f"Warning: Fandom HTML fetch failed for {subdomain}/{page_title}: {e}")
            return None

    def download(self, subdomain: str = None, pages: Iterable[str] = None, section: str = "Characters") -> None:
        """Download pages from a single Fandom subdomain.
        @param subdomain  Fandom subdomain (required).
        @param pages      Iterable of page titles (case-sensitive title slugs, e.g., 'Harry_Potter').
        @param section    Section to extract (default 'Characters'). If missing, saves full wikitext.
        """
        if not subdomain:
            raise ValueError("FandomLoader.download requires `subdomain` parameter (e.g., 'harrypotter').")
        if not pages:
            raise ValueError("FandomLoader.download requires `pages` iterable parameter.")

        target_dir = os.path.join(self.cache_dir, subdomain)
        os.makedirs(target_dir, exist_ok=True)
        metadata_path = os.path.join(target_dir, "metadata.csv")

        rows = []
        for page_title in pages:
            wikitext = self._fetch_wikitext(subdomain, page_title) or ""
            html = self._fetch_html(subdomain, page_title) or ""
            # Try to extract the named section from wikitext
            m = re.search(rf"==\s*{re.escape(section)}\s*==(.+?)(^==|\Z)", wikitext, flags=re.S | re.M | re.I)
            section_text = m.group(1).strip() if m else ""
            # If section empty, fall back to searching lists in HTML
            if not section_text and html:
                # Quick HTML parse to extract <li> items under headers with the section name
                from bs4 import BeautifulSoup
                soup = BeautifulSoup(html, "html.parser")
                found = None
                for header in soup.find_all(['h2', 'h3', 'h4']):
                    if section.lower() in header.text.lower():
                        # collect following sibling list items up to next header
                        items = []
                        for sib in header.next_siblings:
                            if hasattr(sib, "name") and sib.name and sib.name.startswith("h"):
                                break
                            if hasattr(sib, "find_all"):
                                lis = sib.find_all("li")
                                for li in lis:
                                    a = li.find("a")
                                    if a and a.get("href", "").startswith("/wiki/"):
                                        items.append(a.text.strip())
                        if items:
                            found = "\n".join(items)
                            break
                section_text = found or ""

            # Save the section/full text to the global texts dir
            book_id = self._get_next_id()
            norm_title = normalize_title(page_title.replace("_", " "))
            content = section_text if section_text else wikitext
            text_path = self._save_text(book_id, norm_title, content)

            metadata_row = {
                "book_id": book_id,
                "subdomain": subdomain,
                "page_title": page_title,
                "section": section,
                "page_path": text_path
            }
            rows.append(metadata_row)

            index_row = {
                "book_id": book_id,
                "title": norm_title,
                "gutenberg_id": None,
                "text_path": text_path,
                "booksum_id": None,
                "booksum_path": None,
                "nqa_id": None,
                "nqa_path": None,
                "litbank_id": None,
                "litbank_path": None
            }
            self._append_to_index(index_row)
            print(f"Downloaded Fandom: {subdomain}/{page_title}", end="\r")
            time.sleep(0.35)

        # Persist metadata
        df = DataFrame(rows)
        df.to_csv(metadata_path, index=False)
        print()

    def load(self, streaming: bool = False) -> DataFrame:
        # Load combined metadata across all fandom subdirs
        rows = []
        if os.path.exists(self.cache_dir):
            for sd in os.listdir(self.cache_dir):
                md = os.path.join(self.cache_dir, sd, "metadata.csv")
                if os.path.exists(md):
                    rows.extend(read_csv(md).to_dict("records"))
        if not rows:
            raise FileNotFoundError(f"No Fandom metadata found under {self.cache_dir}. Run download() first.")
        return DataFrame(rows)

    def get_schema(self) -> list[str]:
        return ["book_id", "subdomain", "page_title", "section", "page_path"]


class WikidataLoader(DatasetLoader):
    """Loads entity lists (e.g., characters) from Wikidata via SPARQL.
    Behavior:
      - download() accepts either a list of Wikidata QIDs (e.g., 'Q181792') or a list of titles to search for.
      - For each work, it resolves a QID (if needed) and runs a SPARQL query to fetch all items that have P1269 (character in this narrative work) pointing to the work.
    Cache:
      ./datasets/wikidata/metadata.csv (book_id, wikidata_qid, title, characters_path)
      Characters file is newline-separated labels.
    """
    SPARQL_ENDPOINT = "https://query.wikidata.org/sparql"
    SEARCH_API = "https://www.wikidata.org/w/api.php"

    def __init__(self, cache_dir: str = "./datasets/wikidata", lang: str = "en"):
        self.cache_dir = cache_dir
        self.lang = lang
        self.metadata_file = f"{cache_dir}/metadata.csv"
        os.makedirs(self.cache_dir, exist_ok=True)

    def _search_qid(self, title: str) -> Optional[str]:
        params = {
            "action": "wbsearchentities",
            "format": "json",
            "language": self.lang,
            "search": title,
            "type": "item",
            "limit": 1
        }
        try:
            res = requests.get(self.SEARCH_API, params=params, timeout=10)
            res.raise_for_status()
            data = res.json()
            if data.get("search"):
                return data["search"][0].get("id")
        except Exception as e:
            print(f"Warning: Wikidata search failed for {title}: {e}")
        return None

    def _sparql_characters_for_work(self, qid: str) -> list[Mapping]:
        """Return list of dicts {'char_qid': 'Q...', 'label': 'Name'}"""
        # We assume P1269 is used (character in this narrative work) as in common usage.
        query = f"""
        SELECT ?char ?charLabel WHERE {{
          ?char wdt:P1269 wd:{qid} .
          SERVICE wikibase:label {{ bd:serviceParam wikibase:language "en". }}
        }}
        LIMIT 1000
        """
        headers = {"Accept": "application/sparql-results+json", "User-Agent": "dataset-ingest/1.0 (contact)"}
        try:
            res = requests.get(self.SPARQL_ENDPOINT, params={"query": query}, headers=headers, timeout=20)
            if res.status_code == 429:
                time.sleep(1)
                res = requests.get(self.SPARQL_ENDPOINT, params={"query": query}, headers=headers, timeout=20)
            res.raise_for_status()
            data = res.json()
            items = []
            for b in data.get("results", {}).get("bindings", []):
                uri = b.get("char", {}).get("value", "")
                q = uri.rsplit("/", 1)[-1] if "/" in uri else uri
                label = b.get("charLabel", {}).get("value", "")
                items.append({"char_qid": q, "label": label})
            return items
        except Exception as e:
            print(f"Warning: SPARQL query failed for {qid}: {e}")
            return []

    def download(self, qids: Iterable[str] = None, titles: Iterable[str] = None, n: int = None) -> None:
        """Download character lists from Wikidata for given qids or titles.
        @param qids   Iterable of Wikidata QIDs like ['Q181792'].
        @param titles Iterable of work titles (will be searched on Wikidata).
        @param n      If provided and titles/qids not provided, attempts to read n titles from index.csv.
        """
        to_process = []

        if qids:
            for q in qids:
                to_process.append((q, None))
        elif titles:
            for t in titles:
                to_process.append((None, t))
        elif n is not None:
            if os.path.exists(DatasetLoader.INDEX_FILE):
                idx = read_csv(DatasetLoader.INDEX_FILE)
                sample = idx['title'].dropna().astype(str).tolist()[:n]
                for t in sample:
                    to_process.append((None, t))
            else:
                raise ValueError("No qids/titles provided and index.csv not found.")
        else:
            raise ValueError("WikidataLoader.download requires `qids`, `titles`, or `n`.")

        rows = []
        for qid, title in to_process:
            resolved_qid = qid
            resolved_title = title
            if not resolved_qid and resolved_title:
                # convert normalized title to wiki-friendly search string
                resolved_qid = self._search_qid(resolved_title)
                time.sleep(0.2)

            if not resolved_qid:
                print(f"Skipping (no qid found) for title: {resolved_title}")
                continue

            # Query characters via SPARQL
            chars = self._sparql_characters_for_work(resolved_qid)
            labels = [c["label"] for c in chars] if chars else []

            book_id = self._get_next_id()
            norm_title = normalize_title(resolved_title or resolved_qid)
            # Save characters list as newline text
            content = "\n".join(labels)
            text_path = self._save_text(book_id, norm_title, content)

            metadata_row = {
                "book_id": book_id,
                "wikidata_qid": resolved_qid,
                "title": norm_title,
                "characters_path": text_path,
                "num_characters": len(labels)
            }
            rows.append(metadata_row)

            # Append to global index
            index_row = {
                "book_id": book_id,
                "title": norm_title,
                "gutenberg_id": None,
                "text_path": text_path,
                "booksum_id": None,
                "booksum_path": None,
                "nqa_id": None,
                "nqa_path": None,
                "litbank_id": None,
                "litbank_path": None
            }
            self._append_to_index(index_row)
            print(f"Downloaded Wikidata characters for {resolved_qid} ({len(labels)} chars)", end="\r")
            time.sleep(0.35)

        # Persist metadata
        df = DataFrame(rows)
        df.to_csv(self.metadata_file, index=False)
        print()

    def load(self, streaming: bool = False) -> DataFrame:
        if not os.path.exists(self.metadata_file):
            raise FileNotFoundError(f"Wikidata cache not found at {self.metadata_file}. Run download() first.")
        return read_csv(self.metadata_file)

    def get_schema(self) -> list[str]:
        return ["book_id", "wikidata_qid", "title", "characters_path", "num_characters"]

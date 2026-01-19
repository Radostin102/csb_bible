from pathlib import Path
import time
import logging
import requests
import re
import concurrent.futures
from dataclasses import dataclass, field
from typing import List, Optional
import warnings
from bs4 import BeautifulSoup, Tag, XMLParsedAsHTMLWarning
from rich.logging import RichHandler
from rich.console import Console
from rich.highlighter import NullHighlighter

logging.basicConfig(
    level=logging.INFO,
    format="%(message)s",
    datefmt="[%X]",
    handlers=[RichHandler(rich_tracebacks=True, highlighter=NullHighlighter())]
)
logger = logging.getLogger("rich")

@dataclass(frozen=True)
class BibleConfig:
    """ Configuration constants for the Bible application. """
    base_url: str = "https://read.csbible.com/wp-content/themes/lwcsbread/CSB_XML//"

    script_dir: Path = Path(__file__).resolve().parent
    xml_folder: Path = script_dir / "csb_xml_files"
    output_file: Path = script_dir / "CSB_Full_Bible.txt"

    max_workers: int = 10
    timeout: int = 10
    headers: dict = field(default_factory=lambda: {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    })
    books: List[str] = field(default_factory=lambda: [
        "Gen", "Ex", "Lev", "Num", "Deut",
        "Josh", "Jdg", "Ruth", "1Sam", "2Sam",
        "1Ki", "2Ki", "1Chr", "2Chr", "Ezra",
        "Neh", "Esth", "Job", "Ps", "Prov",
        "Eccl", "Song", "Isa", "Jer", "Lam",
        "Ezek", "Dan", "Hos", "Joel", "Amos",
        "Ob", "Jonah", "Mic", "Nah", "Hab",
        "Zeph", "Hag", "Zech", "Mal", "Matt",
        "Mark", "Luke", "John", "Acts", "Rom",
        "1Cor", "2Cor", "Gal", "Eph", "Phil",
        "Col", "1Thes", "2Thes", "1Tim", "2Tim",
        "Titus", "Phlm", "Heb", "Jas", "1Pet",
        "2Pet", "1John", "2John", "3John", "Jude",
        "Rev"
    ])

    @property
    def xml_filenames(self) -> List[str]:
        """ Generates the expected XML filenames based on the book list. """
        return [f"{i:02d}-{book}.xml" for i, book in enumerate(self.books, start=1)]


class BibleDownloader:
    """ Handles downloading of Bible XML files. """

    def __init__(self, config: BibleConfig):
        self.config = config
        self.session = requests.Session()
        self.session.headers.update(self.config.headers)
        self._ensure_directory(self.config.xml_folder)

    def _ensure_directory(self, path: Path) -> None:
        """ Ensures that the output directory exists. """
        if not path.exists():
            path.mkdir(parents=True, exist_ok=True)
            logger.info(f"Created directory: {path}")

    def download_all(self) -> None:
        """ Downloads all Bible XML files concurrently. """
        logger.info(f"Starting concurrent download of {len(self.config.books)} files")

        with concurrent.futures.ThreadPoolExecutor(max_workers=self.config.max_workers) as executor:
            executor.map(self._download_file, self.config.xml_filenames)

    def _download_file(self, filename: str) -> None:
        """ Downloads a single file if it doesn't already exist. """
        file_path: Path = self.config.xml_folder / filename

        if file_path.exists():
            logger.info(f"Skipping {filename} (already exists)")
            return

        url: str = self.config.base_url + filename
        try:
            response: requests.Response = self.session.get(url, timeout=self.config.timeout)
            if response.status_code == 200:
                with open(file_path, "wb") as f:
                    f.write(response.content)
                logger.info(f"Downloaded: {filename}")
            else:
                logger.error(f"Failed to download {filename}: Status {response.status_code}")
        except requests.RequestException as e:
            logger.error(f"Network error downloading {filename}: {e}")
        except Exception:
            logger.exception(f"Unexpected error downloading {filename}")


class BibleParser:
    """ Handles parsing of XML files and conversion to text. """

    def __init__(self, config: BibleConfig, console: Console):
        self.config = config
        self.console = console

    @staticmethod
    def clean_text(text: str) -> str:
        """ Normalizes whitespace and replaces smart quotes. """
        text = text.replace("“", '"').replace("”", '"')
        text = text.replace("‘", "'").replace("’", "'")
        text = text.replace("\u2009", "")
        text = re.sub(r"\s+", " ", text)
        return text.strip()

    def process_all(self) -> None:
        """ Converts all downloaded XML files to a single text file concurrently. """
        logger.info("Starting concurrent conversion to text")
        start_time: float = time.perf_counter()

        results: List[Optional[str]] = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.config.max_workers) as executor:
            results = list(executor.map(self._process_xml_file, self.config.xml_filenames))

        self._write_output(results, start_time)

    def _process_xml_file(self, filename: str) -> Optional[str]:
        """ Parses a single XML file and returns its formatted text content. """
        file_path = self.config.xml_folder / filename

        if not file_path.exists():
            logger.warning(f"File missing, skipping: {filename}")
            return None

        try:
            with open(file_path, "r", encoding="utf-8") as f:
                content = f.read()

            try:
                soup = BeautifulSoup(content, features="xml")
            except Exception:
                with warnings.catch_warnings():
                    warnings.filterwarnings("ignore", category=XMLParsedAsHTMLWarning)
                    soup = BeautifulSoup(content, features="html.parser")

            book_name_tag = soup.find("bookname")
            if not book_name_tag:
                logger.warning(f"No book name found in {filename}")
                return None

            book_name = book_name_tag.get_text()
            logger.info(f"Processed: {book_name}")

            chapters = []
            for chapter in soup.find_all("chapter"):
                chapters.append(self._process_chapter(chapter, book_name))

            return "".join(chapters)

        except Exception as e:
            logger.error(f"Error parsing {filename}: {e}")
            return None

    def _process_chapter(self, chapter_tag: Tag, book_name: str) -> str:
        """ Processes a chapter tag and returns formatted string of verses. """
        chap_num = chapter_tag.get("display")
        chapter_lines: List[str] = []

        for verse in chapter_tag.find_all("verse"):
            verse_num = verse.get("display-number")

            for sup in verse.find_all("sup"):
                sup.decompose()

            for smallcap in verse.find_all("span", class_="smallcaps"):
                smallcap.string = smallcap.get_text().upper()

            verse_content = self.clean_text(verse.get_text())

            if verse_content:
                line = f"{book_name} {chap_num}:{verse_num}    {verse_content}".rstrip() + "\n"
                chapter_lines.append(line)

        return "".join(chapter_lines) + "\n"

    def _write_output(self, results: List[Optional[str]], start_time: float) -> None:
        """ Writes the accumulated results to the final output file. """
        try:
            count = 0
            with open(self.config.output_file, "w", encoding="utf-8") as out_file:
                out_file.write("OLD TESTAMENT\n\n\n\n")

                for i, content in enumerate(results):
                    if content:
                        book_name = self.config.books[i]

                        if book_name == "Matt":
                            if count > 0:
                                out_file.write("\n\n\n\n")
                            out_file.write("NEW TESTAMENT\n\n\n\n")
                        elif count > 0:
                            out_file.write("\n\n")

                        out_file.write(content.rstrip())
                        count += 1

                out_file.write("\n")

            elapsed: float = time.perf_counter() - start_time
            if count == 0:
                logger.error("No content was written to the file! Check XML parsing.")
            else:
                self.console.print(f"[bold green]SUCCESS![/bold green] Full Bible saved to: [bold]{self.config.output_file}[/bold]")
                logger.info(f"Conversion took {elapsed:.2f} seconds.")

        except IOError as e:
            logger.error(f"Error writing final file: {e}")


class BibleApp:
    """ Main application controller. """

    def __init__(self) -> None:
        self.config = BibleConfig()
        self.console = Console(highlight=False)
        self.downloader = BibleDownloader(self.config)
        self.parser = BibleParser(self.config, self.console)

    def run(self) -> None:
        """ Runs the download and conversion process. """
        self.downloader.download_all()
        self.parser.process_all()

        downloaded_bytes: int = sum(f.stat().st_size for f in self.config.xml_folder.glob("*.xml") if f.is_file())
        output_file_bytes: int = self.config.output_file.stat().st_size if self.config.output_file.exists() else 0

        downloaded_mb: float = downloaded_bytes / 1_000_000
        output_file_mb: float = output_file_bytes / 1_000_000

        logger.info(f"Total data downloaded: {downloaded_mb:,.2f} MB")
        logger.info(f"Final {self.config.output_file.name} size: {output_file_mb:,.2f} MB")


if __name__ == "__main__":
    app = BibleApp()
    app.run()

from functools import partial
import urllib.parse
from dataclasses import dataclass
from typing import Callable, List, Optional

from bs4 import BeautifulSoup, Comment, Tag
from bs4.element import PageElement

from traider.platforms.parsers.webpage_helper.common import (
    filter_and_deduplicate_lines,
    get_footer_content,
    get_header_content,
    get_main_content,
    get_navigation_content,
    get_sidebar_content,
    is_irrelevant_link,
    remove_comments,
    remove_unwanted_tags,
    remove_white_spaces,
)

# Set maximum indentation level to prevent excessive indentation
_MAX_INDENT = 5
_MAIN_CONTENT = "main_content"
_CONTENT_TYPES = {
    "main_content": get_main_content,
    "header_content": get_header_content,
    "footer_content": get_footer_content,
    "navigation_content": get_navigation_content,
    "sidebar_content": get_sidebar_content,
}


@dataclass
class StructuredContent:
    main_content: str | None = None
    header_content: str | None = None
    footer_content: str | None = None
    navigation_content: str | None = None
    sidebar_content: str | None = None


def simple_text_extractor_base(
    html: str,
    base_url: str | None = None,
    min_characters: int = 3,
) -> StructuredContent | None:
    """
    Extracts both main content and side content (header, footer, navigation, sidebar)
    from HTML and returns them as a StructuredContent instance.

    Args:
        html: The HTML content to process
        base_url: Optional base URL for resolving relative links
        min_characters: Minimum character length for included text lines

    Returns:
        StructuredContent instance containing different types of extracted text
    """
    soup = BeautifulSoup(html, "lxml")
    remove_unwanted_tags(soup)
    remove_comments(soup)

    # Initialize the content structures
    content_structures = {key: [] for key in _CONTENT_TYPES}

    # Extract content
    for content_type, get_content in _CONTENT_TYPES.items():
        elements = get_content(soup)
        if content_type == _MAIN_CONTENT:
            process_element(
                elements,
                content_structures[content_type],
                base_url,
                content_type=content_type,
            )

            # Retry main content if needed
            if len(content_structures[content_type]) < 5 and elements != soup:
                content_structures[content_type] = []
                process_element(
                    soup.body or soup,
                    content_structures[content_type],
                    base_url,
                    content_type=content_type,
                )
        else:
            for element in elements:
                process_element(
                    element,
                    content_structures[content_type],
                    base_url,
                    content_type=content_type,
                )

    # Filter content
    filtered_content = {
        key: "\n".join(
            filter_and_deduplicate_lines(lines, min_character_length=min_characters)
        )
        or None
        for key, lines in content_structures.items()
    }

    # Return response
    if not any(filtered_content.values()):
        return None

    return StructuredContent(**filtered_content)


def process_element(
    element: PageElement | str,
    structured_lines: List[str],
    base_url: str | None,
    indent_level: int = 0,
    max_indent: int = _MAX_INDENT,
    content_type: str | None = _MAIN_CONTENT,
):
    """
    Processes a DOM element to extract structured text and links.

    Args:
        element (PageElement | str): The DOM element or text to process.
        structured_lines (List[str]): The list to store structured text lines.
        base_url (str | None): The base URL for resolving relative links.
        indent_level (int): The current indentation level.
        max_indent (int): The maximum allowed indentation level.
        content_type (str | None: The type of content being processed.
    """
    indent_level = min(indent_level, max_indent)

    if isinstance(element, Comment):
        return

    if isinstance(element, str):
        if _handle_text_element(element, structured_lines, indent_level):
            return
    elif isinstance(element, Tag):
        if _handle_link_element(element, structured_lines, base_url, indent_level):
            return

        if _handle_heading_element(element, structured_lines):
            return

        if content_type == _MAIN_CONTENT and _is_navigation_element(element):
            return

        for child in element.children:
            process_element(
                child,
                structured_lines,
                base_url,
                _new_indent(element, indent_level),
                content_type=content_type,
            )


def _append_structured_line(
    structured_lines: List[str],
    text: str,
    indent_level: int,
    url: Optional[str] = None,
) -> None:
    line = f"{' ' * indent_level}{text}"
    if url:
        line += f" [URL: {url}]"
    structured_lines.append(line)


def _handle_text_element(
    element: str, structured_lines: List[str], indent_level: int
) -> bool:
    if not isinstance(element, str) or not element.strip():
        return False

    text = remove_white_spaces(element)
    if not text or len(text) < 3:
        return True

    _append_structured_line(structured_lines, text, indent_level)
    return True


def _handle_link_element(
    element: Tag,
    structured_lines: List[str],
    base_url: Optional[str],
    indent_level: int,
) -> bool:
    if not (element.name == "a" and element.get("href")):
        return False

    link_text = remove_white_spaces(element.get_text())
    href_val = element.get("href")
    href = "".join(href_val) if isinstance(href_val, list) else href_val

    if not link_text or len(link_text) < 3 or is_irrelevant_link(href):
        return True

    if (
        base_url
        and href
        and not href.startswith(("http://", "https://", "mailto:", "tel:"))
    ):
        href = urllib.parse.urljoin(base_url, href)

    _append_structured_line(structured_lines, link_text, indent_level, href)
    return True


def _handle_heading_element(element: Tag, structured_lines: List[str]) -> bool:
    if element.name not in ["h1", "h2", "h3", "h4", "h5", "h6"]:
        return False

    heading_text = remove_white_spaces(element.get_text())
    if heading_text:
        heading_level = int(element.name[1])
        heading_indent = min(heading_level - 1, 2)
        _append_structured_line(structured_lines, heading_text, heading_indent)
    return True


def _is_navigation_element(element: Tag) -> bool:
    """
    Determines if an element is a navigation element (header, footer, menu).
    This function is now used to identify side content elements.
    """
    if element.name in ["nav", "footer", "header"]:
        return True

    for attr in ["id", "class"]:
        value = element.get(attr)
        if value:
            value = " ".join(value) if isinstance(value, list) else value
            if any(
                term in value.lower()
                for term in ["nav", "footer", "header", "menu", "sidebar"]
            ):
                return True
    return False


def _new_indent(element: Tag, indent_level: int) -> int:
    return (
        indent_level + 1
        if element.name
        in [
            "div",
            "p",
            "section",
            "article",
            "li",
            "blockquote",
            "table",
            "tr",
            "td",
        ]
        else indent_level
    )


simple_text_extractor = partial(simple_text_extractor_base, min_characters=1)

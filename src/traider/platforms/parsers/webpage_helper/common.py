import json
import re
import logging
from typing import List, Optional, Tuple
logger = logging.getLogger(__name__)

from bs4 import BeautifulSoup, Comment, Tag
from bs4.element import PageElement

def remove_white_spaces(s: str) -> str:
    return ' '.join(s.split())

INCLUDED_TAGS = {
    # Primary structure
    'article',
    'main',
    'section',
    'div',
    # List structures
    'ul',
    'ol',
    'li',
    'dl',
    'dt',
    'dd',
    # Text content
    'p',
    'span',
    'blockquote',
    'pre',
    'code',
    # Headers
    'h1',
    'h2',
    'h3',
    'h4',
    'h5',
    'h6',
    # Tables
    'table',
    'thead',
    'tbody',
    'tr',
    'td',
    'th',
    # Other semantic elements
    'figure',
    'figcaption',
    'details',
    'summary',
    # Text formatting
    'em',
    'strong',
    'b',
    'i',
    'mark',
    'small',
    # Rich content
    'time',
    'address',
    'cite',
    'q',
}

HEADER_TAGS = {'h1', 'h2', 'h3', 'h4', 'h5', 'h6', 'header'}

NEGATIVE_PATTERNS = re.compile(
    r'nav|footer|header|sidebar|ads|comment|promo|advert|social|share', re.I
)

# Set of tokens to remove
_NOISE = {
    'ccp',
    'up',
    '↑',
    '▲',
    '⬆️',
    'a',
    'an',
    'at',
    'by',
    'in',
    'of',
    'on',
    'to',
    'the',
}

_STOP_WORDS = {
    'a',
    'an',
    'and',
    'are',
    'as',
    'at',
    'be',
    'by',
    'for',
    'from',
    'has',
    'he',
    'in',
    'is',
    'it',
    'its',
    'of',
    'on',
    'that',
    'the',
    'to',
    'was',
    'were',
    'will',
    'with',
    # Pronouns
    'i',
    'you',
    'he',
    'she',
    'it',
    'we',
    'they',
    'me',
    'him',
    'her',
    'us',
    'them',
    'my',
    'your',
    'his',
    'her',
    'its',
    'our',
    'their',
    'mine',
    'yours',
    'hers',
    'ours',
    'theirs',
    'myself',
    'yourself',
    'himself',
    'herself',
    'itself',
    'ourselves',
    'themselves',
    # Common verbs
    'am',
    'is',
    'are',
    'was',
    'were',
    'be',
    'been',
    'being',
    'have',
    'has',
    'had',
    'having',
    'do',
    'does',
    'did',
    'doing',
    # Prepositions
    'about',
    'above',
    'across',
    'after',
    'against',
    'along',
    'among',
    'around',
    'at',
    'before',
    'behind',
    'below',
    'beneath',
    'beside',
    'between',
    'beyond',
    'by',
    'down',
    'during',
    'except',
    'for',
    'from',
    'in',
    'inside',
    'into',
    'near',
    'of',
    'off',
    'on',
    'out',
    'outside',
    'over',
    'past',
    'through',
    'to',
    'toward',
    'under',
    'underneath',
    'until',
    'up',
    'upon',
    'with',
    'within',
    # Conjunctions
    'and',
    'but',
    'or',
    'nor',
    'for',
    'yet',
    'so',
    'although',
    'because',
    'since',
    'unless',
    # Articles
    'a',
    'an',
    'the',
    # Other common words
    'this',
    'that',
    'these',
    'those',
    'what',
    'which',
    'who',
    'whom',
    'whose',
    'when',
    'where',
    'why',
    'how',
    'all',
    'any',
    'both',
    'each',
    'few',
    'more',
    'most',
    'other',
    'some',
    'such',
    'can',
    'cannot',
    "can't",
    'could',
    "couldn't",
    'may',
    'might',
    'must',
    "mustn't",
    'shall',
    'should',
    "shouldn't",
    'will',
    "won't",
    'would',
    "wouldn't",
    'not',
    "n't",
    'no',
    'nor',
    'none',
}

_EXCLUDED_TAGS = {
    "script",
    "style",
    "noscript",
    "iframe",
    "object",
    "embed",
    "applet",
    # "head",
    "meta",
    "frameset",
    "frame",
    "noframes",
    "param",
    "canvas",
    "svg",
    "video",
    "audio",
    "source",
    "track",
    "form",
    "input",
    "button",
    "select",
    "option",
    "textarea",
    # "footer",
    # "nav",
    "aside",
}

_EXCLUDED_ATTRS = {
    'style',
    'onclick',
    'onmouseover',
    'align',
    'bgcolor',
    'class',
    'id',
}

_EXCLUDE_PATTERN = re.compile(
    r'cookie|all rights reserved|privacy policy|copyright|sitemap|disclaimer',
    re.IGNORECASE,
)

_IRRELEVANT_LINK_PATTERNS = [
    r'^javascript:',  # JavaScript links
    r'^#',  # Fragment links
    r'^$',  # Empty links
]


def get_main_content(soup: BeautifulSoup) -> PageElement:
    main_content = soup.find(
        ['main', 'article', 'div', 'section'],
        id=lambda x: bool(x and ('content' in x.lower() or 'main' in x.lower())),
    )

    if not main_content:
        main_content = soup.find(
            ['main', 'article', 'div', 'section'],
            class_=lambda x: bool(x and ('content' in x.lower() or 'main' in x.lower())),
        )

    if not main_content:
        main_content = soup.body or soup

    return main_content


def get_header_content(soup: BeautifulSoup) -> List[PageElement]:
    """
    Extracts header elements from the soup.

    Args:
        soup: BeautifulSoup object representing the HTML document

    Returns:
        List of BeautifulSoup elements representing headers
    """
    elements = []

    # Find elements with 'header' tag
    header = soup.find('header')
    if header:
        elements.append(header)

    # Find elements with header-related class or id
    for attr in ['id', 'class']:
        for term in ['header', 'heading', 'top-bar']:
            for elem in soup.find_all(
                attrs={attr: lambda x: bool(x and term in x.lower())}
            ):
                if elem not in elements:
                    elements.append(elem)

    return elements


def get_footer_content(soup: BeautifulSoup) -> List[PageElement]:
    """
    Extracts footer elements from the soup.

    Args:
        soup: BeautifulSoup object representing the HTML document

    Returns:
        List of BeautifulSoup elements representing footers
    """
    elements = []

    # Find elements with 'footer' tag
    footer = soup.find('footer')
    if footer:
        elements.append(footer)

    # Find elements with footer-related class or id
    for attr in ['id', 'class']:
        for term in ['footer', 'bottom', 'copyright']:
            for elem in soup.find_all(
                attrs={attr: lambda x: bool(x and term in x.lower())}
            ):
                if elem not in elements:
                    elements.append(elem)

    return elements


def get_navigation_content(soup: BeautifulSoup) -> List[PageElement]:
    """
    Extracts navigation elements from the soup.

    Args:
        soup: BeautifulSoup object representing the HTML document

    Returns:
        List of BeautifulSoup elements representing navigation sections
    """
    elements = []

    # Find elements with 'nav' tag
    navs = soup.find_all('nav')
    for nav in navs:
        elements.append(nav)

    # Find elements with navigation-related class or id
    for attr in ['id', 'class']:
        for term in ['nav', 'menu', 'navigation']:
            for elem in soup.find_all(
                attrs={attr: lambda x: bool(x and term in x.lower())}
            ):
                if elem not in elements:
                    elements.append(elem)

    return elements


def get_sidebar_content(soup: BeautifulSoup) -> List[PageElement]:
    """
    Extracts sidebar elements from the soup.

    Args:
        soup: BeautifulSoup object representing the HTML document

    Returns:
        List of BeautifulSoup elements representing sidebars
    """
    elements = []

    # Find elements with sidebar-related class or id
    for attr in ['id', 'class']:
        for term in ['sidebar', 'side', 'aside']:
            for elem in soup.find_all(
                attrs={attr: lambda x: bool(x and term in x.lower())}
            ):
                if elem not in elements:
                    elements.append(elem)

    # Find aside elements
    asides = soup.find_all('aside')
    for aside in asides:
        if aside not in elements:
            elements.append(aside)

    return elements


def is_irrelevant_link(href: str | None) -> bool:
    if not href:
        return True

    for pattern in _IRRELEVANT_LINK_PATTERNS:
        if re.match(pattern, href):
            return True

    return False


def filter_and_deduplicate_lines(
    lines: List[str], min_character_length: int = 3
) -> List[str]:
    _filtered_lines = [
        l
        for l in lines
        if len(l.strip()) > min_character_length
        and not _EXCLUDE_PATTERN.search(l)
    ]
    deduplicated_lines = _prune_duplicate_lines(_filtered_lines)
    return deduplicated_lines


def _prune_duplicate_lines(lines: List[str]) -> List[str]:
    return list(dict.fromkeys(lines))


def extract_page_query(
    user_query: Optional[str], soup: BeautifulSoup, body: Tag
) -> str:
    """Common method to extract page metadata with fallbacks"""
    if user_query:
        return user_query

    query_parts: List[str] = []

    # Title
    try:
        if soup.title and soup.title.string:
            query_parts.append(soup.title.string)
    except Exception:
        pass

    h1_tag = soup.find('h1')
    if h1_tag:
        query_parts.append(h1_tag.get_text())

    # Meta tags
    temp = ""
    for meta_name in ['keywords', 'description']:
        meta = soup.find('meta', attrs={'name': meta_name})
        if isinstance(meta, Tag) and meta.get('content'):
            content = meta.get('content')
            if isinstance(content, list):
                content = ' '.join(content)
            if content:
                query_parts.append(content)
                temp += content

    # If still empty, grab first significant paragraph
    if not temp:
        # Find the first tag P for which the text contains more than 50 characters
        for p in body.find_all('p'):
            if len(p.get_text()) > 150:
                query_parts.append(p.get_text()[:150])
                break

    return ' '.join(filter(None, query_parts))


def clean_tokens(tokens: list[str]) -> list[str]:
    return [
        token
        for token in tokens
        if len(token) > 2
        and token not in _NOISE
        and token not in _STOP_WORDS
        and not token.startswith('↑')
        and not token.startswith('▲')
        and not token.startswith('⬆')
    ]


def clean_element(tag: Tag) -> str:
    if not tag or not isinstance(tag, Tag):
        return ""

    builder = []

    def render_tag(elem):
        if not isinstance(elem, Tag):
            if isinstance(elem, str):
                builder.append(elem.strip())
            return

        if elem.name in _EXCLUDED_TAGS:
            return

        builder.append(f'<{elem.name}')

        attrs = {
            k: v for k, v in elem.attrs.items() if k not in _EXCLUDED_ATTRS
        }
        for key, value in attrs.items():
            builder.append(f' {key}="{value}"')

        builder.append('>')

        for child in elem.children:
            render_tag(child)

        builder.append(f'</{elem.name}>')

    try:
        render_tag(tag)
        return ''.join(builder)
    except Exception:
        return str(tag)


def remove_comments(soup: BeautifulSoup):
    for element in soup(text=lambda text: isinstance(text, Comment)):
        element.extract()


def remove_unwanted_tags(soup: BeautifulSoup):
    for tag in _EXCLUDED_TAGS:
        for element in soup.find_all(tag):
            element.decompose()


def load_from_json(file_path: str) -> Optional[Tuple[str, str]]:
    try:
        with open(file_path, 'r') as f:
            data = json.load(f)
            if isinstance(data, dict) and 'body' in data:
                return (
                    data['body'],
                    data.get('url', ''),
                )
            else:
                logger.warning("The JSON file does not contain a 'body' key.")
                return None
    except FileNotFoundError:
        logger.error("File not found at %s", file_path)
        return None
    except json.JSONDecodeError:
        logger.error("Invalid JSON format in %s", file_path)
        return None
    except Exception as e:
        logger.exception("An unexpected error occurred while loading JSON: %s", e)
        return None

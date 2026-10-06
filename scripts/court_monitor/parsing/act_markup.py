"""Полный текст документа с абзацами, без соседних вкладок и подвала."""
from html.parser import HTMLParser
import re

from court_monitor.act_preparation import normalize_text


class _Document(HTMLParser):
    def __init__(self, mode):
        super().__init__(convert_charrefs=True)
        self.mode = mode
        self.depth = 0
        self.root = ''
        self.done = False
        self.hidden = []
        self.embedded_body = False
        self.parts = []

    def handle_starttag(self, tag, attrs):
        if self.done:
            return
        attrs = dict(attrs)
        if not self.depth:
            match = (tag == 'div' and attrs.get('id', '').lower() == 'cont_doc1')
            if self.mode == 'act':
                match = tag == 'div' and any(c == 'act' or c.startswith('act_') or c.startswith('act-') for c in attrs.get('class', '').lower().split())
            if self.mode == 'body':
                match = tag == 'body'
            if match:
                self.root, self.depth = tag, 1
            return
        if tag == 'body':
            self.embedded_body = True
        if tag == self.root:
            self.depth += 1
        if tag in ('script', 'style', 'nav', 'footer', 'button'):
            self.hidden.append(tag)
        if not self.hidden and tag in ('p', 'br', 'div', 'tr', 'li', 'h1', 'h2', 'h3'):
            self.parts.append('\n')

    def handle_endtag(self, tag):
        if not self.depth or self.done:
            return
        if tag == 'body' and self.embedded_body:
            self.depth, self.done = 0, True
            return
        if tag in self.hidden:
            self.hidden.remove(tag)
        if tag == self.root:
            self.depth -= 1
            self.done = not self.depth
        if not self.hidden:
            if tag in ('p', 'div', 'tr', 'li', 'h1', 'h2', 'h3'):
                self.parts.append('\n')
            elif tag in ('td', 'th'):
                self.parts.append(' | ')

    def handle_data(self, text):
        if self.depth and not self.hidden and not self.done:
            self.parts.append(text)


def _extract(html, mode):
    doc = _Document(mode)
    doc.feed(html or '')
    return re.sub(r'\n{3,}', '\n\n', normalize_text(''.join(doc.parts))) if doc.done else ''


def document_div_text(html):
    return _extract(html, 'document')


def act_page_text(html):
    """Предпочитаем явную границу. Body допустим для отдельной печатной страницы."""
    return document_div_text(html) or _extract(html, 'act') or _extract(html, 'body')

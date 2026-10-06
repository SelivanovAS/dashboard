"""Изоляция встроенного документа от соседних вкладок и подвала портала."""
from html.parser import HTMLParser
import re


def document_div_text(html):
    class Document(HTMLParser):
        def __init__(self):
            super().__init__(convert_charrefs=True)
            self.depth = 0
            self.done = False
            self.hidden = 0
            self.embedded_body = False
            self.parts = []

        def handle_starttag(self, tag, attrs):
            if self.done:
                return
            if not self.depth:
                if tag == 'div' and dict(attrs).get('id', '').lower() == 'cont_doc1':
                    self.depth = 1
                return
            if tag == 'body':
                self.embedded_body = True
            if tag == 'div':
                self.depth += 1
            if tag in ('script', 'style'):
                self.hidden += 1
            if tag in ('p', 'br', 'div', 'tr'):
                self.parts.append(' ')

        def handle_endtag(self, tag):
            if not self.depth or self.done:
                return
            if tag == 'body' and self.embedded_body:
                self.depth = 0
                self.done = True
                return
            if tag in ('script', 'style'):
                self.hidden = max(0, self.hidden - 1)
            if tag == 'div':
                self.depth -= 1
                self.done = not self.depth
            if tag in ('p', 'td', 'tr'):
                self.parts.append(' ')

        def handle_data(self, text):
            if self.depth and not self.hidden and not self.done:
                self.parts.append(text)

    doc = Document()
    doc.feed(html or '')
    return re.sub(r'\s+', ' ', ''.join(doc.parts)).strip() if doc.done else ''

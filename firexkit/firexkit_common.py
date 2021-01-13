import os

from jinja2 import Environment, FileSystemLoader, select_autoescape

JINJA_ENV = Environment(
    # Cannot use PackageLoader because we override pkg_resources for module load speed.
    loader=FileSystemLoader(os.path.join(os.path.dirname(__file__), 'templates')),
    autoescape=select_autoescape(['html', 'xml']),
)


def get_link(url, text=None, html_class=None, title_attribute=None, attrs=None, other_elements=''):
    """Creates an html anchor."""
    if attrs is None:
        attrs = {}
    return JINJA_ENV.get_template('link.html').render(
        url=url,
        text=text,
        html_class=html_class,
        title_attribute=title_attribute,
        attrs=attrs,
        other_elements=other_elements
    )

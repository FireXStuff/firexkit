import os

from jinja2 import Environment, FileSystemLoader


def get_resource_filepath(resources_dir, filename, relative_from=None):
    filepath = os.path.join(resources_dir, filename)
    if relative_from:
        filepath = os.path.relpath(filepath, relative_from)
    return filepath


def get_firex_css_filepath(resources_dir, relative_from=None):
    return get_resource_filepath(resources_dir, 'firex.css', relative_from=relative_from)


def get_firex_logo_filepath(resources_dir, relative_from=None):
    return get_resource_filepath(resources_dir, 'firex_logo.png', relative_from=relative_from)


JINJA_ENV = Environment(
    loader=FileSystemLoader(os.path.join(os.path.dirname(__file__), 'templates'))
)

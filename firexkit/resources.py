import os

from jinja2 import Environment, FileSystemLoader


def get_firex_css_filepath(resources_dir):
    return os.path.join(resources_dir, 'firex.css')


def get_firex_logo_filepath(resources_dir):
    return os.path.join(resources_dir, 'firex_logo.png')


JINJA_ENV = Environment(
    loader=FileSystemLoader(os.path.join(os.path.dirname(__file__), 'templates'))
)
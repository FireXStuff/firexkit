from setuptools import setup
import os

# Determine the build number
build_file = os.path.join(os.path.dirname(__file__), "BUILD")
if os.path.exists(build_file):
    with open(build_file) as f:
        version_num = f.read()
else:
    version_num = "dev"

setup(name='firexkit',
      version='0.1.' + version_num,
      description='Core firex libraries',
      url='https://github.com/FireXStuff/firexkit',
      author='Core FireX Team',
      author_email='firex-dev@gmail.com',
      license='BSD-3-Clause',
      packages=['firexkit', ],
      zip_safe=True,
      install_requires=[
        "celery == 4.2.1",
      ],)

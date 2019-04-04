from setuptools import setup
import versioneer


setup(name='firexkit',
      version=versioneer.get_version(),
      cmdclass=versioneer.get_cmdclass(),
      description='Core firex libraries',
      url='https://github.com/FireXStuff/firexkit',
      author='Core FireX Team',
      author_email='firex-dev@gmail.com',
      license='BSD-3-Clause',
      packages=['firexkit', ],
      zip_safe=True,
      install_requires=[
        "celery >= 4.2.1",
      ],)

import codecs
import os
import re

from setuptools import setup


with codecs.open(os.path.join(os.path.abspath(os.path.dirname(
        __file__)), 'aiomcache', '__init__.py'), 'r', 'latin1') as fp:
    try:
        version = re.findall(r'^__version__ = "([^"]+)"\r?$', fp.read(), re.M)[0]
    except IndexError:
        raise RuntimeError('Unable to determine version.')


def read(f):
    return open(os.path.join(os.path.dirname(__file__), f)).read().strip()


setup(name='aiomcache',
      version=version,
      description=('Minimal pure python memcached client'),
      long_description='\n\n'.join((read('README.rst'), read('CHANGES.rst'))),
      long_description_content_type='text/x-rst',
      classifiers=[
          'License :: OSI Approved :: BSD License',
          'Intended Audience :: Developers',
          'Programming Language :: Python',
          'Programming Language :: Python :: 3.8',
          'Programming Language :: Python :: 3.9',
          'Programming Language :: Python :: 3.10',
          'Programming Language :: Python :: 3.11',
          'Programming Language :: Python :: 3.12',
          'Operating System :: POSIX',
          'Operating System :: MacOS :: MacOS X',
          'Operating System :: Microsoft :: Windows',
          'Environment :: Web Environment',
          'Framework :: AsyncIO',
      ],
      author='Nikolay Kim',
      author_email='fafhrd91@gmail.com',
      maintainer=', '.join(('Nikolay Kim <fafhrd91@gmail.com>',
                            'Andrew Svetlov <andrew.svetlov@gmail.com>')),
      maintainer_email='aio-libs@googlegroups.com',
      url='https://github.com/aio-libs/aiomcache/',
      license='BSD',
      packages=("aiomcache",),
      python_requires='>=3.8',
      install_requires=('typing_extensions>=4; python_version<"3.11"',),
      tests_require=("nose",),
      test_suite='nose.collector',
      include_package_data=True)

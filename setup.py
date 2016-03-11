import codecs
import os
import re
import sys
from setuptools import setup, find_packages


with codecs.open(os.path.join(os.path.abspath(os.path.dirname(
        __file__)), 'aiomcache', '__init__.py'), 'r', 'latin1') as fp:
    try:
        version = re.findall(r"^__version__ = '([^']+)'\r?$",
                             fp.read(), re.M)[0]
    except IndexError:
        raise RuntimeError('Unable to determine version.')


if sys.version_info >= (3, 4):
    install_requires = []
else:
    install_requires = ['asyncio']

tests_require = install_requires + ['nose']


def read(f):
    return open(os.path.join(os.path.dirname(__file__), f)).read().strip()


setup(name='aiomcache',
      version=version,
      description=('Minimal pure python memcached client'),
      long_description='\n\n'.join((read('README.rst'), read('CHANGES.txt'))),
      classifiers=[
          'License :: OSI Approved :: BSD License',
          'Intended Audience :: Developers',
          'Programming Language :: Python',
          'Programming Language :: Python :: 3.3',
          'Programming Language :: Python :: 3.4',
          'Programming Language :: Python :: 3.5',
          'Operating System :: POSIX',
          'Operating System :: MacOS :: MacOS X',
          'Operating System :: Microsoft :: Windows',
          'Environment :: Web Environment'],
      author='Nikolay Kim, KeepSafe',
      author_email='fafhrd91@gmail.com',
      url='https://github.com/aio-libs/aiomcache/',
      license='BSD',
      packages=find_packages(),
      install_requires = install_requires,
      tests_require = tests_require,
      test_suite = 'nose.collector',
      include_package_data = True)

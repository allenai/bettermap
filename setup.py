from setuptools import setup, find_packages

setup(
  name='bettermap',
  version='1.1.0',
  description="Drop-in replacements for Python's map function",
  url='https://github.com/allenai/bettermap',
  author="Dirk Groeneveld",
  author_email="dirkg@allenai.org",
  packages=find_packages(),
  install_requires=['dill'],
  python_requires='>=3.6'
)

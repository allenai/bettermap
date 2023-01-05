from setuptools import setup, find_packages


def parse_requirements_file(path):
    requirements = []
    with open(path) as requirements_file:
        import re

        def fix_url_dependencies(req: str) -> str:
            """Pip and setuptools disagree about how URL dependencies should be handled."""
            m = re.match(
                r"^(git\+)?(https|ssh)://(git@)?github\.com/([\w-]+)/(?P<name>[\w-]+)\.git",
                req,
            )
            if m is None:
                return req
            else:
                return f"{m.group('name')} @ {req}"

        for line in requirements_file:
            line = line.strip()
            if line.startswith("#") or len(line) <= 0:
                continue
            req, *comment = line.split("#")
            req = fix_url_dependencies(req.strip())
            requirements.append(req)
    return requirements


setup(
    name='bettermap',
    version='1.3.0',
    description="Parallelized drop-in replacements for Python's map function",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    url='https://github.com/allenai/bettermap',
    author="Dirk Groeneveld",
    author_email="dirkg@allenai.org",
    license="Apache",
    packages=find_packages(
        exclude=[
            "*.tests",
            "*.tests.*",
            "tests.*",
            "tests",
            "test_fixtures",
            "test_fixtures.*",
        ],
    ),
    install_requires=parse_requirements_file("requirements.txt"),
    extras_require={"dev": parse_requirements_file("dev-requirements.txt")},
    python_requires='>=3.6'
)

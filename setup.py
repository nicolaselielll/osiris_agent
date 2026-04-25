import pathlib
import re

from setuptools import setup, find_packages

HERE = pathlib.Path(__file__).parent
long_description = (HERE / "README.md").read_text(encoding="utf-8")
init_text = (HERE / "osiris_agent" / "__init__.py").read_text(encoding="utf-8")
version_match = re.search(r"__version__\s*=\s*['\"]([^'\"]+)['\"]", init_text)
if version_match is None:
    raise RuntimeError("Could not determine osiris_agent version from __init__.py")

PACKAGE_VERSION = version_match.group(1)

setup(
    name='osiris_agent',
    version=PACKAGE_VERSION,
    description='OSIRIS agent for ROS2/Humble',
    long_description=long_description,
    long_description_content_type="text/markdown",
    url='https://github.com/nicolaselielll/osiris_agent',
    author='Nicolas Tuomaala',
    author_email='nicolas.tuomaala00@gmail.com',
    packages=find_packages(),
    package_data={
        'osiris_agent': ['bin/graph_watcher'],
    },
    license='Apache-2.0',
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: Apache Software License',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3 :: Only',
        'Operating System :: OS Independent',
    ],
    keywords='ros2 humble agent',
    install_requires=[
        'websockets',
        'psutil',
        'pyzmq',
    ],
    extras_require={
        'ros': ['rclpy'],
    },
    entry_points={
        'console_scripts': [
            'osiris = osiris_agent.agent_node:main',
        ],
    },
)
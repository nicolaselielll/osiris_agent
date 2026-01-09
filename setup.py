from setuptools import setup, find_packages
import pathlib

HERE = pathlib.Path(__file__).parent
long_description = (HERE / "README.md").read_text(encoding="utf-8")

setup(
    name='osiris_agent',
    version='0.1.0',
    description='OSIRIS agent for ROS2/Humble',
    long_description=long_description,
    long_description_content_type="text/markdown",
    url='https://github.com/nicolaselielll/osiris_agent',
    author='Nicolas Tuomaala',
    author_email='nicolas.tuomaala00@gmail.com',
    packages=find_packages(),
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
    ],
    extras_require={
        'ros': ['rclpy'],
    },
    entry_points={
        'console_scripts': [
            'agent_node = osiris_agent.agent_node:main',
        ],
    },
)
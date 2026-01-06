from setuptools import setup, find_packages

setup(
    name='osiris_agent',
    version='0.1.0',
    packages=find_packages(),
    install_requires=[
        'websockets',
        'psutil',
        # do NOT include ROS-provided packages like rclpy here; they are
        # provided by the user's ROS 2 installation/environment.
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
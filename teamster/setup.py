# -*- coding: utf-8 -*-
from setuptools import setup

long_description = None
INSTALL_REQUIRES = [
    "dagster~=0.13",
    "dagit~=0.13",
    "requests~=2.26",
    "powerschool~=3.0",
    "python-dotenv~=0.19",
    "dagster-gcp~=0.13",
    "dagster-cloud>=0.13.17",
]

setup_kwargs = {
    "name": "",
    "version": "",
    "description": "",
    "long_description": long_description,
    "license": "GPL-3.0-or-later",
    "author": "",
    "author_email": "Charlie Bini <5003326+cbini@users.noreply.github.com>",
    "maintainer": None,
    "maintainer_email": None,
    "url": "",
    "packages": [
        "teamster",
        "teamster.teamster_tests",
        "teamster.common",
        "teamster.tutorial",
        "teamster.kippnewark",
        "teamster.teamster_tests.test_ops",
        "teamster.teamster_tests.test_graphs",
        "teamster.common.graphs",
        "teamster.common.sensors",
        "teamster.common.schedules",
        "teamster.common.ops",
        "teamster.common.resources",
        "teamster.tutorial.graphs",
        "teamster.tutorial.sensors",
        "teamster.tutorial.schedules",
        "teamster.tutorial.ops",
        "teamster.kippnewark.jobs",
        "teamster.kippnewark.config",
        "teamster.kippnewark.graphs",
        "teamster.kippnewark.sensors",
        "teamster.kippnewark.schedules",
        "teamster.kippnewark.ops",
        "teamster.kippnewark.resources",
    ],
    "package_data": {"": ["*"]},
    "install_requires": INSTALL_REQUIRES,
    "python_requires": ">=3.8",
}


setup(**setup_kwargs)

import setuptools

setuptools.setup(
    name="teamster",
    packages=setuptools.find_packages(exclude=["teamster_tests"]),
    install_requires=[
        "dagster~=0.13.6",
        "dagit~=0.13.6",
        "pytest",
    ],
)

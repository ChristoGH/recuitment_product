from setuptools import setup

setup(
    name="recruitment-product",
    version="0.1.0",
    packages=["recruitment"],
    package_dir={"": "src"},
    install_requires=[
        "aio-pika>=9.5.5",
        "aiosqlite>=0.21.0",
        "apscheduler>=3.11.0",
        "python-dotenv>=0.9.9",  # Corrected from 'dotenv'
        "fastapi>=0.115.12",
        "googlesearch-python>=1.3.0",
        "psutil>=7.0.0",
        "tenacity>=9.1.2",
        "uvicorn>=0.28.0",
        "crawl4ai>=0.6.0",  # Add version based on actual requirement
    ],
    extras_require={
        "dev": [
            "types-psutil",
            "mypy",
            "ruff",
            "pre-commit",
        ]
    },
)

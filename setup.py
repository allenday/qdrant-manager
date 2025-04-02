import setuptools

# Read requirements from requirements.txt
# This might be redundant if pyproject.toml handles dependencies
# with open("requirements.txt", "r", encoding="utf-8") as fh:
#     requirements = fh.read().splitlines()

# Read README for long description
with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setuptools.setup(
    name="solr-manager", # Updated name
    version="0.2.0",      # Updated version
    author="Allen Day",
    author_email="allenday@google.com",
    description="A command-line tool for managing SolrCloud collections and documents.", # Updated description
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/allenday/solr-manager", # Updated URL
    packages=setuptools.find_packages(where=".", include=["solr_manager*"]), # Updated package finding
    # install_requires=requirements, # Dependencies managed by pyproject.toml
    install_requires=[
        "click >= 8.0",
        "pyyaml >= 6.0",
        "requests >= 2.20",
        "rich >= 12.0",
        "jsonpath-ng >= 1.5",
        "pysolr >= 3.9.0", # Added Solr client
    ],
    extras_require={
        'zookeeper': ["kazoo >= 2.10.0"], # Added optional ZK dependency
        'dev': [
            "pytest>=6.0.0",
            "pytest-cov>=6.0.0",
        ]
    },
    python_requires=">=3.8",
    entry_points={
        "console_scripts": [
            "solr-manager=solr_manager.cli:main", # Updated entry point
        ],
    },
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "Intended Audience :: System Administrators",
        "License :: OSI Approved :: Apache Software License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Topic :: Database",
        "Topic :: System :: Systems Administration",
        "Topic :: Utilities",
    ],
    keywords=["solr", "solrcloud", "cli", "database", "search", "manager"], # Added keywords
    project_urls={ # Added project URLs
        "Homepage": "https://github.com/allenday/solr-manager",
        "Bug Tracker": "https://github.com/allenday/solr-manager/issues",
    },
)

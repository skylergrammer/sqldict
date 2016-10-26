from setuptools import setup, find_packages

setup(
    name = "sqldict",
    packages = find_packages(),
    version = "0.2.0",
    author='Skyler Grammer',
    author_email="skylergrammer@gmail.com",
    description = "A dict-like object that uses a sqlite3 table",
    url = "https://github.com/skylergrammer/sqldict",
    license = "Apache 2.0",
    classifiers = ["Programming Language :: Python",
    			   "Programming Language :: Python :: 2.7",
    			   "Programming Language :: Python :: 2",
                   "Programming Language :: Python :: 3",
                   "Programming Language :: Python :: 3.4",
    			   "License :: OSI Approved :: Apache Software License",
    			   "Operating System :: OS Independent",
    			   "Development Status :: 4 - Beta",
    			   "Intended Audience :: Developers"
    			   ]
	)

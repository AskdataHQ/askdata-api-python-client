from setuptools import setup, find_packages

setup(name='askdata',
      version='0.4.3959',
      description='The official Askdata Python SDK',
      url='https://github.com/AskdataInc/askdata-api-python-client',
      author=['Giuseppe De Maio','Matteo Giacalone', 'Luca Sarcona', 'Simone Di Somma', 'Massimiliano Bruni'],
      author_email='datascience@askdata.com',
      license='Apache License 2.0',
      packages=find_packages(exclude=["dev","*.tests", "*.tests.*", "tests.*", "tests","entity.py","feed.py"]),
      long_description=open('README.md').read(),
      long_description_content_type="text/markdown",
      include_package_data=True,
      install_requires=[
          'pandas>=0.24.0',
          'numpy>=1.18.2',
          'PyYAML>=5.1',
          'yaml-1.3',
          'requests>=2',
          'urllib3>=1',
          'sqlalchemy>=1.3.8',
          'mysql-connector>=2.2.9',
          'NotebookScripter==6.0.0'
      ],

      classifiers=[
        "Programming Language :: Python :: 3",
        "Operating System :: OS Independent",
        "License :: OSI Approved :: MIT License",
        "Development Status :: 3 - Alpha",
      ],
      keywords='nlp',
      python_requires='>=3, <4',
      zip_safe=False,
      setup_requires=['nose'],
      test_suite='nose.collector'
     )

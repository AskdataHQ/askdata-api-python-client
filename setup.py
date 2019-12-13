from setuptools import setup

setup(name='askdata-api-python-client',
      version='0.0.1',
      description='A library for creating a client for interacting with Askdata',
      url='https://github.com/AskdataINC',
      author=['Giuseppe De Maio','Matteo Giacalone'],
      author_email=['g.demaio@askdata.com','m.giacalone@askdata.com'],
      license='ASKDATA',
      packages=['askdata_api_python_client'],
      install_requires=[
          'pandas',
          'pyyaml'
      ],
      classifiers=[
    # How mature is this project? Common values are
    #   3 - Alpha
    #   4 - Beta
    #   5 - Production/Stable
    'Development Status :: 3 - Alpha',

    # Indicate who your project is intended for
    'Intended Audience :: Developers',
    'Topic :: Software Development :: Build Tools',

    # Pick your license as you wish (should match "license" above)
     'License :: OSI Approved :: MIT License',

    # Specify the Python versions you support here. In particular, ensure
    # that you indicate whether you support Python 2, Python 3 or both.
    'Programming Language :: Python :: 2',
    'Programming Language :: Python :: 2.6',
    'Programming Language :: Python :: 2.7',
    'Programming Language :: Python :: 3',
    'Programming Language :: Python :: 3.2',
    'Programming Language :: Python :: 3.3',
    'Programming Language :: Python :: 3.4',
    ],
    keywords='nlp',
    python_requires='>=2.6, !=3.0.*, !=3.1.*, !=3.2.*, <4',
    zip_safe=False
     )
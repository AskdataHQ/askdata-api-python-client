from setuptools import setup

setup(name='askdata-api-python-client',
      version='0.1',
      description='A library for creating a client for interacting with Askdata',
      url='inserting url github',
      author=['Giuseppe De Maio','Matteo Giacalone'],
      author_email=['g.demaio@askdata.com','m.giacalone@askdata.com'],
      license='ASKDATA',
      packages=['askdata_api_python_client'],
      install_requires=[
          'pandas',
          'pyyaml'
      ],
      zip_safe=False
     )
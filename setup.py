import setuptools

with open('requirements.txt') as f:
    requirements = [pkg for pkg in f.read().splitlines() if not pkg.startswith('git')]

setuptools.setup(name='wiki_entity_vec',
                 author='Pedro Marques',
                 author_email='pedro.r.marques@gmail.com',
                 description='Generate and train Wikipedia entity vectors',
                 url='https://github.com/pedro-r-marques/wikitools',
                 packages=setuptools.find_packages(),
                 install_requires = requirements,
                 python_requires='>=3.6',
                 version='0.0.1')

import glob
import os
import setuptools
import shutil


class CleanCommand(setuptools.Command):
    """Clean command to cleanup after running tests."""
    CLEAN_FILES = [
        './build', './dist', './*.pyc', './*.tgz'
        './*.egg-info', '.eggs', './pytest_cache'
    ]

    user_options = []

    def initialize_options(self):
        pass

    def finalize_options(self):
        pass

    def run(self):
        here = os.getcwd()

        for path_spec in self.CLEAN_FILES:
            # Make paths absolute and relative to this path
            abs_paths = glob.glob(
                os.path.normpath(os.path.join(here, path_spec))
            )
            for path in [str(p) for p in abs_paths]:
                if not path.startswith(here):
                    # Die if path in CLEAN_FILES is absolute + outside this
                    # directory
                    raise ValueError(
                        "{} is not a path inside {}".format(path, here)
                    )
                print('removing %s' % os.path.relpath(path))
                shutil.rmtree(path)


def is_pypy_req(req):
    """
    returns True if `req` a pypy requirement
    """
    pins = ['==', '>=', '~=']
    return any(pin in req for pin in pins)


pypy_requirements = []
git_requirements = []


with open('requirements.txt', 'r') as f:
    pypy_requirements += list(map(str.strip, f.readlines()))


for req in pypy_requirements:
    if not is_pypy_req(req):
        git_requirements.append(req)
        pypy_requirements.remove(req)


setuptools.setup(
    name='serengeti',
    version='0.0.1',
    author='Daniel Gottlieb Dollerup',
    author_email='daniel.dollerup@gmail.com',
    description='Scalable and Easy Workflow Pipelines',
    long_description='null',  # readme,
    long_description_content_type='text/markdown',
    # url='',
    python_requires=">=3.9",
    install_requires=pypy_requirements,
    tests_require=['pytest'],
    packages=setuptools.find_packages(),
    package_data={
        '': ['*.yml', '*.cfg', '*.j2']
    },
    # scripts=[None],
    # entry_points={
    #     'console_scripts': [
    #         'serengeti='
    #     ]
    # },
    cmdclass={
        'clean': CleanCommand
    }
)

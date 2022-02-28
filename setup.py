import setuptools
import yaml

name = 'qrm_client'
file_path = 'qrm_client'

with open(f'{file_path}/requirements.txt') as fid:
    install_requires = fid.readlines()


def get_version_info() -> str:
    try:
        with open(f'{file_path}/ver.yaml') as fid:
            version = yaml.safe_load(fid).get('version')
            print(version)
    except FileNotFoundError:
        with open(f'{file_path}/_ver.yaml') as fid:
            version = yaml.safe_load(fid).get('version')
            print(version)

    return str(version)


setuptools.setup(
    name=name,
    version=get_version_info(),
    description=name,
    url='https://github.com/final-israel/qrm',
    author='iPerf',
    author_email='pavelr@final.israel',
    install_requires=install_requires,
    python_requires='>=3.6.9',
    packages=['qrm_defs', 'qrm_client'],
    include_package_data=True,
    classifiers=[
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: 3.10',
    ],
)

